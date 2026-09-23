"""Cgroup-aware admission control for memory-intensive raster work.

The transform stage may have more workers than can safely hold their
peak native GDAL/Rasterio working sets at once.  This module keeps the
worker pool intact but delays *new* expensive work when cgroup memory
pressure is high.

The controller is created as a module-level singleton (``MEMORY_ADMISSION``,
below) and ``configure()``d in the single-threaded parent before any worker
processes start.

IMPORTANT - why this module exists in its current shape:
ParallelPipe workers (and ``processify`` children) are started with the
explicit ``spawn`` start method, not ``fork`` (see parallelpipe.py and
decorators.py; this was a deliberate correctness fix, since forking a
multithreaded process that has already loaded GDAL is unsafe).  Under
``spawn`` a worker does NOT inherit the parent's memory - it boots a fresh
interpreter and re-imports whatever modules its target callable needs.  If
the transform worker's target function references this module's
``MEMORY_ADMISSION`` singleton only by (module, qualname) - which is how
functions get pickled "by reference" - then re-importing
``gfw_pixetl.memory_admission`` in the child re-executes this module's
top-level code and creates a *brand new*, unconfigured
``MemoryAdmissionController()`` (``enabled=False``, fresh/unshared
``mp.Value``/``mp.RLock``/``mp.BoundedSemaphore`` objects) that has no
relationship to the parent's configured instance. Concretely, this means
admission control was silently a no-op in every transform worker even when
``memory_admission_enabled`` was True, because each worker's local
``self.enabled`` defaulted to False and every gated method (``acquire_transform``,
``wait_for_stats``, etc.) short-circuits on ``if not self.enabled: return``.

The fix: ``mp.Value``/``mp.RLock``/``mp.BoundedSemaphore`` objects (and plain
config values) can be shared correctly across a ``spawn`` boundary, but only
if they are passed *explicitly* as arguments to the worker's target callable
- not picked up implicitly via a re-imported module global. See
``snapshot_shared_state()`` / ``bind_shared_state()`` below: the parent
snapshots its configured, already-shared primitives into a picklable
``AdmissionSharedState``, passes that explicitly into the transform stage's
target function (as an extra argument), and the first thing that function
does in the (freshly spawned, freshly re-imported) worker is call
``MEMORY_ADMISSION.bind_shared_state(state)``, which mutates the worker's
local singleton *in place* to point at the same underlying shared
primitives and configuration the parent has. Because the rebind mutates the
existing object rather than reassigning the module-level name, every other
module in that worker process that already did
``from gfw_pixetl.memory_admission import MEMORY_ADMISSION`` (e.g.
``gfw_pixetl/tiles/tile.py``, ``gfw_pixetl/tiles/raster_src_tile.py``) is
holding a reference to that same object and is therefore correctly
"upgraded" too, without needing its own explicit wiring.
"""

import dataclasses
import json
import multiprocessing as mp
import os
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

from gfw_pixetl import get_module_logger
from gfw_pixetl.telemetry import CGROUP_ROOT, _read_int

LOGGER = get_module_logger(__name__)

STATUS_PATH = "/tmp/pixetl-memory-admission.json"
GIB = 1024**3

# Use an explicit spawn context for the shared primitives below, to match
# the explicit spawn context used everywhere workers are actually created
# (parallelpipe.py, decorators.py). This is not strictly required for the
# fix - a Value/RLock/BoundedSemaphore created under any context can be
# passed as an explicit argument to a spawned child - but keeping the
# context consistent avoids surprises and documents the intent.
_MP_CONTEXT = mp.get_context("spawn")


@dataclasses.dataclass
class AdmissionSharedState:
    """A picklable handle to one configured controller's shared state.

    Passing this object explicitly into a worker's target callable (rather
    than relying on the worker re-importing this module and finding an
    already-configured global) is what makes admission control actually
    shared across a ``spawn`` boundary. Every field here must remain
    picklable via the standard multiprocessing reduction machinery: the
    ``mp.Value``/``mp.RLock``/``mp.BoundedSemaphore`` objects support this
    specifically when passed as explicit process/callable arguments, which
    is the officially supported way to share them with spawned children.
    """

    lock: Any
    reserved_bytes: Any
    waiting: Any
    throttled: Any
    stats_active: Any
    stats_waiting: Any
    stats_semaphore: Any
    copy_active: Any
    copy_waiting: Any
    copy_semaphore: Any
    enabled: bool
    cgroup_root: str
    high_watermark: float
    resume_watermark: float
    stats_workers: int
    copy_workers: int
    reservation_bytes: int
    window_reservation_bytes: int
    poll_seconds: float


class MemoryAdmissionController:
    """Coordinate transform admission using cgroup-v2 memory pressure."""

    def __init__(self) -> None:
        # These are created with an explicit spawn-compatible context. They
        # are only actually *shared* with a worker process if that worker
        # receives them explicitly - see ``bind_shared_state`` above. A
        # worker that does not receive a snapshot keeps its own private
        # (and, since ``enabled`` defaults to False, inert) set of these.
        self._lock = _MP_CONTEXT.RLock()
        self._reserved_bytes = _MP_CONTEXT.Value("q", 0, lock=False)
        self._waiting = _MP_CONTEXT.Value("i", 0, lock=False)
        self._throttled = _MP_CONTEXT.Value("b", 0, lock=False)
        self._stats_active = _MP_CONTEXT.Value("i", 0, lock=False)
        self._stats_waiting = _MP_CONTEXT.Value("i", 0, lock=False)
        self._stats_semaphore = _MP_CONTEXT.BoundedSemaphore(4)
        self._copy_active = _MP_CONTEXT.Value("i", 0, lock=False)
        self._copy_waiting = _MP_CONTEXT.Value("i", 0, lock=False)
        self._copy_semaphore = _MP_CONTEXT.BoundedSemaphore(8)

        # Per-process state. Each transform worker handles one tile at a time.
        self._local_reservation_held = False

        self.enabled = False
        self.cgroup_root = CGROUP_ROOT
        self.high_watermark = 0.80
        self.resume_watermark = 0.75
        self.stats_workers = 4
        self.copy_workers = 8
        self.reservation_bytes = 8 * GIB
        self.window_reservation_bytes = 8 * GIB
        self.poll_seconds = 1.0

    def configure(
        self,
        *,
        enabled: bool,
        cgroup_root: str = CGROUP_ROOT,
        high_watermark: float = 0.80,
        resume_watermark: float = 0.75,
        stats_workers: int = 4,
        copy_workers: int = 8,
        reservation_bytes: int = 8 * GIB,
        window_reservation_bytes: int = 8 * GIB,
        poll_seconds: float = 1.0,
    ) -> None:
        if not 0 < resume_watermark < high_watermark < 1:
            raise ValueError("memory admission requires 0 < resume < high < 1")
        if stats_workers <= 0:
            raise ValueError("stats_workers must be positive")
        if copy_workers <= 0:
            raise ValueError("copy_workers must be positive")
        if reservation_bytes < 0:
            raise ValueError("reservation_bytes must not be negative")
        if window_reservation_bytes < 0:
            raise ValueError("window_reservation_bytes must not be negative")
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")

        with self._lock:
            self.enabled = enabled
            self.cgroup_root = cgroup_root
            self.high_watermark = high_watermark
            self.resume_watermark = resume_watermark
            self.stats_workers = stats_workers
            self.copy_workers = copy_workers
            # Recreated here so a later snapshot_shared_state() call picks up
            # the configured stats_workers limit. This object only actually
            # becomes shared with worker processes once it is explicitly
            # passed to them via AdmissionSharedState - see the module
            # docstring.
            self._stats_semaphore = _MP_CONTEXT.BoundedSemaphore(stats_workers)
            self._copy_semaphore = _MP_CONTEXT.BoundedSemaphore(copy_workers)
            self.reservation_bytes = reservation_bytes
            self.window_reservation_bytes = window_reservation_bytes
            self.poll_seconds = poll_seconds
            self._reserved_bytes.value = 0
            self._waiting.value = 0
            self._throttled.value = 0
            self._stats_active.value = 0
            self._stats_waiting.value = 0
            self._copy_active.value = 0
            self._copy_waiting.value = 0
            self._local_reservation_held = False
            self._write_status_locked()

        if enabled:
            LOGGER.info(
                "Memory admission enabled: high=%.0f%% resume=%.0f%% "
                "stats_workers=%d reservation=%.1fGiB window_reservation=%.1fGiB",
                high_watermark * 100,
                resume_watermark * 100,
                stats_workers,
                reservation_bytes / GIB,
                window_reservation_bytes / GIB,
            )

    def snapshot_shared_state(self) -> AdmissionSharedState:
        """Capture this (parent-process, already-configured) controller's
        shared primitives and scalar config into a picklable handle.

        Call this once, after ``configure()``, in the single-threaded
        parent. Pass the result explicitly into whatever callable will
        run in a spawned worker process, and have that callable call
        ``bind_shared_state`` on its own (freshly re-imported)
        ``MEMORY_ADMISSION`` before doing any admission-gated work.
        """
        with self._lock:
            return AdmissionSharedState(
                lock=self._lock,
                reserved_bytes=self._reserved_bytes,
                waiting=self._waiting,
                throttled=self._throttled,
                stats_active=self._stats_active,
                stats_waiting=self._stats_waiting,
                stats_semaphore=self._stats_semaphore,
                copy_active=self._copy_active,
                copy_waiting=self._copy_waiting,
                copy_semaphore=self._copy_semaphore,
                enabled=self.enabled,
                cgroup_root=self.cgroup_root,
                high_watermark=self.high_watermark,
                resume_watermark=self.resume_watermark,
                stats_workers=self.stats_workers,
                copy_workers=self.copy_workers,
                reservation_bytes=self.reservation_bytes,
                window_reservation_bytes=self.window_reservation_bytes,
                poll_seconds=self.poll_seconds,
            )

    def bind_shared_state(self, state: AdmissionSharedState) -> None:
        """Rebind this controller's primitives/config onto a shared state
        snapshot taken from another (typically parent-process) instance.

        This mutates ``self`` in place rather than replacing the
        module-level ``MEMORY_ADMISSION`` name, so every other module in
        this process that already imported ``MEMORY_ADMISSION`` sees the
        change too - see the module docstring for why that matters under
        ``spawn``.

        Safe to call multiple times; not thread-safe against concurrent use
        of the controller, so call it once, early, before any admission-
        gated work starts in this process.
        """
        self._lock = state.lock
        self._reserved_bytes = state.reserved_bytes
        self._waiting = state.waiting
        self._throttled = state.throttled
        self._stats_active = state.stats_active
        self._stats_waiting = state.stats_waiting
        self._stats_semaphore = state.stats_semaphore
        self._copy_active = state.copy_active
        self._copy_waiting = state.copy_waiting
        self._copy_semaphore = state.copy_semaphore
        self.enabled = state.enabled
        self.cgroup_root = state.cgroup_root
        self.high_watermark = state.high_watermark
        self.resume_watermark = state.resume_watermark
        self.stats_workers = state.stats_workers
        self.copy_workers = state.copy_workers
        self.reservation_bytes = state.reservation_bytes
        self.window_reservation_bytes = state.window_reservation_bytes
        self.poll_seconds = state.poll_seconds
        # This is per-process bookkeeping (whether *this* process is
        # currently holding a startup reservation) and must never be copied
        # from another process's snapshot.
        self._local_reservation_held = False
        if self.enabled:
            LOGGER.info(
                "Memory admission shared state bound in worker pid %d: "
                "high=%.0f%% resume=%.0f%% stats_workers=%d reservation=%.1fGiB window_reservation=%.1fGiB",
                os.getpid(),
                self.high_watermark * 100,
                self.resume_watermark * 100,
                self.stats_workers,
                self.reservation_bytes / GIB,
                self.window_reservation_bytes / GIB,
            )

    def _memory(self) -> tuple[Optional[int], Optional[int]]:
        current = _read_int(os.path.join(self.cgroup_root, "memory.current"))
        limit = _read_int(os.path.join(self.cgroup_root, "memory.max"))
        return current, limit

    def memory_attribution_snapshot(
        self, pid: Optional[int] = None
    ) -> Dict[str, Optional[int]]:
        """Return a lightweight cgroup/process memory attribution snapshot.

        ``memory.current`` tells us whether the Batch cgroup is growing,
        while selected ``memory.stat`` counters distinguish
        anonymous/native memory from file-backed page cache.  RSS is
        read directly from ``/proc`` so this diagnostic does not add a
        psutil dependency to the hot path.
        """
        current, limit = self._memory()
        values: Dict[str, Optional[int]] = {
            "current": current,
            "limit": limit,
            "anon": None,
            "file": None,
            "kernel": None,
            "pagetables": None,
            "rss": None,
        }

        try:
            with open(os.path.join(self.cgroup_root, "memory.stat")) as f:
                for line in f:
                    key, raw_value = line.split(None, 1)
                    if key in ("anon", "file", "kernel", "pagetables"):
                        values[key] = int(raw_value)
        except (OSError, ValueError):
            pass

        target_pid = os.getpid() if pid is None else pid
        try:
            with open(f"/proc/{target_pid}/statm") as f:
                fields = f.read().split()
            if len(fields) >= 2:
                values["rss"] = int(fields[1]) * os.sysconf("SC_PAGE_SIZE")
        except (OSError, ValueError):
            pass

        return values

    def log_memory_attribution(
        self, stage: str, tile_id: str, child_pid: Optional[int] = None
    ) -> None:
        """Log cgroup buckets plus transform and optional child RSS."""
        cgroup = self.memory_attribution_snapshot()
        child = (
            self.memory_attribution_snapshot(child_pid)
            if child_pid is not None
            else None
        )

        def gib(value: Optional[int]) -> str:
            return "n/a" if value is None else f"{value / GIB:.2f}"

        LOGGER.info(
            "PERF memory_attribution "
            "stage=%s tile=%s cgroup_gib=%s anon_gib=%s file_gib=%s "
            "kernel_gib=%s pagetables_gib=%s transform_rss_gib=%s child_rss_gib=%s",
            stage,
            tile_id,
            gib(cgroup["current"]),
            gib(cgroup["anon"]),
            gib(cgroup["file"]),
            gib(cgroup["kernel"]),
            gib(cgroup["pagetables"]),
            gib(cgroup["rss"]),
            "n/a" if child is None else gib(child["rss"]),
        )

    def _cgroup_pids(self) -> list[int]:
        """Return all PIDs currently charged to this cgroup."""
        try:
            with open(os.path.join(self.cgroup_root, "cgroup.procs")) as f:
                return sorted({int(line) for line in f if line.strip()})
        except (OSError, ValueError):
            return []

    @staticmethod
    def _process_memory_snapshot(
        pid: int, include_smaps: bool = True
    ) -> Optional[Dict[str, object]]:
        """Read cheap process metadata plus smaps_rollup attribution.

        This is diagnostic-only and deliberately reads smaps_rollup only
        for processes selected by ``log_process_census``.
        """
        result: Dict[str, object] = {
            "pid": pid,
            "ppid": None,
            "name": None,
            "rss": None,
            "vmsize": None,
            "swap": None,
            "pss": None,
            "pss_anon": None,
            "pss_file": None,
            "anonymous": None,
        }
        try:
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    key, _, raw = line.partition(":")
                    raw = raw.strip()
                    if key == "Name":
                        result["name"] = raw
                    elif key == "PPid":
                        result["ppid"] = int(raw)
                    elif key == "VmRSS":
                        result["rss"] = int(raw.split()[0]) * 1024
                    elif key == "VmSize":
                        result["vmsize"] = int(raw.split()[0]) * 1024
                    elif key == "VmSwap":
                        result["swap"] = int(raw.split()[0]) * 1024
        except (OSError, ValueError):
            return None

        if not include_smaps:
            return result

        try:
            with open(f"/proc/{pid}/smaps_rollup") as f:
                for line in f:
                    key, _, raw = line.partition(":")
                    if key in ("Rss", "Pss", "Pss_Anon", "Pss_File", "Anonymous"):
                        value = int(raw.strip().split()[0]) * 1024
                        result[
                            {
                                "Rss": "rss",
                                "Pss": "pss",
                                "Pss_Anon": "pss_anon",
                                "Pss_File": "pss_file",
                                "Anonymous": "anonymous",
                            }[key]
                        ] = value
        except (OSError, ValueError):
            pass
        return result

    def log_process_census(self, reason: str, tile_id: str, top_n: int = 20) -> None:
        """Attribute cgroup memory to the largest live processes.

        First rank every cgroup process using the cheap VmRSS value from
        /proc/<pid>/status. Then read smaps_rollup only for the largest
        processes, keeping the diagnostic bounded even on large Batch
        jobs.
        """
        processes = []
        for pid in self._cgroup_pids():
            snapshot = self._process_memory_snapshot(pid, include_smaps=False)
            if snapshot is not None:
                processes.append(snapshot)
        processes.sort(key=lambda item: int(item.get("rss") or 0), reverse=True)
        # Enrich only the largest processes with proportional/anonymous
        # attribution from smaps_rollup.
        for index, item in enumerate(processes[:top_n]):
            enriched = self._process_memory_snapshot(
                int(item["pid"]), include_smaps=True
            )
            if enriched is not None:
                processes[index] = enriched

        cgroup = self.memory_attribution_snapshot()
        total_rss = sum(int(item.get("rss") or 0) for item in processes)
        total_swap = sum(int(item.get("swap") or 0) for item in processes)
        LOGGER.warning(
            "PERF process_census reason=%s tile=%s process_count=%d "
            "cgroup_gib=%.2f anon_gib=%.2f file_gib=%.2f total_rss_gib=%.2f total_swap_gib=%.2f",
            reason,
            tile_id,
            len(processes),
            (cgroup["current"] or 0) / GIB,
            (cgroup["anon"] or 0) / GIB,
            (cgroup["file"] or 0) / GIB,
            total_rss / GIB,
            total_swap / GIB,
        )
        for rank, item in enumerate(processes[:top_n], 1):
            LOGGER.warning(
                "PERF process_census_process reason=%s tile=%s rank=%d pid=%s ppid=%s "
                "name=%s rss_gib=%.3f pss_gib=%s pss_anon_gib=%s pss_file_gib=%s "
                "anonymous_gib=%s vmsize_gib=%s swap_gib=%s",
                reason,
                tile_id,
                rank,
                item["pid"],
                item["ppid"],
                item["name"],
                int(item.get("rss") or 0) / GIB,
                "n/a" if item.get("pss") is None else f'{int(item["pss"]) / GIB:.3f}',
                (
                    "n/a"
                    if item.get("pss_anon") is None
                    else f'{int(item["pss_anon"]) / GIB:.3f}'
                ),
                (
                    "n/a"
                    if item.get("pss_file") is None
                    else f'{int(item["pss_file"]) / GIB:.3f}'
                ),
                (
                    "n/a"
                    if item.get("anonymous") is None
                    else f'{int(item["anonymous"]) / GIB:.3f}'
                ),
                (
                    "n/a"
                    if item.get("vmsize") is None
                    else f'{int(item["vmsize"]) / GIB:.3f}'
                ),
                "n/a" if item.get("swap") is None else f'{int(item["swap"]) / GIB:.3f}',
            )

    def log_process_census_if_due(
        self, reason: str, tile_id: str, interval_seconds: float = 15.0
    ) -> None:
        """Emit at most one cgroup-wide census per interval across workers.

        The timestamp file is protected with an atomic create/replace
        pattern. A small race can produce one extra diagnostic census,
        which is harmless; the important property is avoiding one
        expensive census per blocked tile.
        """
        stamp_path = os.path.join("/tmp", "pixetl-memory-census.stamp")
        now = time.time()
        try:
            mtime = os.stat(stamp_path).st_mtime
            if now - mtime < interval_seconds:
                return
        except OSError:
            pass

        claim_path = f"{stamp_path}.{os.getpid()}.claim"
        try:
            with open(claim_path, "w") as f:
                f.write(str(now))
            # Re-check immediately before publishing our claim. os.replace is
            # atomic; simultaneous contenders can at worst produce one extra
            # census around the interval boundary.
            try:
                mtime = os.stat(stamp_path).st_mtime
                if now - mtime < interval_seconds:
                    return
            except OSError:
                pass
            os.replace(claim_path, stamp_path)
            self.log_process_census(reason, tile_id)
        finally:
            try:
                os.remove(claim_path)
            except OSError:
                pass

    def _write_status_locked(self) -> None:
        payload = {
            "enabled": self.enabled,
            "waiting": int(self._waiting.value),
            "reserved_bytes": int(self._reserved_bytes.value),
            "throttled": bool(self._throttled.value),
            "stats_active": int(self._stats_active.value),
            "stats_waiting": int(self._stats_waiting.value),
            "copy_active": int(self._copy_active.value),
            "copy_waiting": int(self._copy_waiting.value),
        }
        tmp = f"{STATUS_PATH}.{os.getpid()}.tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(payload, f)
            os.replace(tmp, STATUS_PATH)
        except OSError:
            # Admission must never fail merely because diagnostic state cannot
            # be published for telemetry.
            try:
                os.remove(tmp)
            except OSError:
                pass

    def _set_throttled_locked(
        self, value: bool, current: int, limit: int, projected: int
    ) -> None:
        previous = bool(self._throttled.value)
        self._throttled.value = int(value)
        if previous == value:
            return
        self._write_status_locked()
        if value:
            LOGGER.warning(
                "Memory admission throttled: current=%.1fGiB reserved=%.1fGiB "
                "projected=%.1fGiB limit=%.1fGiB waiting=%d",
                current / GIB,
                self._reserved_bytes.value / GIB,
                projected / GIB,
                limit / GIB,
                self._waiting.value,
            )
        else:
            LOGGER.info(
                "Memory admission resumed: current=%.1fGiB reserved=%.1fGiB "
                "limit=%.1fGiB waiting=%d",
                current / GIB,
                self._reserved_bytes.value / GIB,
                limit / GIB,
                self._waiting.value,
            )

    def acquire_transform(self, tile_id: str) -> None:
        """Wait until another transform can safely begin.

        A fixed reservation covers only startup, before the new worker's
        memory is visible in ``memory.current``. The transform worker
        releases that reservation after its first window completes;
        actual cgroup memory is authoritative after that point.
        """
        if not self.enabled:
            return

        waiting_registered = False
        while True:
            with self._lock:
                current, limit = self._memory()
                if current is None or limit is None or limit <= 0:
                    if waiting_registered:
                        self._waiting.value -= 1
                        self._write_status_locked()
                    LOGGER.warning(
                        "Memory admission unavailable for tile %s: cgroup memory "
                        "current/limit could not be read; admitting tile",
                        tile_id,
                    )
                    return

                reserved = int(self._reserved_bytes.value)
                projected = current + reserved + self.reservation_bytes
                high = int(limit * self.high_watermark)
                resume = int(limit * self.resume_watermark)

                throttled = bool(self._throttled.value)
                if throttled and current + reserved < resume:
                    self._set_throttled_locked(False, current, limit, projected)
                    throttled = False
                elif not throttled and projected >= high:
                    self._set_throttled_locked(True, current, limit, projected)
                    throttled = True

                if not throttled:
                    if waiting_registered:
                        self._waiting.value -= 1
                    self._reserved_bytes.value += self.reservation_bytes
                    self._local_reservation_held = True
                    self._write_status_locked()
                    return

                if not waiting_registered:
                    self._waiting.value += 1
                    waiting_registered = True
                    self._write_status_locked()

            time.sleep(self.poll_seconds)

    def commit_transform_reservation(self) -> None:
        """Release startup reservation once the working set is observable."""
        if not self.enabled or not self._local_reservation_held:
            return
        with self._lock:
            self._reserved_bytes.value = max(
                0, self._reserved_bytes.value - self.reservation_bytes
            )
            self._local_reservation_held = False
            self._write_status_locked()

    def release_transform(self) -> None:
        """Release a reservation if transform exited before committing it."""
        self.commit_transform_reservation()

    @contextmanager
    def transform_slot(self, tile_id: str) -> Iterator[None]:
        self.acquire_transform(tile_id)
        try:
            yield
        finally:
            self.release_transform()

    def try_acquire_window(self, tile_id: str, window_index: int) -> bool:
        """Atomically reserve memory for one window if headroom is available.

        ``memory.current`` alone is not sufficient because many
        transform workers can observe the same free memory and start
        together.  The shared ``_reserved_bytes`` value makes those
        decisions serial and immediately visible across spawned
        transform workers.
        """
        if not self.enabled:
            return True

        with self._lock:
            current, limit = self._memory()
            if current is None or limit is None or limit <= 0:
                LOGGER.warning(
                    "Window admission unavailable for tile %s window %d; admitting window",
                    tile_id,
                    window_index + 1,
                )
                return True

            reserved = int(self._reserved_bytes.value)
            projected = current + reserved + self.window_reservation_bytes
            high = int(limit * self.high_watermark)
            if projected >= high:
                return False

            self._reserved_bytes.value += self.window_reservation_bytes
            self._write_status_locked()
            return True

    def acquire_window(self, tile_id: str, window_index: int) -> None:
        """Wait until an atomic per-window memory reservation can be
        acquired."""
        if not self.enabled:
            return

        waiting_registered = False
        while True:
            if self.try_acquire_window(tile_id, window_index):
                if waiting_registered:
                    with self._lock:
                        self._waiting.value = max(0, self._waiting.value - 1)
                        self._write_status_locked()
                    LOGGER.info(
                        "Memory window admission resumed: tile=%s window=%d",
                        tile_id,
                        window_index + 1,
                    )
                return

            if not waiting_registered:
                with self._lock:
                    self._waiting.value += 1
                    self._write_status_locked()
                waiting_registered = True
                LOGGER.warning(
                    "Memory window admission waiting: tile=%s window=%d",
                    tile_id,
                    window_index + 1,
                )
            time.sleep(self.poll_seconds)

    def release_window(self) -> None:
        """Release one executing-window reservation."""
        if not self.enabled:
            return
        with self._lock:
            self._reserved_bytes.value = max(
                0, self._reserved_bytes.value - self.window_reservation_bytes
            )
            self._write_status_locked()

    def memory_pressure_high(self) -> bool:
        """Return whether actual cgroup memory is at/above the high watermark.

        This intentionally considers actual ``memory.current`` only. It
        is used after a window completes to decide whether retiring that
        window worker could help return native GDAL/Rasterio allocations
        to the cgroup. Startup reservations are an admission concern and
        must not cause an already-running worker to recycle.
        """
        if not self.enabled:
            return False
        current, limit = self._memory()
        if current is None or limit is None or limit <= 0:
            return False
        return current >= int(limit * self.high_watermark)

    def wait_for_memory_resume(self, tile_id: str) -> None:
        """Wait for actual cgroup memory to fall below the resume watermark."""
        if not self.enabled:
            return

        waiting = False
        while True:
            current, limit = self._memory()
            if current is None or limit is None or limit <= 0:
                return
            fraction = current / float(limit)
            if fraction < self.resume_watermark:
                if waiting:
                    LOGGER.info(
                        "Memory window-worker gate resumed: tile=%s current=%.1f%%",
                        tile_id,
                        fraction * 100,
                    )
                return
            if not waiting:
                waiting = True
                LOGGER.warning(
                    "Memory window-worker gate waiting: tile=%s current=%.1f%%",
                    tile_id,
                    fraction * 100,
                )
            time.sleep(self.poll_seconds)

    def wait_for_stats(self, tile_id: str) -> None:
        """Do not launch GDAL statistics while cgroup memory is pressured."""
        if not self.enabled:
            return

        waiting = False
        while True:
            with self._lock:
                current, limit = self._memory()
                if current is None or limit is None or limit <= 0:
                    return
                fraction = current / float(limit)
                threshold = self.resume_watermark if waiting else self.high_watermark
                if fraction < threshold:
                    if waiting:
                        LOGGER.info(
                            "Memory stats gate resumed: tile=%s current=%.1f%%",
                            tile_id,
                            fraction * 100,
                        )
                    return
                if not waiting:
                    waiting = True
                    LOGGER.warning(
                        "Memory stats gate waiting: tile=%s current=%.1f%%",
                        tile_id,
                        fraction * 100,
                    )
            time.sleep(self.poll_seconds)

    def wait_for_copy(self, tile_id: str) -> None:
        """Do not launch a GeoTIFF copy while cgroup memory is pressured."""
        if not self.enabled:
            return

        waiting = False
        while True:
            with self._lock:
                current, limit = self._memory()
                if current is None or limit is None or limit <= 0:
                    return
                fraction = current / float(limit)
                threshold = self.resume_watermark if waiting else self.high_watermark
                if fraction < threshold:
                    if waiting:
                        LOGGER.info(
                            "Memory copy gate resumed: tile=%s current=%.1f%%",
                            tile_id,
                            fraction * 100,
                        )
                    return
                if not waiting:
                    waiting = True
                    LOGGER.warning(
                        "Memory copy gate waiting: tile=%s current=%.1f%%",
                        tile_id,
                        fraction * 100,
                    )
            time.sleep(self.poll_seconds)

    @contextmanager
    def copy_slot(self, tile_id: str) -> Iterator[None]:
        """Limit concurrent GeoTIFF copies and gate them on actual memory."""
        with self._lock:
            self._copy_waiting.value += 1
            self._write_status_locked()
        self._copy_semaphore.acquire()
        active = False
        try:
            if self.enabled:
                self.wait_for_copy(tile_id)
            with self._lock:
                self._copy_waiting.value = max(0, self._copy_waiting.value - 1)
                self._copy_active.value += 1
                active = True
                self._write_status_locked()
            yield
        finally:
            if active:
                with self._lock:
                    self._copy_active.value = max(0, self._copy_active.value - 1)
                    self._write_status_locked()
            else:
                with self._lock:
                    self._copy_waiting.value = max(0, self._copy_waiting.value - 1)
                    self._write_status_locked()
            self._copy_semaphore.release()

    @contextmanager
    def stats_slot(self, tile_id: str) -> Iterator[None]:
        """Limit concurrent stats scans and gate them on actual memory."""
        with self._lock:
            self._stats_waiting.value += 1
            self._write_status_locked()
        self._stats_semaphore.acquire()
        active = False
        try:
            if self.enabled:
                self.wait_for_stats(tile_id)
            with self._lock:
                self._stats_waiting.value = max(0, self._stats_waiting.value - 1)
                self._stats_active.value += 1
                active = True
                self._write_status_locked()
            yield
        finally:
            if active:
                with self._lock:
                    self._stats_active.value = max(0, self._stats_active.value - 1)
                    self._write_status_locked()
            else:
                with self._lock:
                    self._stats_waiting.value = max(0, self._stats_waiting.value - 1)
                    self._write_status_locked()
            self._stats_semaphore.release()


MEMORY_ADMISSION = MemoryAdmissionController()


def read_admission_status(path: str = STATUS_PATH) -> Dict[str, object]:
    """Read the best-effort admission snapshot published for telemetry."""
    try:
        with open(path, "r") as f:
            value = json.load(f)
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
