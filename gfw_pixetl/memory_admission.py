"""Cgroup-aware admission control for memory-intensive raster work.

The controller limits new transform, window, and statistics work when
current cgroup memory plus outstanding reservations approaches the
configured high watermark. Work resumes below a lower watermark to avoid
oscillation.

Workers use the ``spawn`` start method, so shared multiprocessing
primitives are passed explicitly via :class:`AdmissionSharedState` and
rebound to the worker-local ``MEMORY_ADMISSION`` singleton before gated
work begins.
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

# Keep shared primitives compatible with the worker start method.
_MP_CONTEXT = mp.get_context("spawn")


@dataclasses.dataclass
class AdmissionSharedState:
    """Shared controller state passed explicitly to spawned workers."""

    lock: Any
    reserved_bytes: Any
    waiting: Any
    throttled: Any
    stats_active: Any
    stats_waiting: Any
    stats_semaphore: Any
    enabled: bool
    cgroup_root: str
    high_watermark: float
    resume_watermark: float
    stats_workers: int
    reservation_bytes: int
    window_reservation_bytes: int
    poll_seconds: float


class MemoryAdmissionController:
    """Coordinate transform admission using cgroup-v2 memory pressure."""

    def __init__(self) -> None:
        # Workers receive these primitives explicitly via AdmissionSharedState.
        self._lock = _MP_CONTEXT.RLock()
        self._reserved_bytes = _MP_CONTEXT.Value("q", 0, lock=False)
        self._waiting = _MP_CONTEXT.Value("i", 0, lock=False)
        self._throttled = _MP_CONTEXT.Value("b", 0, lock=False)
        self._stats_active = _MP_CONTEXT.Value("i", 0, lock=False)
        self._stats_waiting = _MP_CONTEXT.Value("i", 0, lock=False)
        self._stats_semaphore = _MP_CONTEXT.BoundedSemaphore(4)

        # Per-process state. Each transform worker handles one tile at a time.
        self._local_reservation_held = False

        self.enabled = False
        self.cgroup_root = CGROUP_ROOT
        self.high_watermark = 0.80
        self.resume_watermark = 0.75
        self.stats_workers = 4
        self.reservation_bytes = 4 * GIB
        self.window_reservation_bytes = 4 * GIB
        self.poll_seconds = 1.0

    def configure(
        self,
        *,
        enabled: bool,
        cgroup_root: str = CGROUP_ROOT,
        high_watermark: float = 0.80,
        resume_watermark: float = 0.75,
        stats_workers: int = 4,
        reservation_bytes: int = 4 * GIB,
        window_reservation_bytes: int = 4 * GIB,
        poll_seconds: float = 1.0,
    ) -> None:
        if not 0 < resume_watermark < high_watermark < 1:
            raise ValueError("memory admission requires 0 < resume < high < 1")
        if stats_workers <= 0:
            raise ValueError("stats_workers must be positive")
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
            # Recreated here so a later snapshot_shared_state() call picks up
            # the configured stats_workers limit. This object only actually
            # becomes shared with worker processes once it is explicitly
            # passed to them via AdmissionSharedState - see the module
            # docstring.
            self._stats_semaphore = _MP_CONTEXT.BoundedSemaphore(stats_workers)
            self.reservation_bytes = reservation_bytes
            self.window_reservation_bytes = window_reservation_bytes
            self.poll_seconds = poll_seconds
            self._reserved_bytes.value = 0
            self._waiting.value = 0
            self._throttled.value = 0
            self._stats_active.value = 0
            self._stats_waiting.value = 0
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
        """Return the configured state that spawned workers must share."""
        with self._lock:
            return AdmissionSharedState(
                lock=self._lock,
                reserved_bytes=self._reserved_bytes,
                waiting=self._waiting,
                throttled=self._throttled,
                stats_active=self._stats_active,
                stats_waiting=self._stats_waiting,
                stats_semaphore=self._stats_semaphore,
                enabled=self.enabled,
                cgroup_root=self.cgroup_root,
                high_watermark=self.high_watermark,
                resume_watermark=self.resume_watermark,
                stats_workers=self.stats_workers,
                reservation_bytes=self.reservation_bytes,
                window_reservation_bytes=self.window_reservation_bytes,
                poll_seconds=self.poll_seconds,
            )

    def bind_shared_state(self, state: AdmissionSharedState) -> None:
        """Bind this worker-local controller to parent-owned shared state."""
        self._lock = state.lock
        self._reserved_bytes = state.reserved_bytes
        self._waiting = state.waiting
        self._throttled = state.throttled
        self._stats_active = state.stats_active
        self._stats_waiting = state.stats_waiting
        self._stats_semaphore = state.stats_semaphore
        self.enabled = state.enabled
        self.cgroup_root = state.cgroup_root
        self.high_watermark = state.high_watermark
        self.resume_watermark = state.resume_watermark
        self.stats_workers = state.stats_workers
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

    def _write_status_locked(self) -> None:
        payload = {
            "enabled": self.enabled,
            "waiting": int(self._waiting.value),
            "reserved_bytes": int(self._reserved_bytes.value),
            "throttled": bool(self._throttled.value),
            "stats_active": int(self._stats_active.value),
            "stats_waiting": int(self._stats_waiting.value),
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
