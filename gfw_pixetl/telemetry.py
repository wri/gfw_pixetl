import json
import os
import signal
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import psutil

from gfw_pixetl import get_module_logger

LOGGER = get_module_logger(__name__)

# ---------- helpers to read container limits (cgroup v1 & v2) ----------


def _read_int(path: str) -> Optional[int]:
    try:
        with open(path, "r") as f:
            v = f.read().strip()
        if v in ("max", ""):
            return None
        return int(v)
    except Exception:
        return None


def _detect_cgroup_version() -> int:
    # v2 has a single mount; v1 has multiple controllers
    try:
        with open("/proc/filesystems") as f:
            content = f.read()
        return 2 if "cgroup2" in content else 1
    except Exception:
        return 1


def _cgroup_paths() -> Tuple[str, str]:
    """Returns (mem_path, cpu_path) roots depending on cgroup version.

    We fall back gracefully if not present (e.g., non-container local
    runs).
    """
    if _detect_cgroup_version() == 2:
        base = "/sys/fs/cgroup"
        return base, base
    else:
        return "/sys/fs/cgroup/memory", "/sys/fs/cgroup/cpu"


def read_container_limits() -> Dict[str, Optional[int]]:
    """Return container limits in bytes (memory) and cpu quota/period.

    May return None when not constrained or not available.
    """
    mem_path, cpu_path = _cgroup_paths()

    # Memory limits
    # cgroup v2: memory.max, memory.current
    # cgroup v1: memory.limit_in_bytes, memory.usage_in_bytes
    mem_limit = _read_int(os.path.join(mem_path, "memory.max")) or _read_int(
        os.path.join(mem_path, "memory.limit_in_bytes")
    )
    mem_usage = _read_int(os.path.join(mem_path, "memory.current")) or _read_int(
        os.path.join(mem_path, "memory.usage_in_bytes")
    )

    # CPU limits (v2: cpu.max "quota period"; v1: cpu.cfs_quota_us / cpu.cfs_period_us)
    cpu_max_path = os.path.join(cpu_path, "cpu.max")
    cpu_quota = None
    cpu_period = None
    try:
        if os.path.exists(cpu_max_path):
            with open(cpu_max_path, "r") as f:
                raw = f.read().strip()  # e.g., "50000 100000"
            parts = raw.split()
            if len(parts) == 2 and parts[0] != "max":
                cpu_quota = int(parts[0])
                cpu_period = int(parts[1])
        else:
            cpu_quota = _read_int(os.path.join(cpu_path, "cpu.cfs_quota_us"))
            cpu_period = _read_int(os.path.join(cpu_path, "cpu.cfs_period_us"))
    except Exception:
        pass

    return {
        "memory_limit_bytes": mem_limit,
        "memory_usage_bytes": mem_usage,
        "cpu_quota_us": cpu_quota,
        "cpu_period_us": cpu_period,
    }


def effective_cpu_count(limits: Dict[str, Optional[int]]) -> Optional[float]:
    """Derive CPU limit in vCPUs from cgroup quota/period when available."""
    q = limits.get("cpu_quota_us")
    p = limits.get("cpu_period_us")
    if q is None or p in (None, 0):
        return None
    return max(0.0, float(q) / float(p))


# ---------- reporter ----------


@dataclass
class ReporterConfig:
    interval: float = 5.0  # seconds between reports
    warmup: float = 0.5  # seconds used for cpu_percent sampling
    workdir: str = "."  # disk usage path to track
    emit_emf: bool = True  # also log CloudWatch EMF JSON
    namespace: str = "Pixetl/Batch"  # EMF namespace
    dimensions: Tuple[str, ...] = ("JobId", "Attempt")  # EMF dimensions


class ResourceReporter:
    def __init__(self, cfg: ReporterConfig):
        # self.log = logger
        self.cfg = cfg
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._proc = psutil.Process(os.getpid())

        # discover Batch identifiers if present
        self.job_id = os.environ.get("AWS_BATCH_JOB_ID", "unknown")
        self.attempt = os.environ.get("AWS_BATCH_JOB_ATTEMPT", "0")

        # prime psutil CPU measurement
        psutil.cpu_percent(interval=None)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run, name="pixetl-resource-reporter", daemon=True
        )
        self._thread.start()

        # Handle SIGTERM for graceful shutdown (Batch uses SIGTERM before SIGKILL)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.cfg.interval + 1.0)

    def _handle_sigterm(self, signum, frame):
        LOGGER.info("ResourceReporter received SIGTERM; stopping.")
        self.stop()

    def _collect_snapshot(self) -> Dict[str, float]:
        # system-level (container view) cpu & mem
        cpu_pct = psutil.cpu_percent(interval=self.cfg.warmup)
        virt = psutil.virtual_memory()
        swap = psutil.swap_memory()

        # process-level memory (pixetl process + children)
        rss_self = self._proc.memory_info().rss
        rss_children = sum(
            (
                c.memory_info().rss
                for c in self._proc.children(recursive=True)
                if c.is_running()
            ),
            0,
        )

        # disk usage at workdir
        try:
            disk = psutil.disk_usage(self.cfg.workdir)
            disk_used_pct = disk.percent
        except Exception:
            disk_used_pct = float("nan")

        # cgroup-derived limits
        limits = read_container_limits()
        mem_limit = limits.get("memory_limit_bytes")
        mem_usage_bytes = limits.get("memory_usage_bytes")
        cpu_limit = effective_cpu_count(limits)

        # derive % of limit when available
        mem_pct_of_limit = None
        if mem_limit and mem_limit > 0 and mem_usage_bytes is not None:
            mem_pct_of_limit = (mem_usage_bytes / float(mem_limit)) * 100.0

        snapshot = {
            "cpu_percent": float(cpu_pct),
            "mem_percent": float(virt.percent),
            "swap_percent": float(swap.percent),
            "disk_percent": float(disk_used_pct),
            "proc_rss_bytes": float(rss_self),
            "children_rss_bytes": float(rss_children),
            "cgroup_mem_used_bytes": float(mem_usage_bytes or 0),
            "cgroup_mem_limit_bytes": float(mem_limit or 0),
            "cgroup_cpu_limit": float(cpu_limit)
            if cpu_limit is not None
            else float("nan"),
            "timestamp": float(time.time()),
        }
        return snapshot

    def _log_human(self, snap: Dict[str, float]) -> None:
        LOGGER.debug(
            "TS:%d CPU:%.1f%% MEM:%.1f%% SWAP:%.1f%% DISK:%.1f%% "
            "RSS(proc):%dB RSS(children):%dB cgrpMem:%d/%dB(%.1f%%) cgrpCPU:%.2f",
            int(snap["timestamp"]),
            snap["cpu_percent"],
            snap["mem_percent"],
            snap["swap_percent"],
            snap["disk_percent"],
            int(snap["proc_rss_bytes"]),
            int(snap["children_rss_bytes"]),
            int(snap["cgroup_mem_used_bytes"]),
            int(snap["cgroup_mem_limit_bytes"]),
            (snap["cgroup_mem_used_bytes"] / snap["cgroup_mem_limit_bytes"] * 100.0)
            if snap["cgroup_mem_limit_bytes"] > 0
            else float("nan"),
            snap["cgroup_cpu_limit"]
            if snap["cgroup_cpu_limit"] == snap["cgroup_cpu_limit"]
            else -1.0,  # NaN guard
        )

    def _log_emf(self, snap: Dict[str, float]) -> None:
        # CloudWatch EMF payload; one block with multiple metrics
        emf = {
            "_aws": {
                "Timestamp": int(snap["timestamp"] * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": self.cfg.namespace,
                        "Dimensions": [list(self.cfg.dimensions)],
                        "Metrics": [
                            {"Name": "CPUPercent", "Unit": "Percent"},
                            {"Name": "MemPercent", "Unit": "Percent"},
                            {"Name": "SwapPercent", "Unit": "Percent"},
                            {"Name": "DiskPercent", "Unit": "Percent"},
                            {"Name": "ProcRSS", "Unit": "Bytes"},
                            {"Name": "ChildrenRSS", "Unit": "Bytes"},
                            {"Name": "CgroupMemUsed", "Unit": "Bytes"},
                            {"Name": "CgroupMemLimit", "Unit": "Bytes"},
                            {"Name": "CgroupCPULimit", "Unit": "None"},
                        ],
                    }
                ],
            },
            "JobId": self.job_id,
            "Attempt": self.attempt,
            "CPUPercent": round(snap["cpu_percent"], 3),
            "MemPercent": round(snap["mem_percent"], 3),
            "SwapPercent": round(snap["swap_percent"], 3),
            "DiskPercent": round(snap["disk_percent"], 3),
            "ProcRSS": int(snap["proc_rss_bytes"]),
            "ChildrenRSS": int(snap["children_rss_bytes"]),
            "CgroupMemUsed": int(snap["cgroup_mem_used_bytes"]),
            "CgroupMemLimit": int(snap["cgroup_mem_limit_bytes"]),
            "CgroupCPULimit": float(snap["cgroup_cpu_limit"])
            if snap["cgroup_cpu_limit"] == snap["cgroup_cpu_limit"]
            else None,
        }
        # EMF must be a single JSON line on stdout/your logs
        print(json.dumps(emf), flush=True)

    def _run(self) -> None:
        interval = self.cfg.interval
        next_tick = time.monotonic()
        try:
            while not self._stop.is_set():
                snap = self._collect_snapshot()
                self._log_human(snap)
                if self.cfg.emit_emf:
                    self._log_emf(snap)
                next_tick += interval
                # sleep with drift correction
                to_sleep = max(0.1, next_tick - time.monotonic())
                self._stop.wait(timeout=to_sleep)
        except Exception as e:
            LOGGER.exception("ResourceReporter crashed: %s", e)
