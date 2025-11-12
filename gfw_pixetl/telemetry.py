# -*- coding: utf-8 -*-
"""A resource reporter that runs as its own process (safe with other
multiprocessing).

- Emits human-readable INFO lines.
- Emits CloudWatch EMF JSON lines for auto-created metrics.
"""

import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import psutil

# ------------ small cgroup helpers (v1 & v2) ------------


def _read_int(path: str) -> Optional[int]:
    try:
        with open(path, "r") as f:
            v = f.read().strip()
        if v == "" or v == "max":
            return None
        return int(v)
    except Exception:
        return None


def _detect_cgroup_version() -> int:
    try:
        with open("/proc/filesystems", "r") as f:
            return 2 if "cgroup2" in f.read() else 1
    except Exception:
        return 1


def _cgroup_paths() -> Tuple[str, str]:
    if _detect_cgroup_version() == 2:
        base = "/sys/fs/cgroup"
        return base, base
    return "/sys/fs/cgroup/memory", "/sys/fs/cgroup/cpu"


def read_container_limits() -> Dict[str, Optional[int]]:
    mem_p, cpu_p = _cgroup_paths()

    mem_limit = _read_int(os.path.join(mem_p, "memory.max")) or _read_int(
        os.path.join(mem_p, "memory.limit_in_bytes")
    )
    mem_usage = _read_int(os.path.join(mem_p, "memory.current")) or _read_int(
        os.path.join(mem_p, "memory.usage_in_bytes")
    )

    cpu_quota = None
    cpu_period = None
    cpu_max_path = os.path.join(cpu_p, "cpu.max")
    try:
        if os.path.exists(cpu_max_path):
            # v2: "quota period" OR "max period"
            raw = open(cpu_max_path).read().strip()
            q, p = raw.split()
            cpu_quota = None if q == "max" else int(q)
            cpu_period = int(p)
        else:
            cpu_quota = _read_int(os.path.join(cpu_p, "cpu.cfs_quota_us"))
            cpu_period = _read_int(os.path.join(cpu_p, "cpu.cfs_period_us"))
    except Exception:
        pass

    return {
        "memory_limit_bytes": mem_limit,
        "memory_usage_bytes": mem_usage,
        "cpu_quota_us": cpu_quota,
        "cpu_period_us": cpu_period,
    }


def effective_cpu_count(lim: Dict[str, Optional[int]]) -> Optional[float]:
    q, p = lim.get("cpu_quota_us"), lim.get("cpu_period_us")
    if q is None or p in (None, 0):
        return None
    return max(0.0, float(q) / float(p))


# ------------ configuration ------------


@dataclass
class ReporterConfig:
    interval: float = 4.0
    warmup: float = 0.3
    workdir: str = "."
    emit_emf: bool = True
    namespace: str = "Pixetl/Batch"
    # EMF dimensions included on each metric row
    dimensions: Tuple[str, ...] = ("JobId", "Attempt")


# ------------ child process main loop ------------


class _StopFlag:
    """Signal-safe stop flag for the child process."""

    def __init__(self):
        self._stop = False

    def set(self, *_args):
        self._stop = True

    def is_set(self) -> bool:
        return self._stop


def _configure_logging():
    # Unbuffer Python I/O for prompt CloudWatch visibility
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(line_buffering=True)
            sys.stderr.reconfigure(line_buffering=True)
        except Exception:
            pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def _collect_snapshot(
    proc: psutil.Process, workdir: str, warmup: float
) -> Dict[str, float]:
    # System view (container-scoped)
    cpu_pct = psutil.cpu_percent(interval=warmup)
    virt = psutil.virtual_memory()
    swap = psutil.swap_memory()

    # Process + children RSS
    rss_self = proc.memory_info().rss
    rss_children = 0
    for c in proc.children(recursive=True):
        try:
            if c.is_running():
                rss_children += c.memory_info().rss
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            pass

    # Disk usage where you work (often /tmp or cwd)
    try:
        disk_used_pct = psutil.disk_usage(workdir).percent
    except Exception:
        disk_used_pct = float("nan")

    # cgroup limits/usage
    limits = read_container_limits()
    mem_limit = limits.get("memory_limit_bytes") or 0
    mem_used = limits.get("memory_usage_bytes") or 0
    cpu_limit = effective_cpu_count(limits)

    return {
        "ts": time.time(),
        "cpu_percent": float(cpu_pct),
        "mem_percent": float(virt.percent),
        "swap_percent": float(swap.percent),
        "disk_percent": float(disk_used_pct),
        "proc_rss_bytes": float(rss_self),
        "children_rss_bytes": float(rss_children),
        "cgroup_mem_used_bytes": float(mem_used),
        "cgroup_mem_limit_bytes": float(mem_limit),
        "cgroup_cpu_limit": float(cpu_limit) if cpu_limit is not None else float("nan"),
    }


def _log_human(log: logging.Logger, s: Dict[str, float]) -> None:
    # Keep line compact for CloudWatch readability
    pct_of_limit = (
        (s["cgroup_mem_used_bytes"] / s["cgroup_mem_limit_bytes"] * 100.0)
        if s["cgroup_mem_limit_bytes"] > 0
        else float("nan")
    )
    log.info(
        "TS:%d CPU:%.1f%% MEM:%.1f%% SWAP:%.1f%% DISK:%.1f%% RSS:%dB CHILD_RSS:%dB cgrpMem:%d/%dB(%.1f%%) cgrpCPU:%s",
        int(s["ts"]),
        s["cpu_percent"],
        s["mem_percent"],
        s["swap_percent"],
        s["disk_percent"],
        int(s["proc_rss_bytes"]),
        int(s["children_rss_bytes"]),
        int(s["cgroup_mem_used_bytes"]),
        int(s["cgroup_mem_limit_bytes"]),
        pct_of_limit,
        "NaN"
        if s["cgroup_cpu_limit"] != s["cgroup_cpu_limit"]
        else f"{s['cgroup_cpu_limit']:.2f}",
    )


def _log_emf(cfg: ReporterConfig, dims: Dict[str, str], s: Dict[str, float]) -> None:
    emf = {
        "_aws": {
            "Timestamp": int(s["ts"] * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": cfg.namespace,
                    "Dimensions": [list(cfg.dimensions)],
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
        **dims,
        "CPUPercent": round(s["cpu_percent"], 3),
        "MemPercent": round(s["mem_percent"], 3),
        "SwapPercent": round(s["swap_percent"], 3),
        "DiskPercent": round(s["disk_percent"], 3),
        "ProcRSS": int(s["proc_rss_bytes"]),
        "ChildrenRSS": int(s["children_rss_bytes"]),
        "CgroupMemUsed": int(s["cgroup_mem_used_bytes"]),
        "CgroupMemLimit": int(s["cgroup_mem_limit_bytes"]),
        "CgroupCPULimit": None
        if s["cgroup_cpu_limit"] != s["cgroup_cpu_limit"]
        else s["cgroup_cpu_limit"],
    }
    # One JSON per line; stdout; flush so it lands in CloudWatch quickly
    print(json.dumps(emf), flush=True)


def reporter_process_main(cfg: ReporterConfig) -> None:
    """Child process entrypoint.

    Never call in the parent—spawn this.
    """
    _configure_logging()
    log = logging.getLogger("pixetl.telemetry.proc")

    # Discover Batch context (safe defaults when running locally)
    dims = {
        "JobId": os.environ.get("AWS_BATCH_JOB_ID", "unknown"),
        "Attempt": os.environ.get("AWS_BATCH_JOB_ATTEMPT", "0"),
    }

    stop = _StopFlag()
    signal.signal(signal.SIGTERM, stop.set)
    signal.signal(signal.SIGINT, stop.set)

    # Prime CPU percent baseline
    psutil.cpu_percent(interval=None)
    proc = psutil.Process(os.getpid())

    log.warning(
        "Telemetry process starting (interval=%.2fs, workdir=%s)",
        cfg.interval,
        cfg.workdir,
    )

    # Drift-corrected loop
    nxt = time.monotonic()
    try:
        while not stop.is_set():
            snap = _collect_snapshot(proc, cfg.workdir, cfg.warmup)
            _log_human(log, snap)
            if cfg.emit_emf:
                _log_emf(cfg, dims, snap)
            nxt += cfg.interval
            # sleep with drift correction, but wake promptly on signals
            rem = max(0.1, nxt - time.monotonic())
            # simple sleep since signals will interrupt it
            time.sleep(rem)
    except Exception:
        log.exception("Telemetry process crashed")
    finally:
        log.warning("Telemetry process stopping")
        # Best-effort flush
        for h in logging.getLogger().handlers:
            try:
                h.flush()
            except Exception:
                pass
