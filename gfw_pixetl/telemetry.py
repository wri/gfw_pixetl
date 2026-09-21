import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import psutil

from gfw_pixetl import get_module_logger
from gfw_pixetl.logs import configure_worker_logging

CGROUP_ROOT = "/sys/fs/cgroup"
TELEMETRY_EVENT = "pixetl.telemetry"
TELEMETRY_SCHEMA_VERSION = 1
Number = Union[int, float]
Snapshot = Dict[str, Optional[Number]]


def _read_text(path: str) -> Optional[str]:
    try:
        with open(path, "r") as f:
            value = f.read().strip()
        return value or None
    except (OSError, ValueError):
        return None


def _read_int(path: str) -> Optional[int]:
    value = _read_text(path)
    if value in (None, "max"):
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _read_cpu_max(path: str) -> Tuple[Optional[int], Optional[int]]:
    raw = _read_text(path)
    if raw is None:
        return None, None

    parts = raw.split()
    if len(parts) != 2:
        return None, None

    quota_raw, period_raw = parts
    try:
        period = int(period_raw)
        quota = None if quota_raw == "max" else int(quota_raw)
    except ValueError:
        return None, None
    return quota, period


def _read_keyed_int(path: str, key: str) -> Optional[int]:
    raw = _read_text(path)
    if raw is None:
        return None

    for line in raw.splitlines():
        item_key, _, value = line.partition(" ")
        if item_key != key:
            continue
        try:
            return int(value)
        except ValueError:
            return None
    return None


def read_cgroup_stats(cgroup_root: str = CGROUP_ROOT) -> Dict[str, Optional[int]]:
    """Read resource accounting from the pixetl cgroups v2 hierarchy.

    pixetl's Batch runtime is expected to use cgroups v2. Local
    environments without a mounted v2 hierarchy simply return
    unavailable values.
    """
    quota, period = _read_cpu_max(os.path.join(cgroup_root, "cpu.max"))
    cpu_stat = os.path.join(cgroup_root, "cpu.stat")
    memory_events = os.path.join(cgroup_root, "memory.events")
    return {
        "memory_limit_bytes": _read_int(os.path.join(cgroup_root, "memory.max")),
        "memory_usage_bytes": _read_int(os.path.join(cgroup_root, "memory.current")),
        "memory_peak_bytes": _read_int(os.path.join(cgroup_root, "memory.peak")),
        "memory_oom_events": _read_keyed_int(memory_events, "oom"),
        "memory_oom_kills": _read_keyed_int(memory_events, "oom_kill"),
        "cpu_quota_us": quota,
        "cpu_period_us": period,
        "cpu_usage_usec": _read_keyed_int(cpu_stat, "usage_usec"),
    }


def effective_cpu_count(stats: Dict[str, Optional[int]]) -> Optional[float]:
    """Return the cgroup v2 CPU quota as a number of vCPUs, when
    constrained."""
    quota = stats.get("cpu_quota_us")
    period = stats.get("cpu_period_us")
    if quota is None or period in (None, 0):
        return None
    return max(0.0, float(quota) / float(period))


@dataclass
class ReporterConfig:
    interval: float = 4.0
    workdir: str = "."
    emit_emf: bool = True
    namespace: str = "Pixetl/Batch"
    dimensions: Tuple[str, ...] = ("JobId", "Attempt")
    cgroup_root: str = CGROUP_ROOT


class ResourceReporter:
    """Report resource use for the pixetl parent process and its cgroup."""

    def __init__(self, logger: logging.Logger, cfg: ReporterConfig, parent_pid: int):
        self.log = logger
        self.cfg = cfg
        self.parent_pid = parent_pid
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._proc = psutil.Process(parent_pid)
        self._previous_cpu_usage_usec: Optional[int] = None
        self._previous_cpu_sample_monotonic: Optional[float] = None

        self.job_id = os.environ.get("AWS_BATCH_JOB_ID", "unknown")
        self.attempt = os.environ.get("AWS_BATCH_JOB_ATTEMPT", "0")

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run, name="pixetl-resource-reporter", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.cfg.interval + 1.0)

    def _process_stats(
        self,
    ) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """Return parent RSS, child RSS, and live child count.

        The dedicated telemetry process is itself a child of pixetl, so
        it is explicitly excluded from workload child accounting.
        """
        try:
            rss_self = self._proc.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None, None, None

        rss_children = 0
        child_count = 0
        try:
            children = self._proc.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            children = []

        reporter_pid = os.getpid()
        for child in children:
            try:
                if child.pid == reporter_pid or not child.is_running():
                    continue
                child_count += 1
                rss_children += child.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        return rss_self, rss_children, child_count

    def _cgroup_cpu_usage(
        self, cpu_usage_usec: Optional[int], cpu_limit: Optional[float], now: float
    ) -> Tuple[Optional[float], Optional[float]]:
        """Return CPU cores consumed and percent of the configured CPU
        quota."""
        previous_usage = self._previous_cpu_usage_usec
        previous_time = self._previous_cpu_sample_monotonic
        self._previous_cpu_usage_usec = cpu_usage_usec
        self._previous_cpu_sample_monotonic = now

        if cpu_usage_usec is None or previous_usage is None or previous_time is None:
            return None, None

        elapsed = now - previous_time
        delta_usage_usec = cpu_usage_usec - previous_usage
        if elapsed <= 0 or delta_usage_usec < 0:
            return None, None

        used_cpu_seconds = delta_usage_usec / 1_000_000.0
        cores_used = used_cpu_seconds / elapsed
        cpu_percent = None
        if cpu_limit is not None and cpu_limit > 0:
            cpu_percent = (cores_used / cpu_limit) * 100.0

        return cores_used, cpu_percent

    def _collect_snapshot(self) -> Snapshot:
        now_monotonic = time.monotonic()
        timestamp = time.time()
        rss_self, rss_children, child_count = self._process_stats()

        try:
            disk_percent: Optional[float] = float(
                psutil.disk_usage(self.cfg.workdir).percent
            )
        except (OSError, ValueError):
            disk_percent = None

        stats = read_cgroup_stats(self.cfg.cgroup_root)
        mem_limit = stats.get("memory_limit_bytes")
        mem_usage = stats.get("memory_usage_bytes")
        cpu_usage_usec = stats.get("cpu_usage_usec")
        cpu_limit = effective_cpu_count(stats)
        try:
            cpu_affinity_count: Optional[int] = len(os.sched_getaffinity(0))  # type: ignore[attr-defined]
        except (AttributeError, OSError, ProcessLookupError):
            cpu_affinity_count = None
        host_cpu_count = os.cpu_count()
        cpu_capacity: Optional[float] = cpu_limit
        if cpu_capacity is None and cpu_affinity_count is not None:
            cpu_capacity = float(cpu_affinity_count)
        if cpu_capacity is None and host_cpu_count is not None:
            cpu_capacity = float(host_cpu_count)

        mem_percent = None
        if mem_limit is not None and mem_limit > 0 and mem_usage is not None:
            mem_percent = (mem_usage / float(mem_limit)) * 100.0

        cpu_cores_used, cpu_percent = self._cgroup_cpu_usage(
            cpu_usage_usec, cpu_limit, now_monotonic
        )
        cpu_capacity_percent = None
        if cpu_cores_used is not None and cpu_capacity is not None and cpu_capacity > 0:
            cpu_capacity_percent = (cpu_cores_used / cpu_capacity) * 100.0

        total_process_rss = None
        process_count = None
        if rss_self is not None and rss_children is not None:
            total_process_rss = rss_self + rss_children
        if rss_self is not None and child_count is not None:
            process_count = child_count + 1

        return {
            "timestamp": timestamp,
            "disk_percent": disk_percent,
            "proc_rss_bytes": rss_self,
            "children_rss_bytes": rss_children,
            "total_process_rss_bytes": total_process_rss,
            "child_process_count": child_count,
            "process_count": process_count,
            "cgroup_mem_used_bytes": mem_usage,
            "cgroup_mem_peak_bytes": stats.get("memory_peak_bytes"),
            "cgroup_mem_limit_bytes": mem_limit,
            "cgroup_mem_percent": mem_percent,
            "cgroup_oom_events": stats.get("memory_oom_events"),
            "cgroup_oom_kills": stats.get("memory_oom_kills"),
            "cgroup_cpu_usage_usec": cpu_usage_usec,
            "cgroup_cpu_cores_used": cpu_cores_used,
            "cgroup_cpu_limit": cpu_limit,
            "cgroup_cpu_percent": cpu_percent,
            "cpu_affinity_count": cpu_affinity_count,
            "host_cpu_count": host_cpu_count,
            "cpu_capacity": cpu_capacity,
            "cpu_capacity_percent": cpu_capacity_percent,
        }

    @staticmethod
    def _display(value: Optional[Number], fmt: str = ".1f") -> str:
        if value is None:
            return "n/a"
        return format(value, fmt)

    def _log_human(self, snap: Snapshot) -> None:
        self.log.info(
            "TS:%d procs:%s CPU:%s/%s-vCPU(%s%%) "
            "cgrpMem:%s/%sB(%s%%) peak:%sB "
            "RSS(total):%sB DISK:%s%% OOM:%s kills:%s",
            int(snap["timestamp"] or 0),
            self._display(snap["process_count"], ".0f"),
            self._display(snap["cgroup_cpu_cores_used"], ".2f"),
            self._display(snap["cpu_capacity"], ".2f"),
            self._display(snap["cpu_capacity_percent"]),
            self._display(snap["cgroup_mem_used_bytes"], ".0f"),
            self._display(snap["cgroup_mem_limit_bytes"], ".0f"),
            self._display(snap["cgroup_mem_percent"]),
            self._display(snap["cgroup_mem_peak_bytes"], ".0f"),
            self._display(snap["total_process_rss_bytes"], ".0f"),
            self._display(snap["disk_percent"]),
            self._display(snap["cgroup_oom_events"], ".0f"),
            self._display(snap["cgroup_oom_kills"], ".0f"),
        )

    def _log_emf(self, snap: Snapshot) -> None:
        definitions = {
            "DiskPercent": ("disk_percent", "Percent"),
            "ProcessCount": ("process_count", "Count"),
            "ChildProcessCount": ("child_process_count", "Count"),
            "ProcRSS": ("proc_rss_bytes", "Bytes"),
            "ChildrenRSS": ("children_rss_bytes", "Bytes"),
            "TotalProcessRSS": ("total_process_rss_bytes", "Bytes"),
            "CgroupMemUsed": ("cgroup_mem_used_bytes", "Bytes"),
            "CgroupMemPeak": ("cgroup_mem_peak_bytes", "Bytes"),
            "CgroupMemLimit": ("cgroup_mem_limit_bytes", "Bytes"),
            "CgroupMemPercent": ("cgroup_mem_percent", "Percent"),
            "CgroupOOMEvents": ("cgroup_oom_events", "Count"),
            "CgroupOOMKills": ("cgroup_oom_kills", "Count"),
            "CgroupCPUUsage": ("cgroup_cpu_usage_usec", "Microseconds"),
            "CgroupCPUCoresUsed": ("cgroup_cpu_cores_used", "Count"),
            "CgroupCPULimit": ("cgroup_cpu_limit", "Count"),
            "CgroupCPUPercent": ("cgroup_cpu_percent", "Percent"),
            "CPUAffinityCount": ("cpu_affinity_count", "Count"),
            "HostCPUCount": ("host_cpu_count", "Count"),
            "CPUCapacity": ("cpu_capacity", "Count"),
            "CPUCapacityPercent": ("cpu_capacity_percent", "Percent"),
        }

        metrics: List[Dict[str, str]] = []
        values: Dict[str, Number] = {}
        for metric_name, (snapshot_name, unit) in definitions.items():
            value = snap.get(snapshot_name)
            if value is None:
                continue
            metrics.append({"Name": metric_name, "Unit": unit})
            values[metric_name] = value

        if not metrics:
            return

        emf = {
            "event": TELEMETRY_EVENT,
            "schema_version": TELEMETRY_SCHEMA_VERSION,
            "_aws": {
                "Timestamp": int((snap["timestamp"] or 0) * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": self.cfg.namespace,
                        "Dimensions": [list(self.cfg.dimensions)],
                        "Metrics": metrics,
                    }
                ],
            },
            "JobId": self.job_id,
            "Attempt": self.attempt,
            "ParentPid": self.parent_pid,
            **values,
        }
        print(json.dumps(emf, allow_nan=False), flush=True)

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
                self._stop.wait(timeout=max(0.1, next_tick - time.monotonic()))
        except Exception:
            self.log.exception("Resource reporter failed")


def telemetry_worker(stop_evt, parent_pid: int, cfg_dict: Dict) -> None:
    """Entry point for the dedicated telemetry process."""
    configure_worker_logging("INFO")
    log = get_module_logger("pixetl.telemetry.proc")

    cfg = ReporterConfig(**cfg_dict)
    try:
        reporter = ResourceReporter(logger=log, cfg=cfg, parent_pid=parent_pid)
    except psutil.NoSuchProcess:
        log.warning(
            "pixetl parent process %s no longer exists; telemetry exiting", parent_pid
        )
        return

    local_stop = threading.Event()

    def _handle_term(signum, frame):
        log.info("Telemetry process received signal %s, shutting down", signum)
        local_stop.set()

    signal.signal(signal.SIGTERM, _handle_term)
    signal.signal(signal.SIGINT, _handle_term)

    log.info(
        "Telemetry process starting for pixetl PID %s "
        "(interval=%.2fs, workdir=%s, namespace=%s)",
        parent_pid,
        cfg.interval,
        cfg.workdir,
        cfg.namespace,
    )

    reporter.start()
    try:
        while not (stop_evt.is_set() or local_stop.is_set()):
            if not psutil.pid_exists(parent_pid):
                log.warning(
                    "pixetl parent process %s exited; telemetry stopping", parent_pid
                )
                break
            local_stop.wait(timeout=0.5)
    finally:
        log.info("Telemetry process stopping reporter")
        reporter.stop()
        for handler in logging.getLogger().handlers:
            try:
                handler.flush()
            except Exception:
                pass
        log.info("Telemetry process exiting")
