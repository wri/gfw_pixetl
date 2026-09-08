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


def _read_cpu_usage_usec(path: str) -> Optional[int]:
    raw = _read_text(path)
    if raw is None:
        return None

    for line in raw.splitlines():
        key, _, value = line.partition(" ")
        if key == "usage_usec":
            try:
                return int(value)
            except ValueError:
                return None
    return None


def read_cgroup_stats(cgroup_root: str = CGROUP_ROOT) -> Dict[str, Optional[int]]:
    """Read resource accounting from a cgroups v2 hierarchy.

    pixetl's Batch runtime is expected to use cgroups v2.  Local
    environments without a mounted v2 hierarchy simply return
    unavailable values.
    """
    quota, period = _read_cpu_max(os.path.join(cgroup_root, "cpu.max"))
    return {
        "memory_limit_bytes": _read_int(os.path.join(cgroup_root, "memory.max")),
        "memory_usage_bytes": _read_int(os.path.join(cgroup_root, "memory.current")),
        "cpu_quota_us": quota,
        "cpu_period_us": period,
        "cpu_usage_usec": _read_cpu_usage_usec(os.path.join(cgroup_root, "cpu.stat")),
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

    def _process_memory(self) -> Tuple[Optional[int], Optional[int]]:
        try:
            rss_self = self._proc.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None, None

        rss_children = 0
        try:
            children = self._proc.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            children = []

        reporter_pid = os.getpid()
        for child in children:
            try:
                if child.pid == reporter_pid:
                    continue
                if child.is_running():
                    rss_children += child.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        return rss_self, rss_children

    def _cgroup_cpu_percent(
        self, cpu_usage_usec: Optional[int], cpu_limit: Optional[float], now: float
    ) -> Optional[float]:
        previous_usage = self._previous_cpu_usage_usec
        previous_time = self._previous_cpu_sample_monotonic
        self._previous_cpu_usage_usec = cpu_usage_usec
        self._previous_cpu_sample_monotonic = now

        if (
            cpu_usage_usec is None
            or cpu_limit is None
            or cpu_limit <= 0
            or previous_usage is None
            or previous_time is None
        ):
            return None

        elapsed = now - previous_time
        delta_usage_usec = cpu_usage_usec - previous_usage
        if elapsed <= 0 or delta_usage_usec < 0:
            return None

        used_cpu_seconds = delta_usage_usec / 1_000_000.0
        return (used_cpu_seconds / elapsed / cpu_limit) * 100.0

    def _collect_snapshot(self) -> Snapshot:
        now_monotonic = time.monotonic()
        timestamp = time.time()
        rss_self, rss_children = self._process_memory()

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

        mem_percent = None
        if mem_limit is not None and mem_limit > 0 and mem_usage is not None:
            mem_percent = (mem_usage / float(mem_limit)) * 100.0

        cpu_percent = self._cgroup_cpu_percent(cpu_usage_usec, cpu_limit, now_monotonic)

        return {
            "timestamp": timestamp,
            "disk_percent": disk_percent,
            "proc_rss_bytes": rss_self,
            "children_rss_bytes": rss_children,
            "cgroup_mem_used_bytes": mem_usage,
            "cgroup_mem_limit_bytes": mem_limit,
            "cgroup_mem_percent": mem_percent,
            "cgroup_cpu_usage_usec": cpu_usage_usec,
            "cgroup_cpu_limit": cpu_limit,
            "cgroup_cpu_percent": cpu_percent,
        }

    @staticmethod
    def _display(value: Optional[Number], fmt: str = ".1f") -> str:
        if value is None:
            return "n/a"
        return format(value, fmt)

    def _log_human(self, snap: Snapshot) -> None:
        self.log.info(
            "TS:%d cgrpCPU:%s%%/%s-vCPU cgrpMem:%s/%sB(%s%%) "
            "RSS(proc):%sB RSS(children):%sB DISK:%s%%",
            int(snap["timestamp"] or 0),
            self._display(snap["cgroup_cpu_percent"]),
            self._display(snap["cgroup_cpu_limit"], ".2f"),
            self._display(snap["cgroup_mem_used_bytes"], ".0f"),
            self._display(snap["cgroup_mem_limit_bytes"], ".0f"),
            self._display(snap["cgroup_mem_percent"]),
            self._display(snap["proc_rss_bytes"], ".0f"),
            self._display(snap["children_rss_bytes"], ".0f"),
            self._display(snap["disk_percent"]),
        )

    def _log_emf(self, snap: Snapshot) -> None:
        definitions = {
            "DiskPercent": ("disk_percent", "Percent"),
            "ProcRSS": ("proc_rss_bytes", "Bytes"),
            "ChildrenRSS": ("children_rss_bytes", "Bytes"),
            "CgroupMemUsed": ("cgroup_mem_used_bytes", "Bytes"),
            "CgroupMemLimit": ("cgroup_mem_limit_bytes", "Bytes"),
            "CgroupMemPercent": ("cgroup_mem_percent", "Percent"),
            "CgroupCPUUsage": ("cgroup_cpu_usage_usec", "Microseconds"),
            "CgroupCPULimit": ("cgroup_cpu_limit", "Count"),
            "CgroupCPUPercent": ("cgroup_cpu_percent", "Percent"),
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
        except Exception as exc:
            self.log.exception("ResourceReporter crashed: %s", exc)


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
