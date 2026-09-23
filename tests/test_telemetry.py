import json
import logging
import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from gfw_pixetl.telemetry import (
    TELEMETRY_EVENT,
    TELEMETRY_SCHEMA_VERSION,
    ReporterConfig,
    ResourceReporter,
    effective_cpu_count,
    read_cgroup_stats,
)


def _write(path: Path, value: str) -> None:
    path.write_text(value)


@pytest.fixture
def cgroup_tmp_path():
    repo_tmp = Path(__file__).resolve().parents[1] / "tmp"
    repo_tmp.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=repo_tmp) as directory:
        yield Path(directory)


def test_read_cgroup_v2_stats(cgroup_tmp_path):
    _write(cgroup_tmp_path / "memory.max", "1073741824\n")
    _write(cgroup_tmp_path / "memory.current", "536870912\n")
    _write(cgroup_tmp_path / "memory.peak", "805306368\n")
    _write(
        cgroup_tmp_path / "memory.events",
        "low 0\nhigh 1\nmax 3\noom 2\noom_kill 1\noom_group_kill 0\n",
    )
    _write(cgroup_tmp_path / "cpu.max", "200000 100000\n")
    _write(
        cgroup_tmp_path / "cpu.stat",
        "usage_usec 123456\nuser_usec 100000\nsystem_usec 23456\n",
    )

    stats = read_cgroup_stats(str(cgroup_tmp_path))

    assert stats == {
        "memory_limit_bytes": 1073741824,
        "memory_usage_bytes": 536870912,
        "memory_peak_bytes": 805306368,
        "memory_oom_events": 2,
        "memory_oom_kills": 1,
        "cpu_quota_us": 200000,
        "cpu_period_us": 100000,
        "cpu_usage_usec": 123456,
    }
    assert effective_cpu_count(stats) == 2.0


def test_read_cgroup_v2_unlimited_or_missing_values(cgroup_tmp_path):
    _write(cgroup_tmp_path / "memory.max", "max\n")
    _write(cgroup_tmp_path / "memory.current", "123\n")
    _write(cgroup_tmp_path / "cpu.max", "max 100000\n")
    _write(cgroup_tmp_path / "cpu.stat", "usage_usec 456\n")

    stats = read_cgroup_stats(str(cgroup_tmp_path))

    assert stats["memory_limit_bytes"] is None
    assert stats["memory_peak_bytes"] is None
    assert stats["memory_oom_events"] is None
    assert stats["memory_oom_kills"] is None
    assert stats["cpu_quota_us"] is None
    assert stats["cpu_period_us"] == 100000
    assert effective_cpu_count(stats) is None


def test_reporter_monitors_supplied_parent_process():
    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"), ReporterConfig(emit_emf=False), os.getpid()
    )

    assert reporter._proc.pid == os.getpid()
    assert reporter._proc.pid != 0


def test_process_stats_excludes_telemetry_process(monkeypatch):
    proc = mock.Mock()
    proc.memory_info.return_value.rss = 100
    telemetry_child = mock.Mock()
    telemetry_child.pid = os.getpid()
    telemetry_child.is_running.return_value = True
    telemetry_child.memory_info.return_value.rss = 999
    worker_child = mock.Mock()
    worker_child.pid = os.getpid() + 1
    worker_child.is_running.return_value = True
    worker_child.memory_info.return_value.rss = 50
    stopped_child = mock.Mock()
    stopped_child.pid = os.getpid() + 2
    stopped_child.is_running.return_value = False
    proc.children.return_value = [telemetry_child, worker_child, stopped_child]

    monkeypatch.setattr("gfw_pixetl.telemetry.psutil.Process", lambda pid: proc)
    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"), ReporterConfig(emit_emf=False), 4242
    )

    assert reporter._process_stats() == (100, 50, 1)


def test_cgroup_cpu_usage_reports_cores_and_percent_of_quota():
    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"), ReporterConfig(emit_emf=False), os.getpid()
    )

    assert reporter._cgroup_cpu_usage(1_000_000, 2.0, 10.0) == (None, None)

    # 1 CPU-second consumed over 2 wall-seconds = 0.5 cores. With a 2-vCPU
    # quota, that is 25% of the cgroup's available CPU.
    cores, percent = reporter._cgroup_cpu_usage(2_000_000, 2.0, 12.0)
    assert cores == pytest.approx(0.5)
    assert percent == pytest.approx(25.0)


def test_cgroup_cpu_usage_reports_cores_without_a_quota():
    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"), ReporterConfig(emit_emf=False), os.getpid()
    )

    reporter._cgroup_cpu_usage(1_000_000, None, 10.0)
    cores, percent = reporter._cgroup_cpu_usage(3_000_000, None, 12.0)

    assert cores == pytest.approx(1.0)
    assert percent is None


def test_collect_snapshot_reports_analysis_metrics(cgroup_tmp_path):
    _write(cgroup_tmp_path / "memory.max", "1000\n")
    _write(cgroup_tmp_path / "memory.current", "250\n")
    _write(cgroup_tmp_path / "memory.peak", "400\n")
    _write(cgroup_tmp_path / "memory.events", "oom 2\noom_kill 1\n")
    _write(cgroup_tmp_path / "cpu.max", "100000 100000\n")
    _write(cgroup_tmp_path / "cpu.stat", "usage_usec 1000000\n")

    proc = mock.Mock()
    proc.memory_info.return_value.rss = 100
    child = mock.Mock()
    child.pid = os.getpid() + 1000
    child.is_running.return_value = True
    child.memory_info.return_value.rss = 50
    proc.children.return_value = [child]

    with (
        mock.patch("gfw_pixetl.telemetry.psutil.Process", return_value=proc),
        mock.patch("gfw_pixetl.telemetry.psutil.disk_usage") as disk_usage,
    ):
        disk_usage.return_value.percent = 12.5
        reporter = ResourceReporter(
            logging.getLogger("test.telemetry"),
            ReporterConfig(emit_emf=False, cgroup_root=str(cgroup_tmp_path)),
            4242,
        )
        snap = reporter._collect_snapshot()

    assert snap["proc_rss_bytes"] == 100
    assert snap["children_rss_bytes"] == 50
    assert snap["total_process_rss_bytes"] == 150
    assert snap["child_process_count"] == 1
    assert snap["process_count"] == 2
    assert snap["cgroup_mem_used_bytes"] == 250
    assert snap["cgroup_mem_peak_bytes"] == 400
    assert snap["cgroup_mem_limit_bytes"] == 1000
    assert snap["cgroup_mem_percent"] == 25.0
    assert snap["cgroup_oom_events"] == 2
    assert snap["cgroup_oom_kills"] == 1
    assert snap["disk_percent"] == 12.5


def test_snapshot_uses_cpu_affinity_when_cgroup_cpu_is_unlimited(
    monkeypatch, cgroup_tmp_path
):
    _write(cgroup_tmp_path / "memory.max", "1000\n")
    _write(cgroup_tmp_path / "memory.current", "250\n")
    _write(cgroup_tmp_path / "cpu.max", "max 100000\n")
    _write(cgroup_tmp_path / "cpu.stat", "usage_usec 1000000\n")

    monkeypatch.setattr(
        "gfw_pixetl.telemetry.os.sched_getaffinity", lambda pid: set(range(96))
    )
    monkeypatch.setattr("gfw_pixetl.telemetry.os.cpu_count", lambda: 96)
    monkeypatch.setattr(
        "gfw_pixetl.telemetry.psutil.disk_usage",
        lambda path: mock.Mock(percent=12.5),
    )

    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"),
        ReporterConfig(emit_emf=False, cgroup_root=str(cgroup_tmp_path)),
        os.getpid(),
    )
    monkeypatch.setattr(reporter, "_process_stats", lambda: (100, 50, 1))
    snap = reporter._collect_snapshot()

    assert snap["cgroup_cpu_limit"] is None
    assert snap["cpu_affinity_count"] == 96
    assert snap["host_cpu_count"] == 96
    assert snap["cpu_capacity"] == 96.0


def test_emf_has_stable_analysis_schema_and_strict_json(capsys):
    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"), ReporterConfig(), os.getpid()
    )
    snap = {
        "timestamp": 100.0,
        "disk_percent": 12.5,
        "proc_rss_bytes": 10,
        "children_rss_bytes": 20,
        "total_process_rss_bytes": 30,
        "child_process_count": 2,
        "process_count": 3,
        "cgroup_mem_used_bytes": 30,
        "cgroup_mem_peak_bytes": 35,
        "cgroup_mem_limit_bytes": 100,
        "cgroup_mem_percent": 30.0,
        "cgroup_oom_events": 1,
        "cgroup_oom_kills": 0,
        "cgroup_cpu_usage_usec": 40,
        "cgroup_cpu_cores_used": 1.5,
        "cgroup_cpu_limit": 2.0,
        "cgroup_cpu_percent": 75.0,
        "cpu_affinity_count": 96,
        "host_cpu_count": 96,
        "cpu_capacity": 96.0,
        "cpu_capacity_percent": 1.5625,
    }

    reporter._log_emf(snap)
    raw = capsys.readouterr().out.strip()
    payload = json.loads(raw, parse_constant=lambda value: pytest.fail(value))

    assert payload["event"] == TELEMETRY_EVENT
    assert payload["schema_version"] == TELEMETRY_SCHEMA_VERSION
    assert payload["ParentPid"] == os.getpid()
    assert payload["ProcessCount"] == 3
    assert payload["ChildProcessCount"] == 2
    assert payload["TotalProcessRSS"] == 30
    assert payload["CgroupMemPeak"] == 35
    assert payload["CgroupCPUCoresUsed"] == 1.5
    assert payload["CPUCapacity"] == 96.0
    assert payload["CPUAffinityCount"] == 96
    assert payload["CgroupOOMEvents"] == 1
    assert payload["CgroupOOMKills"] == 0

    metric_names = {
        item["Name"] for item in payload["_aws"]["CloudWatchMetrics"][0]["Metrics"]
    }
    assert metric_names == {
        "DiskPercent",
        "ProcessCount",
        "ChildProcessCount",
        "ProcRSS",
        "ChildrenRSS",
        "TotalProcessRSS",
        "CgroupMemUsed",
        "CgroupMemPeak",
        "CgroupMemLimit",
        "CgroupMemPercent",
        "CgroupOOMEvents",
        "CgroupOOMKills",
        "CgroupCPUUsage",
        "CgroupCPUCoresUsed",
        "CgroupCPULimit",
        "CgroupCPUPercent",
        "CPUAffinityCount",
        "HostCPUCount",
        "CPUCapacity",
        "CPUCapacityPercent",
    }


def test_emf_omits_unavailable_values(capsys):
    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"), ReporterConfig(), os.getpid()
    )
    snap = {
        "timestamp": 100.0,
        "disk_percent": None,
        "proc_rss_bytes": 10,
        "children_rss_bytes": 20,
        "total_process_rss_bytes": 30,
        "child_process_count": 2,
        "process_count": 3,
        "cgroup_mem_used_bytes": 30,
        "cgroup_mem_peak_bytes": None,
        "cgroup_mem_limit_bytes": None,
        "cgroup_mem_percent": None,
        "cgroup_oom_events": None,
        "cgroup_oom_kills": None,
        "cgroup_cpu_usage_usec": 40,
        "cgroup_cpu_cores_used": None,
        "cgroup_cpu_limit": None,
        "cgroup_cpu_percent": None,
        "cpu_affinity_count": 96,
        "host_cpu_count": 96,
        "cpu_capacity": 96.0,
        "cpu_capacity_percent": None,
    }

    reporter._log_emf(snap)
    payload = json.loads(capsys.readouterr().out.strip())

    assert "DiskPercent" not in payload
    assert "CgroupMemPeak" not in payload
    assert "CgroupCPUCoresUsed" not in payload
    assert "CgroupCPULimit" not in payload
    assert payload["TotalProcessRSS"] == 30


def test_resource_reporter_does_not_install_signal_handlers(monkeypatch):
    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"), ReporterConfig(interval=0.01), os.getpid()
    )
    signal_mock = mock.Mock()
    monkeypatch.setattr("gfw_pixetl.telemetry.signal.signal", signal_mock)
    monkeypatch.setattr(reporter, "_run", lambda: None)

    reporter.start()
    reporter.stop()

    signal_mock.assert_not_called()
