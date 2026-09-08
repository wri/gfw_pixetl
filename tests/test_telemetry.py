import json
import logging
import os
from pathlib import Path
from unittest import mock

import pytest

from gfw_pixetl.telemetry import (
    ReporterConfig,
    ResourceReporter,
    effective_cpu_count,
    read_cgroup_stats,
)


def _write(path: Path, value: str) -> None:
    path.write_text(value)


def test_read_cgroup_v2_stats(tmp_path):
    _write(tmp_path / "memory.max", "1073741824\n")
    _write(tmp_path / "memory.current", "536870912\n")
    _write(tmp_path / "cpu.max", "200000 100000\n")
    _write(
        tmp_path / "cpu.stat",
        "usage_usec 123456\nuser_usec 100000\nsystem_usec 23456\n",
    )

    stats = read_cgroup_stats(str(tmp_path))

    assert stats == {
        "memory_limit_bytes": 1073741824,
        "memory_usage_bytes": 536870912,
        "cpu_quota_us": 200000,
        "cpu_period_us": 100000,
        "cpu_usage_usec": 123456,
    }
    assert effective_cpu_count(stats) == 2.0


def test_read_cgroup_v2_unlimited_values(tmp_path):
    _write(tmp_path / "memory.max", "max\n")
    _write(tmp_path / "memory.current", "123\n")
    _write(tmp_path / "cpu.max", "max 100000\n")
    _write(tmp_path / "cpu.stat", "usage_usec 456\n")

    stats = read_cgroup_stats(str(tmp_path))

    assert stats["memory_limit_bytes"] is None
    assert stats["cpu_quota_us"] is None
    assert stats["cpu_period_us"] == 100000
    assert effective_cpu_count(stats) is None


def test_reporter_monitors_supplied_parent_process():
    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"), ReporterConfig(emit_emf=False), os.getpid()
    )

    assert reporter._proc.pid == os.getpid()
    assert reporter._proc.pid != 0


def test_process_memory_excludes_telemetry_process(monkeypatch):
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
    proc.children.return_value = [telemetry_child, worker_child]

    monkeypatch.setattr("gfw_pixetl.telemetry.psutil.Process", lambda pid: proc)
    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"), ReporterConfig(emit_emf=False), 4242
    )

    assert reporter._process_memory() == (100, 50)


def test_cgroup_cpu_percent_uses_usage_delta_and_cpu_quota():
    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"), ReporterConfig(emit_emf=False), os.getpid()
    )

    assert reporter._cgroup_cpu_percent(1_000_000, 2.0, 10.0) is None
    # 1 CPU-second consumed over 2 wall-seconds with a 2-vCPU quota = 25%.
    assert reporter._cgroup_cpu_percent(2_000_000, 2.0, 12.0) == pytest.approx(25.0)


def test_collect_snapshot_reports_parent_and_children_memory(tmp_path):
    _write(tmp_path / "memory.max", "1000\n")
    _write(tmp_path / "memory.current", "250\n")
    _write(tmp_path / "cpu.max", "100000 100000\n")
    _write(tmp_path / "cpu.stat", "usage_usec 1000000\n")

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
            ReporterConfig(emit_emf=False, cgroup_root=str(tmp_path)),
            4242,
        )
        snap = reporter._collect_snapshot()

    assert snap["proc_rss_bytes"] == 100
    assert snap["children_rss_bytes"] == 50
    assert snap["cgroup_mem_used_bytes"] == 250
    assert snap["cgroup_mem_limit_bytes"] == 1000
    assert snap["cgroup_mem_percent"] == 25.0
    assert snap["disk_percent"] == 12.5


def test_emf_omits_unavailable_values_and_is_strict_json(capsys):
    reporter = ResourceReporter(
        logging.getLogger("test.telemetry"), ReporterConfig(), os.getpid()
    )
    snap = {
        "timestamp": 100.0,
        "disk_percent": None,
        "proc_rss_bytes": 10,
        "children_rss_bytes": 20,
        "cgroup_mem_used_bytes": 30,
        "cgroup_mem_limit_bytes": None,
        "cgroup_mem_percent": None,
        "cgroup_cpu_usage_usec": 40,
        "cgroup_cpu_limit": None,
        "cgroup_cpu_percent": None,
    }

    reporter._log_emf(snap)
    raw = capsys.readouterr().out.strip()
    payload = json.loads(raw, parse_constant=lambda value: pytest.fail(value))

    assert payload["ProcRSS"] == 10
    assert payload["ChildrenRSS"] == 20
    assert payload["CgroupMemUsed"] == 30
    assert payload["CgroupCPUUsage"] == 40
    assert "DiskPercent" not in payload
    assert "CgroupCPULimit" not in payload
    metric_names = {
        item["Name"] for item in payload["_aws"]["CloudWatchMetrics"][0]["Metrics"]
    }
    assert metric_names == {
        "ProcRSS",
        "ChildrenRSS",
        "CgroupMemUsed",
        "CgroupCPUUsage",
    }


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
