import os

from gfw_pixetl.settings.globals import Globals, available_cpu_count


def test_available_cpu_count_prefers_affinity(monkeypatch):
    monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: {0, 2, 4})
    assert available_cpu_count() == 3


def test_available_cpu_count_falls_back_to_cpu_count(monkeypatch):
    monkeypatch.delattr(os, "sched_getaffinity", raising=False)
    monkeypatch.setattr(os, "cpu_count", lambda: 6)
    assert available_cpu_count() == 6


def test_process_workers_default_and_env_override(monkeypatch):
    monkeypatch.delenv("PROCESS_WORKERS", raising=False)
    assert Globals().process_workers == available_cpu_count()

    monkeypatch.setenv("PROCESS_WORKERS", "2")
    assert Globals().process_workers == 2


def test_memory_admission_defaults():
    config = Globals()
    assert config.memory_admission_enabled is True
    assert config.memory_admission_high_watermark == 0.80
    assert config.memory_admission_resume_watermark == 0.75
    assert config.memory_admission_reservation_gib == 4.0
    assert config.memory_admission_window_reservation_gib == 4.0
    assert config.raster_stats_reservation_gib == 12.0
    assert config.memory_admission_poll_seconds == 1.0


def test_download_workers_default_and_env_override(monkeypatch):
    monkeypatch.delenv("DOWNLOAD_WORKERS", raising=False)
    assert Globals().download_workers == 8

    monkeypatch.setenv("DOWNLOAD_WORKERS", "12")
    assert Globals().download_workers == 12


def test_upload_workers_default_and_env_override(monkeypatch):
    monkeypatch.delenv("UPLOAD_WORKERS", raising=False)
    assert Globals().upload_workers == 8

    monkeypatch.setenv("UPLOAD_WORKERS", "12")
    assert Globals().upload_workers == 12


def test_cleanup_workers_default_and_env_override(monkeypatch):
    monkeypatch.delenv("CLEANUP_WORKERS", raising=False)
    assert Globals().cleanup_workers == 4

    monkeypatch.setenv("CLEANUP_WORKERS", "3")
    assert Globals().cleanup_workers == 3
