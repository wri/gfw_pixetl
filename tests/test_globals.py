import os
from copy import deepcopy

from gfw_pixetl.settings.globals import Globals


def test_global_workers():
    config = Globals()
    assert config.num_processes == os.cpu_count()
    assert config.workers == os.cpu_count()

    config.workers = os.cpu_count() + 1
    assert config.num_processes == os.cpu_count()
    assert config.workers == os.cpu_count()

    config.num_processes = 1
    assert config.num_processes == 1
    assert config.workers == 1

    config = Globals(num_processes=2, workers=3)
    assert config.num_processes == 2
    assert config.workers == 2

    config = Globals(cores=4, num_processes=3, workers=2)
    assert config.num_processes == 3
    assert config.workers == 2

    vars = deepcopy(os.environ)

    os.environ["NUM_PROCESSES"] = "2"
    config = Globals(workers=3)
    if os.cpu_count() >= 2:
        assert config.num_processes == 2
        assert config.workers == 2
    else:
        assert config.num_processes == 1
        assert config.workers == 1

    os.environ = vars


def test_download_workers_default_and_env_override(monkeypatch):
    monkeypatch.delenv("DOWNLOAD_WORKERS", raising=False)
    assert Globals().download_workers == 8

    monkeypatch.setenv("DOWNLOAD_WORKERS", "12")
    assert Globals().download_workers == 12


def test_pipeline_io_worker_defaults_and_env_overrides(monkeypatch):
    monkeypatch.delenv("UPLOAD_WORKERS", raising=False)
    monkeypatch.delenv("CLEANUP_WORKERS", raising=False)
    assert Globals().upload_workers == 8
    assert Globals().cleanup_workers == 4

    monkeypatch.setenv("UPLOAD_WORKERS", "12")
    monkeypatch.setenv("CLEANUP_WORKERS", "3")
    config = Globals()
    assert config.upload_workers == 12
    assert config.cleanup_workers == 3
