import os
from copy import deepcopy
from unittest import mock

from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.pipes import RasterPipe
from gfw_pixetl.pixetl import pixetl
from tests.conftest import minimal_layer_dict

LAYER_DICT = deepcopy(minimal_layer_dict)
LAYER_DICT.update(
    {
        "dataset": "aqueduct_erosion_risk",
        "version": "v201911",
        "pixel_meaning": "level",
        "grid": "1/4000",
    }
)

RASTER_LAYER_DEF = LayerModel.parse_obj(LAYER_DICT)

SUBSET = ["10N_010E"]


def test_pixetl():

    cwd = os.getcwd()

    with mock.patch.object(
        RasterPipe, "create_tiles", return_value=(list(), list(), list(), list())
    ):
        tiles, skipped_tiles, failed_tiles, existing_tiles = pixetl(
            RASTER_LAYER_DEF,
            subset=SUBSET,
            overwrite=True,
        )

    assert tiles == list()
    assert skipped_tiles == list()
    assert failed_tiles == list()
    assert existing_tiles == list()
    assert cwd == os.getcwd()

    os.chdir(cwd)


class _FakeReporterManager:
    instances = []

    def __init__(self, cfg, logger=None):
        self.cfg = cfg
        self.logger = logger
        self.start_calls = []
        self.stop_calls = 0
        type(self).instances.append(self)

    def start(self, parent_pid=None):
        self.start_calls.append(parent_pid)

    def stop(self):
        self.stop_calls += 1


def test_main_stops_telemetry_when_cli_raises(monkeypatch):
    import gfw_pixetl.pixetl as pixetl_module

    _FakeReporterManager.instances.clear()
    monkeypatch.setattr(pixetl_module, "ReporterManager", _FakeReporterManager)
    monkeypatch.setattr(pixetl_module, "setup_logging", mock.Mock())
    monkeypatch.setattr(
        pixetl_module, "cli", mock.Mock(side_effect=RuntimeError("boom"))
    )

    with (
        mock.patch.object(
            pixetl_module.signal, "getsignal", return_value=mock.sentinel.old
        ),
        mock.patch.object(pixetl_module.signal, "signal"),
    ):
        try:
            pixetl_module.main()
        except RuntimeError as exc:
            assert str(exc) == "boom"
        else:
            raise AssertionError("main() should propagate the CLI exception")

    reporter = _FakeReporterManager.instances[0]
    assert reporter.start_calls == [os.getpid()]
    assert reporter.stop_calls == 1


def test_main_sigterm_unwinds_through_cleanup(monkeypatch):
    import gfw_pixetl.pixetl as pixetl_module

    _FakeReporterManager.instances.clear()
    handlers = {}

    def fake_signal(sig, handler):
        handlers[sig] = handler

    def fake_cli():
        handlers[pixetl_module.signal.SIGTERM](pixetl_module.signal.SIGTERM, None)

    monkeypatch.setattr(pixetl_module, "ReporterManager", _FakeReporterManager)
    monkeypatch.setattr(pixetl_module, "setup_logging", mock.Mock())
    monkeypatch.setattr(pixetl_module, "cli", fake_cli)
    monkeypatch.setattr(
        pixetl_module.signal, "getsignal", lambda sig: mock.sentinel.old
    )
    monkeypatch.setattr(pixetl_module.signal, "signal", fake_signal)

    try:
        pixetl_module.main()
    except SystemExit as exc:
        assert exc.code == 143
    else:
        raise AssertionError("SIGTERM should terminate main() with exit code 143")

    reporter = _FakeReporterManager.instances[0]
    assert reporter.start_calls == [os.getpid()]
    assert reporter.stop_calls == 1


def test_console_script_runs_main():
    from pathlib import Path

    import tomllib

    with (Path(__file__).parents[1] / "pyproject.toml").open("rb") as src:
        pyproject = tomllib.load(src)

    assert pyproject["project"]["scripts"]["pixetl"] == "gfw_pixetl.pixetl:main"
