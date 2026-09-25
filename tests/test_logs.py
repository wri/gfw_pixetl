import json
import logging

from gfw_pixetl.logs import configure_worker_logging, setup_logging


def test_setup_logging_is_idempotent_and_emits_json(monkeypatch, capsys):
    monkeypatch.setenv("PIXETL_LOG_JSON", "1")

    first = setup_logging("INFO")
    second = setup_logging("INFO")

    root = logging.getLogger()
    pixetl_handlers = [
        handler
        for handler in root.handlers
        if getattr(handler, "_pixetl_handler", False)
    ]
    assert pixetl_handlers == [second]
    assert first is not second

    logging.getLogger("pixetl.test").info("hello")
    payload = json.loads(capsys.readouterr().out.strip())

    assert payload["level"] == "INFO"
    assert payload["logger"] == "pixetl.test"
    assert payload["msg"] == "hello"
    assert payload["process"]["pid"] > 0


def test_configure_worker_logging_needs_no_parent_handler():
    handler = configure_worker_logging("WARNING")

    assert isinstance(handler, logging.StreamHandler)
    assert handler in logging.getLogger().handlers
    assert getattr(handler, "_pixetl_handler") is True
    assert logging.getLogger().level == logging.WARNING


def test_setup_logging_quiets_expected_rasterio_errors():
    setup_logging()

    assert logging.getLogger("rasterio._env").level == logging.WARNING
