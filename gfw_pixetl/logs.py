import json
import logging
import os
import sys
from typing import Union


class JsonOrTextFormatter(logging.Formatter):
    """CloudWatch-friendly JSON in containers, readable text when requested."""

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)s %(name)s: %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        if os.getenv("PIXETL_LOG_JSON", "1") == "1":
            payload = {
                "time": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
                "level": record.levelname,
                "logger": record.name,
                "msg": record.getMessage(),
                "process": {
                    "pid": record.process,
                    "name": record.processName,
                },
            }
            if record.exc_info:
                payload["exc_info"] = self.formatException(record.exc_info)
            return json.dumps(payload, ensure_ascii=False)
        return super().format(record)


def setup_logging(level: Union[str, int] = "INFO") -> logging.StreamHandler:
    """Configure direct stdout logging for the current process.

    Batch/CloudWatch already aggregates stdout from the container, so pixetl does
    not need a QueueListener thread in the parent process. Spawned workers call
    ``configure_worker_logging`` to install the same direct handler in their fresh
    interpreters.

    Calling this function repeatedly is idempotent: pixetl replaces only the
    handler it owns, rather than accumulating duplicates or background threads.
    Existing handlers installed by an embedding application or test runner are
    left alone.
    """
    root = logging.getLogger()

    for handler in list(root.handlers):
        if not getattr(handler, "_pixetl_handler", False):
            continue
        root.removeHandler(handler)
        try:
            handler.flush()
        except Exception:
            pass
        handler.close()

    stream = logging.StreamHandler(stream=sys.stdout)
    stream._pixetl_handler = True  # type: ignore[attr-defined]
    stream.setFormatter(JsonOrTextFormatter())
    root.addHandler(stream)
    root.setLevel(level)

    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return stream


def configure_worker_logging(level: Union[str, int] = "INFO") -> logging.StreamHandler:
    """Configure stdout logging in a child process that uses a fresh
    interpreter.

    Spawned pixetl workers start with a fresh interpreter and do not
    inherit the parent's logging handlers. This helper installs the
    standard pixetl stdout handler in those child entrypoints.
    """
    return setup_logging(level)
