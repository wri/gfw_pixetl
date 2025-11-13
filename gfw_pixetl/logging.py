import atexit
import json
import logging
import logging.handlers
import os
import queue
import sys
from typing import Optional


# ---- JSON formatter that is CloudWatch-friendly but human-readable locally
class JsonOrTextFormatter(logging.Formatter):
    def __init__(self):
        super().__init__("%(asctime)s %(levelname)s %(name)s: %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        if os.getenv("PIXETL_LOG_JSON", "1") == "1":
            payload = {
                "time": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
                "level": record.levelname,
                "logger": record.name,
                "msg": record.getMessage(),
            }
            if record.exc_info:
                payload["exc_info"] = self.formatException(record.exc_info)
            # Include process info so child logs are traceable
            payload["process"] = {
                "pid": record.process,
                "name": record.processName,
            }
            return json.dumps(payload, ensure_ascii=False)
        # fallback pretty text for local runs
        return super().format(record)


_listener: Optional[logging.handlers.QueueListener] = None


def setup_logging(level: str = "INFO") -> logging.handlers.QueueHandler:
    """Configure root logging ONCE for the main process:

    - One QueueListener reading from a multiprocessing-safe Queue
    - The listener writes to a single StreamHandler (stdout) with JSON/text format
    - Returns a QueueHandler you can attach to any logger (incl. in child processes)
    """
    global _listener

    # Prevent duplicate config in case something called basicConfig earlier
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    # Create a shared queue and listener
    log_q: "queue.Queue[logging.LogRecord]" = queue.Queue(-1)
    stream = logging.StreamHandler(stream=sys.stdout)
    stream.setFormatter(JsonOrTextFormatter())
    root.setLevel(level)
    _listener = logging.handlers.QueueListener(
        log_q, stream, respect_handler_level=False
    )
    _listener.start()
    atexit.register(_listener.stop)

    # Attach QueueHandler to the root so normal getLogger() works everywhere
    qh = logging.handlers.QueueHandler(log_q)
    root.addHandler(qh)

    # Make third-party noisy libs less chatty if you like
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Ensure unbuffered stdout for containers (important for CloudWatch)
    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    return qh


def configure_worker_logging(
    queue_handler: logging.Handler, level: str = "INFO"
) -> None:
    """Call this at the top of any child process entrypoint to forward its logs
    into the parent's QueueListener instead of going nowhere."""
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(queue_handler)
    root.setLevel(level)
