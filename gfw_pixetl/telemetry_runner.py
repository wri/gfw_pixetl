import multiprocessing as mp
import os
from logging import Logger
from typing import Optional

from gfw_pixetl import get_module_logger
from gfw_pixetl.telemetry import ReporterConfig, telemetry_worker


class ReporterManager:
    """Own the lifecycle of the dedicated telemetry subprocess.

    The reporter is deliberately spawned instead of forked so it starts
    with a clean interpreter state. Shutdown is cooperative first
    (shared Event), then escalates to SIGTERM and finally SIGKILL if the
    telemetry process is stuck.
    """

    def __init__(self, cfg: ReporterConfig, logger: Optional[Logger] = None):
        self.cfg = cfg
        self.log = logger or get_module_logger(__name__)
        self._ctx = mp.get_context("spawn")
        self._stop_evt = self._ctx.Event()
        self.proc: Optional[mp.Process] = None

    def start(self, parent_pid: Optional[int] = None) -> None:
        """Start telemetry for *parent_pid*; repeated starts are idempotent."""
        if self.proc is not None and self.proc.is_alive():
            return

        # A Manager may be restarted after a clean stop.
        self._stop_evt.clear()
        monitored_pid = parent_pid if parent_pid is not None else os.getpid()
        proc = self._ctx.Process(
            target=telemetry_worker,
            args=(self._stop_evt, monitored_pid, self.cfg.__dict__),
            name="pixetl-telemetry",
            daemon=False,
        )

        try:
            proc.start()
        except Exception:
            self.proc = None
            self.log.exception("Failed to start telemetry process")
            raise

        self.proc = proc
        self.log.info("Started telemetry process with PID %s", proc.pid)

    def stop(
        self,
        timeout: float = 10.0,
        terminate_timeout: float = 5.0,
        kill_timeout: float = 1.0,
    ) -> None:
        """Stop telemetry, escalating only when cooperative shutdown fails."""
        proc = self.proc
        if proc is None:
            return

        self._stop_evt.set()

        # A Process object can exist even if start() failed. In that case there
        # is nothing to join or signal.
        if proc.pid is None:
            self.proc = None
            return

        proc.join(timeout=timeout)
        if proc.is_alive():
            self.log.warning(
                "Telemetry process did not stop within %.1fs; sending SIGTERM",
                timeout,
            )
            proc.terminate()
            proc.join(timeout=terminate_timeout)

        if proc.is_alive():
            self.log.warning(
                "Telemetry process did not respond to SIGTERM within %.1fs; "
                "sending SIGKILL",
                terminate_timeout,
            )
            proc.kill()
            proc.join(timeout=kill_timeout)

        if proc.is_alive():
            self.log.error(
                "Telemetry process is still alive after SIGKILL and %.1fs join",
                kill_timeout,
            )
            # Keep the reference so callers can observe/retry cleanup rather
            # than losing track of a process that is unexpectedly still live.
            return

        self.proc = None
