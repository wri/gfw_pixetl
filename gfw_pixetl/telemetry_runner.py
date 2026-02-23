import multiprocessing as mp
from typing import Optional

from gfw_pixetl.telemetry import ReporterConfig, reporter_process_main


class ReporterManager:
    """Starts/stops the reporter as a spawned process."""

    def __init__(self, cfg: ReporterConfig):
        self.cfg = cfg
        self.proc: Optional[mp.Process] = None

    def start(self) -> None:
        if self.proc and self.proc.is_alive():
            return
        ctx = mp.get_context("spawn")
        self.proc = ctx.Process(
            target=reporter_process_main, args=(self.cfg,), name="pixetl-telemetry"
        )
        # Daemon is okay—one-way telemetry. If you prefer a strict join, set False.
        self.proc.daemon = True
        self.proc.start()

    def stop(self, timeout: float = 5.0) -> None:
        if not self.proc:
            return
        # Ask politely first
        # Process handles SIGTERM to exit cleanly
        if self.proc.is_alive():
            try:
                self.proc.terminate()
            except Exception:
                pass
            self.proc.join(timeout=timeout)
        if self.proc.is_alive():
            # Last resort
            try:
                self.proc.kill()
            except Exception:
                pass
            self.proc.join(timeout=1.0)
        self.proc = None
