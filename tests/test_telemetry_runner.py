from unittest import mock

import pytest

from gfw_pixetl.telemetry import ReporterConfig
from gfw_pixetl.telemetry_runner import ReporterManager


class FakeEvent:
    def __init__(self):
        self.set_calls = 0
        self.clear_calls = 0

    def set(self):
        self.set_calls += 1

    def clear(self):
        self.clear_calls += 1


class FakeProcess:
    def __init__(self, alive_states=None, start_error=None):
        self.pid = None
        self._alive_states = list(alive_states or [False])
        self._last_alive = self._alive_states[-1]
        self.start_error = start_error
        self.join_calls = []
        self.terminate_calls = 0
        self.kill_calls = 0

    def start(self):
        if self.start_error is not None:
            raise self.start_error
        self.pid = 4321

    def is_alive(self):
        if self._alive_states:
            self._last_alive = self._alive_states.pop(0)
        return self._last_alive

    def join(self, timeout=None):
        self.join_calls.append(timeout)

    def terminate(self):
        self.terminate_calls += 1

    def kill(self):
        self.kill_calls += 1


class FakeContext:
    def __init__(self, processes):
        self.event = FakeEvent()
        self.processes = list(processes)
        self.process_calls = []

    def Event(self):
        return self.event

    def Process(self, **kwargs):
        self.process_calls.append(kwargs)
        return self.processes.pop(0)


def make_manager(monkeypatch, processes):
    ctx = FakeContext(processes)
    monkeypatch.setattr(
        "gfw_pixetl.telemetry_runner.mp.get_context", lambda method: ctx
    )
    logger = mock.Mock()
    manager = ReporterManager(ReporterConfig(emit_emf=False), logger=logger)
    return manager, ctx, logger


def test_start_uses_spawn_non_daemon_and_parent_pid(monkeypatch):
    proc = FakeProcess()
    manager, ctx, _ = make_manager(monkeypatch, [proc])

    manager.start(parent_pid=1234)

    assert manager.proc is proc
    assert ctx.event.clear_calls == 1
    call = ctx.process_calls[0]
    assert call["args"][1] == 1234
    assert call["daemon"] is False
    assert call["name"] == "pixetl-telemetry"


def test_repeated_start_is_idempotent_while_alive(monkeypatch):
    proc = FakeProcess(alive_states=[True])
    manager, ctx, _ = make_manager(monkeypatch, [proc])

    manager.start(parent_pid=1234)
    manager.start(parent_pid=5678)

    assert len(ctx.process_calls) == 1


def test_stop_is_cooperative_when_process_exits(monkeypatch):
    proc = FakeProcess(alive_states=[False])
    manager, ctx, _ = make_manager(monkeypatch, [proc])
    manager.start(parent_pid=1234)

    manager.stop(timeout=7.0)

    assert ctx.event.set_calls == 1
    assert proc.join_calls == [7.0]
    assert proc.terminate_calls == 0
    assert proc.kill_calls == 0
    assert manager.proc is None


def test_stop_escalates_to_sigterm(monkeypatch):
    proc = FakeProcess(alive_states=[True, False])
    manager, _, _ = make_manager(monkeypatch, [proc])
    manager.start(parent_pid=1234)

    manager.stop(timeout=7.0, terminate_timeout=2.0)

    assert proc.join_calls == [7.0, 2.0]
    assert proc.terminate_calls == 1
    assert proc.kill_calls == 0


def test_stop_escalates_to_sigkill(monkeypatch):
    proc = FakeProcess(alive_states=[True, True, False])
    manager, _, _ = make_manager(monkeypatch, [proc])
    manager.start(parent_pid=1234)

    manager.stop(timeout=7.0, terminate_timeout=2.0, kill_timeout=0.5)

    assert proc.join_calls == [7.0, 2.0, 0.5]
    assert proc.terminate_calls == 1
    assert proc.kill_calls == 1


def test_manager_can_restart_after_stop(monkeypatch):
    first = FakeProcess(alive_states=[False])
    second = FakeProcess(alive_states=[False])
    manager, ctx, _ = make_manager(monkeypatch, [first, second])

    manager.start(parent_pid=1234)
    manager.stop()
    manager.start(parent_pid=1234)

    assert manager.proc is second
    assert len(ctx.process_calls) == 2
    assert ctx.event.clear_calls == 2


def test_start_failure_does_not_leave_process_registered(monkeypatch):
    proc = FakeProcess(start_error=RuntimeError("spawn failed"))
    manager, _, logger = make_manager(monkeypatch, [proc])

    with pytest.raises(RuntimeError, match="spawn failed"):
        manager.start(parent_pid=1234)

    assert manager.proc is None
    logger.exception.assert_called_once()


def test_stop_retains_reference_if_process_survives_sigkill(monkeypatch):
    proc = FakeProcess(alive_states=[True, True, True])
    manager, _, logger = make_manager(monkeypatch, [proc])
    manager.start(parent_pid=1234)

    manager.stop(timeout=1.0, terminate_timeout=1.0, kill_timeout=1.0)

    assert manager.proc is proc
    logger.error.assert_called_once()
