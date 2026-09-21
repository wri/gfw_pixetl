import threading
import time

from gfw_pixetl.memory_admission import GIB, MemoryAdmissionController


def _write(path, value):
    path.write_text(str(value))


def _controller(tmp_path, monkeypatch, *, current_gib=60, limit_gib=100):
    import gfw_pixetl.memory_admission as module

    _write(tmp_path / "memory.current", current_gib * GIB)
    _write(tmp_path / "memory.max", limit_gib * GIB)
    monkeypatch.setattr(module, "STATUS_PATH", str(tmp_path / "admission.json"))

    controller = MemoryAdmissionController()
    controller.configure(
        enabled=True,
        cgroup_root=str(tmp_path),
        reservation_bytes=8 * GIB,
        poll_seconds=0.01,
    )
    return controller


def test_transform_reservation_is_released_after_startup_commit(tmp_path, monkeypatch):
    controller = _controller(tmp_path, monkeypatch)

    controller.acquire_transform("00N_000E")
    assert controller._reserved_bytes.value == 8 * GIB

    # Reservation only protects startup; the first completed transform window
    # commits it once memory.current reflects the real working set.
    controller.commit_transform_reservation()
    assert controller._reserved_bytes.value == 0

    # release_transform remains idempotent for cleanup/error paths.
    controller.release_transform()
    assert controller._reserved_bytes.value == 0


def test_transform_slot_cleans_up_uncommitted_reservation(tmp_path, monkeypatch):
    controller = _controller(tmp_path, monkeypatch)

    with controller.transform_slot("00N_000E"):
        assert controller._reserved_bytes.value == 8 * GIB
        controller.commit_transform_reservation()
        assert controller._reserved_bytes.value == 0

    assert controller._reserved_bytes.value == 0


def test_transform_waits_for_resume_watermark(tmp_path, monkeypatch):
    controller = _controller(tmp_path, monkeypatch, current_gib=75)
    admitted = threading.Event()

    def acquire():
        controller.acquire_transform("00N_000E")
        admitted.set()

    thread = threading.Thread(target=acquire)
    thread.start()
    time.sleep(0.05)

    assert not admitted.is_set()
    assert controller._throttled.value == 1
    assert controller._waiting.value == 1

    _write(tmp_path / "memory.current", 70 * GIB)
    thread.join(timeout=1)

    assert admitted.is_set()
    assert controller._throttled.value == 0
    assert controller._waiting.value == 0
    controller.release_transform()


def test_stats_gate_uses_80_75_hysteresis(tmp_path, monkeypatch):
    controller = _controller(tmp_path, monkeypatch, current_gib=81)
    passed = threading.Event()

    thread = threading.Thread(
        target=lambda: (controller.wait_for_stats("00N_000E"), passed.set())
    )
    thread.start()
    time.sleep(0.05)
    assert not passed.is_set()

    # It must fall below the 75% resume threshold, not merely 80%.
    _write(tmp_path / "memory.current", 77 * GIB)
    time.sleep(0.05)
    assert not passed.is_set()

    _write(tmp_path / "memory.current", 74 * GIB)
    thread.join(timeout=1)
    assert passed.is_set()
