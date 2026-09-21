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


def test_transform_reservation_is_held_until_transform_finishes(tmp_path, monkeypatch):
    controller = _controller(tmp_path, monkeypatch)

    controller.acquire_transform("00N_000E")
    assert controller._reserved_bytes.value == 8 * GIB

    # V2 keeps the reservation for the entire tile transform. It is released
    # only when the transform slot exits, after postprocessing has completed.
    controller.release_transform()
    assert controller._reserved_bytes.value == 0

    # release_transform remains idempotent for cleanup/error paths.
    controller.release_transform()
    assert controller._reserved_bytes.value == 0


def test_transform_slot_holds_reservation_for_entire_context(tmp_path, monkeypatch):
    controller = _controller(tmp_path, monkeypatch)

    with controller.transform_slot("00N_000E"):
        assert controller._reserved_bytes.value == 8 * GIB

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


def test_postprocessing_uses_critical_hysteresis(tmp_path, monkeypatch):
    controller = _controller(tmp_path, monkeypatch, current_gib=91)
    passed = threading.Event()

    thread = threading.Thread(
        target=lambda: (controller.wait_for_postprocessing("00N_000E"), passed.set())
    )
    thread.start()
    time.sleep(0.05)
    assert not passed.is_set()

    # It must fall below the 85% critical resume threshold, not merely 90%.
    _write(tmp_path / "memory.current", 87 * GIB)
    time.sleep(0.05)
    assert not passed.is_set()

    _write(tmp_path / "memory.current", 84 * GIB)
    thread.join(timeout=1)
    assert passed.is_set()
