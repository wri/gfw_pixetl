import threading
import time

from gfw_pixetl.memory_admission import GIB, MemoryAdmissionController


def _write(path, value):
    tmp = path.with_name(f".{path.name}.{threading.get_ident()}.tmp")
    tmp.write_text(str(value))
    tmp.replace(path)


def _controller(tmp_path, monkeypatch, *, current_gib=60, limit_gib=100):
    # These tests exercise admission state, synchronization, and hysteresis,
    # not publication of the diagnostic status file.
    monkeypatch.setattr(
        MemoryAdmissionController,
        "_write_status_locked",
        lambda self: None,
    )

    _write(tmp_path / "memory.current", current_gib * GIB)
    _write(tmp_path / "memory.max", limit_gib * GIB)

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

    try:
        time.sleep(0.05)

        assert not admitted.is_set()
        assert controller._throttled.value == 1
        assert controller._waiting.value == 1

        # Exactly 75% remains throttled; resume requires falling below it.
        _write(tmp_path / "memory.current", 75 * GIB)
        time.sleep(0.05)
        assert not admitted.is_set()

        _write(tmp_path / "memory.current", 74 * GIB)
        thread.join(timeout=1)

        assert admitted.is_set()
        assert controller._throttled.value == 0
        assert controller._waiting.value == 0
        controller.release_transform()
    finally:
        # Never leave a polling thread alive after pytest removes tmp_path.
        _write(tmp_path / "memory.current", 0)
        thread.join(timeout=1)


def test_stats_gate_uses_80_75_hysteresis(tmp_path, monkeypatch):
    controller = _controller(tmp_path, monkeypatch, current_gib=81)
    passed = threading.Event()

    thread = threading.Thread(
        target=lambda: (controller.wait_for_stats("00N_000E"), passed.set())
    )
    thread.start()

    try:
        time.sleep(0.05)
        assert not passed.is_set()

        # It must fall below the 75% resume threshold, not merely 80%.
        _write(tmp_path / "memory.current", 77 * GIB)
        time.sleep(0.05)
        assert not passed.is_set()

        _write(tmp_path / "memory.current", 74 * GIB)
        thread.join(timeout=1)
        assert passed.is_set()
    finally:
        # Ensure wait_for_stats can escape even if an assertion above fails.
        _write(tmp_path / "memory.current", 0)
        thread.join(timeout=1)


def test_stats_slot_tracks_active_and_waiting(tmp_path, monkeypatch):
    controller = _controller(tmp_path, monkeypatch)
    controller.configure(
        enabled=True,
        cgroup_root=str(tmp_path),
        stats_workers=1,
        reservation_bytes=8 * GIB,
        poll_seconds=0.01,
    )
    first_entered = threading.Event()
    release_first = threading.Event()
    second_entered = threading.Event()

    def first():
        with controller.stats_slot("first"):
            first_entered.set()
            release_first.wait(timeout=1)

    def second():
        with controller.stats_slot("second"):
            second_entered.set()

    first_thread = threading.Thread(target=first)
    second_thread = threading.Thread(target=second)
    first_thread.start()

    try:
        assert first_entered.wait(timeout=1)
        second_thread.start()
        time.sleep(0.05)

        assert controller._stats_active.value == 1
        assert controller._stats_waiting.value == 1
        assert not second_entered.is_set()

        release_first.set()
        first_thread.join(timeout=1)
        second_thread.join(timeout=1)

        assert second_entered.is_set()
        assert controller._stats_active.value == 0
        assert controller._stats_waiting.value == 0
    finally:
        # Never strand either thread if an assertion fails.
        release_first.set()
        _write(tmp_path / "memory.current", 0)
        first_thread.join(timeout=1)
        if second_thread.ident is not None:
            second_thread.join(timeout=1)
