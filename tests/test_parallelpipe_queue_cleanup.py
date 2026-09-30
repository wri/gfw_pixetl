import threading

from gfw_pixetl.parallelpipe import stage


@stage()
def _producer():
    yield 1


def test_pipeline_reaps_queue_feeder_threads():
    assert list(_producer.results()) == [1]
    assert not any(t.name == "QueueFeederThread" for t in threading.enumerate())
