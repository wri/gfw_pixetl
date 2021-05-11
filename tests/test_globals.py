import os
from copy import deepcopy

from gfw_pixetl.settings.globals import Globals


def test_global_workers():
    config = Globals()
    assert config.num_processes == os.cpu_count()
    assert config.workers == os.cpu_count()

    config.workers = os.cpu_count() + 1
    assert config.num_processes == os.cpu_count()
    assert config.workers == os.cpu_count()

    config.num_processes = 1
    assert config.num_processes == 1
    assert config.workers == 1

    config = Globals(num_processes=2, workers=3)
    assert config.num_processes == 2
    assert config.workers == 2

    config = Globals(cores=4, num_processes=3, workers=2)
    assert config.num_processes == 3
    assert config.workers == 2

    vars = deepcopy(os.environ)

    os.environ["NUM_PROCESSES"] = "2"
    config = Globals(workers=3)
    if os.cpu_count() >= 2:
        assert config.num_processes == 2
        assert config.workers == 2
    else:
        assert config.num_processes == 1
        assert config.workers == 1

    os.environ = vars
