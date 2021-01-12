import os
from copy import deepcopy

from gfw_pixetl.settings.globals import Globals

#
# def test_global_workers():
#     config = Globals(cores=1, workers=2)
#     assert config.cores == 1
#     assert config.workers == 1


def test_global_workers():
    config = Globals()
    assert config.cores == os.cpu_count()
    assert config.workers == 1

    config.workers = os.cpu_count() + 1
    assert config.cores == os.cpu_count()
    assert config.workers == os.cpu_count()

    config.cores = 1
    assert config.cores == 1
    assert config.workers == 1

    config = Globals(cores=2, workers=3)
    assert config.cores == 2
    assert config.workers == 2

    vars = deepcopy(os.environ)

    os.environ["CORES"] = "2"
    config = Globals(workers=3)
    if os.cpu_count() >= 2:
        assert config.cores == 2
        assert config.workers == 2
    else:
        assert config.cores == 1
        assert config.workers == 1

    os.environ = vars
