from rasterio.warp import Resampling
from gfw_pixetl.resampling import resampling_factory


def test_resampling_factory():

    assert resampling_factory("nearest") == Resampling.nearest

    try:
        resampling_factory("test")
    except Exception as e:
        assert isinstance(e, ValueError)
