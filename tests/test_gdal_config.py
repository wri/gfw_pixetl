import numpy as np

from gfw_pixetl.tiles.raster_src_tile import _gdal_cache_size


def test_gdal_cache_size_is_plain_int():
    cache_size = _gdal_cache_size(np.int64(7372800), 100)

    assert cache_size == 737280000
    assert type(cache_size) is int
