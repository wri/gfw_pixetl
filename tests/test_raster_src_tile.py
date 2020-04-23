import os
from math import isclose
from typing import Any, Dict

import numpy as np
import rasterio
from rasterio.windows import Window
from shapely.geometry import Point

from gfw_pixetl import layers, get_module_logger
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.tiles import RasterSrcTile

os.environ["ENV"] = "test"
LOGGER = get_module_logger(__name__)

GRID = grid_factory("1/4000")
RASTER_LAYER: Dict[str, Any] = {
    "name": "umd_tree_cover_density_2000",
    "version": "v1.6",
    "field": "percent",
    "grid": GRID,
}

LAYER_TYPE = layers._get_source_type(
    RASTER_LAYER["name"], RASTER_LAYER["field"], RASTER_LAYER["grid"].name
)

LAYER = layers.layer_factory(**RASTER_LAYER)


def test_src_tile_intersects():
    if isinstance(LAYER, layers.RasterSrcLayer):
        tile = RasterSrcTile(Point(10, 10), GRID, LAYER)
        assert tile.within()
    else:
        raise ValueError("Not a RasterSrcLayer")


def test_transform_final():
    if isinstance(LAYER, layers.RasterSrcLayer):
        tile = RasterSrcTile(Point(10, 10), GRID, LAYER)

        with rasterio.open(tile.src.uri) as tile_src:
            window = rasterio.windows.from_bounds(
                10, 9, 11, 10, transform=tile_src.transform
            )
            input = tile_src.read(1, window=window)

        tile.transform()

        LOGGER.debug(tile.local_dst[tile.default_format].uri)
        with rasterio.open(tile.local_dst[tile.default_format].uri) as src:
            src_profile = src.profile
            output = src.read(1)

        LOGGER.debug(src_profile)

        assert input.shape == output.shape
        np.testing.assert_array_equal(input, output)

        assert src_profile["blockxsize"] == GRID.blockxsize
        assert src_profile["blockysize"] == GRID.blockysize
        # assert src_profile["compress"].lower() == LAYER.dst_profile["compress"].lower()
        assert src_profile["count"] == 1
        assert src_profile["crs"] == {"init": GRID.srs.srs}
        assert src_profile["driver"] == "GTiff"
        assert src_profile["dtype"] == LAYER.dst_profile["dtype"]
        assert src_profile["height"] == GRID.cols
        assert src_profile["interleave"] == "band"
        assert src_profile["nodata"] == LAYER.dst_profile["nodata"]
        assert src_profile["tiled"] is True
        assert src_profile["width"] == GRID.rows
        # assert src_profile["nbits"] == nbits # Not exposed in rasterio API

        assert not hasattr(src_profile, "compress")

        os.remove(tile.local_dst[tile.default_format].uri)
    else:
        raise ValueError("Not a RasterSrcLayer")


def test__calc():
    window = Window(0, 0, 1, 3)
    if isinstance(LAYER, layers.RasterSrcLayer):
        tile = RasterSrcTile(Point(10, 10), GRID, LAYER)

        tile.layer.calc = "A+1"
        data = np.zeros((1, 3))
        result = tile._calc(data, window)
        assert result.sum() == 3

        tile.layer.calc = "A+1*5"
        data = np.zeros((1, 3))
        result = tile._calc(data, window)
        assert result.sum() == 15

        tile.layer.calc = "A*5+1"
        data = np.zeros((1, 3))
        result = tile._calc(data, window)
        assert result.sum() == 3

    else:
        raise ValueError("Not a RasterSrcLayer")


def test__set_dtype():
    window = Window(0, 0, 10, 10)
    data = np.random.randint(4, size=(10, 10))
    masked_data = np.ma.masked_values(data, 0)
    count = masked_data.mask.sum()
    if isinstance(LAYER, layers.RasterSrcLayer):
        tile = RasterSrcTile(Point(10, 10), GRID, LAYER)
        tile.dst[tile.default_format].nodata = 5
        result = tile._set_dtype(masked_data, window)
        masked_result = np.ma.masked_values(result, 5)
        assert count == masked_result.mask.sum()

    else:
        raise ValueError("Not a RasterSrcLayer")


def test__snap_coordinates():
    tile = RasterSrcTile(Point(10, 10), GRID, LAYER)

    lat = 9.777
    lng = 10.111
    top, left = tile._snap_coordinates(lat, lng)
    assert isclose(top, lat)
    assert isclose(left, lng)

    lat = 9.7777
    lng = 10.1117
    top, left = tile._snap_coordinates(lat, lng)
    assert isclose(top, 9.77775)
    assert isclose(left, 10.1115)


def test__vrt_transform():
    tile = RasterSrcTile(Point(10, 10), GRID, LAYER)

    transform, width, height = tile._vrt_transform(9.1, 9.1, 9.2, 9.2)

    assert transform.almost_equals(rasterio.Affine(0.00025, 0, 9.1, 0, -0.00025, 9.2))
    assert isclose(width, 400)
    assert isclose(height, 400)
