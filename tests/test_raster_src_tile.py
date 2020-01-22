import os
from typing import Any, Dict

import numpy as np
import rasterio
from shapely.geometry import Point

from gfw_pixetl import layers, get_module_logger
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.tiles import RasterSrcTile


os.environ["ENV"] = "test"
LOGGER = get_module_logger(__name__)

GRID = grid_factory("1/4000")
RASTER_LAYER: Dict[str, Any] = {
    "name": "erosion_risk",
    "version": "v201911",
    "field": "level",
    "grid": GRID,
}

LAYER_TYPE = layers._get_source_type(
    RASTER_LAYER["name"], RASTER_LAYER["field"], RASTER_LAYER["grid"].name
)

LAYER = layers.layer_factory(**RASTER_LAYER)


def test_src_tile_intersects():
    if isinstance(LAYER, layers.RasterSrcLayer):
        tile = RasterSrcTile(Point(10, 10), GRID, LAYER)
        assert tile.src_tile_intersects()
    else:
        raise ValueError("Not a RasterSrcLayer")


def test_transform_final():
    if isinstance(LAYER, layers.RasterSrcLayer):
        tile = RasterSrcTile(Point(10, 10), GRID, LAYER)

        tile.transform(is_final=True)

        LOGGER.debug(tile.local_src.uri)
        with rasterio.open(tile.local_src.uri) as src:
            src_profile = src.profile

        LOGGER.debug(src_profile)

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

        os.remove(tile.local_src.uri)
    else:
        raise ValueError("Not a RasterSrcLayer")


def test_transform():
    if isinstance(LAYER, layers.RasterSrcLayer):
        tile = RasterSrcTile(Point(10, 10), GRID, LAYER)

        tile.transform(is_final=False)

        LOGGER.debug(tile.local_src.uri)
        with rasterio.open(tile.local_src.uri) as src:
            src_profile = src.profile
        assert src_profile["blockxsize"] == GRID.blockxsize
        assert src_profile["blockysize"] == GRID.blockysize
        # assert src_profile["compress"].lower() == LAYER.dst_profile["compress"].lower()
        assert src_profile["count"] == 1
        assert src_profile["crs"] == {"init": GRID.srs.srs}
        assert src_profile["driver"] == "GTiff"
        # assert src_profile["dtype"] == LAYER.dst_profile["dtype"]
        assert src_profile["height"] == GRID.cols
        assert src_profile["interleave"] == "band"
        # assert src_profile["nodata"] == LAYER.dst_profile["nodata"]
        assert src_profile["tiled"] is True
        assert src_profile["width"] == GRID.rows
        # assert src_profile["nbits"] == 8 # Not exposed :(

        assert not hasattr(src_profile, "compress")

        os.remove(tile.local_src.uri)
    else:
        raise ValueError("Not a RasterSrcLayer")


def test_compress():
    if isinstance(LAYER, layers.RasterSrcLayer):
        tile = RasterSrcTile(Point(10, 10), GRID, LAYER)

        tile.transform(is_final=True)
        tile.compress()

        LOGGER.debug(tile.local_src.uri)
        with rasterio.open(tile.local_src.uri) as src:
            src_profile = src.profile
        assert src_profile["blockxsize"] == GRID.blockxsize
        assert src_profile["blockysize"] == GRID.blockysize
        assert src_profile["compress"].lower() == LAYER.dst_profile["compress"].lower()
        assert src_profile["count"] == 1
        assert src_profile["crs"] == {"init": GRID.srs.srs}
        assert src_profile["driver"] == "GTiff"
        assert src_profile["dtype"] == LAYER.dst_profile["dtype"]
        assert src_profile["height"] == GRID.cols
        assert src_profile["interleave"] == "band"
        assert src_profile["nodata"] == LAYER.dst_profile["nodata"]
        assert src_profile["tiled"] is True
        assert src_profile["width"] == GRID.rows
        # assert src_profile["nbits"] == LAYER.dst_profile["nbits"] # Not exposed :(

        os.remove(tile.local_src.uri)
    else:
        raise ValueError("Not a RasterSrcLayer")


def test_update_values():
    pass


def test__is_final_cmd():
    pass


def test__apply_calc():
    if isinstance(LAYER, layers.RasterSrcLayer):
        tile = RasterSrcTile(Point(10, 10), GRID, LAYER)

        tile.layer.calc = "A+1"
        data = np.zeros((1, 3))
        result = tile._apply_calc(data)
        assert result.sum() == 3

        tile.layer.calc = "A+1*5"
        data = np.zeros((1, 3))
        result = tile._apply_calc(data)
        assert result.sum() == 15

        tile.layer.calc = "A*5+1"
        data = np.zeros((1, 3))
        result = tile._apply_calc(data)
        assert result.sum() == 3

    else:
        raise ValueError("Not a RasterSrcLayer")


def test__set_no_data_calc():
    data = np.random.randint(4, size=(10, 10))
    masked_data = np.ma.masked_values(data, 0)
    count = masked_data.mask.sum()
    if isinstance(LAYER, layers.RasterSrcLayer):
        tile = RasterSrcTile(Point(10, 10), GRID, LAYER)
        tile.dst.profile["nodata"] = 5
        result = tile._set_no_data_calc(masked_data)
        masked_result = np.ma.masked_values(result, 5)
        assert count == masked_result.mask.sum()

    else:
        raise ValueError("Not a RasterSrcLayer")
