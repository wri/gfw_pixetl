import os
from typing import Any, Dict

import rasterio
from shapely.geometry import Point

from gfw_pixetl import layers, utils
from gfw_pixetl.errors import GDALError, GDALNoneTypeError
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.tiles import RasterSrcTile


os.environ["ENV"] = "test"

GRID = grid_factory("10/40000")
RASTER_LAYER: Dict[str, Any] = {
    "name": "aboveground_biomass_stock_2000",
    "version": "v201911",
    "field": "Mg_ha-1",
    "grid": GRID,
}

LAYER_TYPE = layers._get_source_type(
    RASTER_LAYER["name"], RASTER_LAYER["field"], RASTER_LAYER["grid"].name
)

LAYER = layers.layer_factory(**RASTER_LAYER)

if isinstance(LAYER, layers.RasterSrcLayer):
    TILE = RasterSrcTile(Point(10, 10), GRID, LAYER)


def test_src_tile_intersects():
    assert TILE.src_tile_intersects()


def test_translate_src_tile():

    TILE.transform(is_final=True)

    with rasterio.open(TILE.local_src) as src:
        src_profile = src.profile

    assert src_profile["blockxsize"] == GRID.blockxsize
    assert src_profile["blockysize"] == GRID.blockysize
    assert src_profile["compress"].lower() == LAYER.dst_profile["compression"].lower()
    assert src_profile["count"] == 1
    assert src_profile["crs"] == {"init": GRID.srs.srs}
    assert src_profile["driver"] == "GTiff"
    assert src_profile["dtype"] == LAYER.dst_profile["dtype"]
    assert src_profile["height"] == GRID.cols
    assert src_profile["interleave"] == "band"
    assert src_profile["nodata"] == LAYER.dst_profile["nodata"]
    assert src_profile["tiled"] is True
    # assert src_profile['transform']: Affine(30.0, 0.0, 381885.0, 0.0, -30.0, 2512815.0),
    assert src_profile["width"] == GRID.rows

    os.remove(TILE.local_src)
