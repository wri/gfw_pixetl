import os

import numpy as np
import rasterio

from gfw_pixetl.data_type import data_type_factory
from gfw_pixetl.grid import grid_factory, Grid
from gfw_pixetl.layer import CalcRasterLayer
from gfw_pixetl.tile import RasterSrcTile


minx: int = 10
maxy: int = 10
subset: str = "10N_010E"

grid: Grid = grid_factory("10x10")

src_uri: str = "gfw2-data/forest_change/hansen_2018/10N_010E.tif"

datatype = data_type_factory("uint", 5)

uri: str = "gfw-test-data/gfw_pixetl/reproject/{tile_id}.tif"

calc_layer: CalcRasterLayer = CalcRasterLayer(
    "test_layer",
    "test",
    "test",
    grid,
    datatype,
    src_uri,
    calc="",
    single_tile=True,
    subset=[subset],
)

tile = RasterSrcTile(minx, maxy, calc_layer.grid, calc_layer.src, calc_layer.uri)
tile.src_tile_exists()


def test_calc():

    tile.calc_uri = tile.src_uri
    calc_layer.calc = "A+1"

    calc_layer._calc(tile)

    with rasterio.open(tile.uri) as trg:
        trg_profile = trg.profile

    assert trg_profile["blockxsize"] == grid.blockxsize
    assert trg_profile["blockysize"] == grid.blockysize
    assert trg_profile["compress"].lower() == datatype.compression.lower()
    assert trg_profile["count"] == 1
    assert trg_profile["crs"] == {"init": grid.srs.srs}
    assert trg_profile["driver"] == "GTiff"
    assert trg_profile["dtype"] == "uint8"
    assert trg_profile["height"] == grid.cols
    assert trg_profile["interleave"] == "band"
    assert trg_profile["nodata"] == datatype.no_data
    assert trg_profile["tiled"] is True
    # assert trg_profile['transform']: Affine(30.0, 0.0, 381885.0, 0.0, -30.0, 2512815.0),
    assert trg_profile["width"] == grid.rows

    os.remove(tile.uri)


def test_apply_calc():
    array = np.ma.array(
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], mask=[0, 0, 0, 1, 1, 1, 0, 0, 0, 1]
    )

    calc_layer.calc = "A+1"

    new_array = calc_layer._apply_calc(array)

    np.testing.assert_array_equal(
        new_array.data, np.array([2, 3, 4, 4, 5, 6, 8, 9, 10, 10])
    )

    calc_layer.calc = "1 * (A > 3) + 1* (A>6) + 1 * (A >9)"

    new_array = calc_layer._apply_calc(array)

    np.testing.assert_array_equal(new_array, np.array([0, 0, 0, 4, 5, 6, 2, 2, 2, 10]))


def test_set_no_data_calc():
    array = np.ma.array(
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], mask=[0, 0, 0, 1, 1, 1, 0, 0, 0, 1]
    )
    calc_layer.data_type.no_data = 0
    new_array = calc_layer._set_no_data_calc(array)

    np.testing.assert_array_equal(new_array, np.array([1, 2, 3, 0, 0, 0, 7, 8, 9, 0]))

    calc_layer.data_type.no_data = 9
    new_array = calc_layer._set_no_data_calc(array)

    np.testing.assert_array_equal(new_array, np.array([1, 2, 3, 9, 9, 9, 7, 8, 9, 9]))
