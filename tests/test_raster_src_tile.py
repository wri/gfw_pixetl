import os
from copy import deepcopy
from math import isclose

import numpy as np
import rasterio
from rasterio.windows import Window

from gfw_pixetl import get_module_logger, layers
from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.settings.gdal import GDAL_ENV
from gfw_pixetl.tiles import RasterSrcTile
from tests.conftest import BUCKET, GEOJSON_2_NAME, LAYER_DICT

LOGGER = get_module_logger(__name__)


def test_src_tile_intersects(LAYER):
    assert isinstance(LAYER, layers.RasterSrcLayer)

    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)
    assert tile.within()


def test_src_tile_intersects_wm(LAYER_WM):
    assert isinstance(LAYER_WM, layers.RasterSrcLayer)

    tile = RasterSrcTile("030R_034C", LAYER_WM.grid, LAYER_WM)
    assert tile.within()

    tile = RasterSrcTile("010R_014C", LAYER_WM.grid, LAYER_WM)
    assert not tile.within()


def test_transform_final(LAYER):
    assert isinstance(LAYER, layers.RasterSrcLayer)
    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)
    assert tile.dst[tile.default_format].crs.is_valid

    with rasterio.Env(**GDAL_ENV), rasterio.open(tile.src.uri) as tile_src:
        window = rasterio.windows.from_bounds(
            10, 9, 11, 10, transform=tile_src.transform
        )
        input = tile_src.read(1, window=window)

    tile.transform()

    LOGGER.debug(tile.local_dst[tile.default_format].uri)
    with rasterio.Env(**GDAL_ENV), rasterio.open(
        tile.local_dst[tile.default_format].uri
    ) as src:
        src_profile = src.profile
        output = src.read(1)

    LOGGER.debug(src_profile)

    assert input.shape == output.shape
    np.testing.assert_array_equal(input, output)

    assert src_profile["blockxsize"] == LAYER.grid.blockxsize
    assert src_profile["blockysize"] == LAYER.grid.blockysize
    assert src_profile["compress"].lower() == LAYER.dst_profile["compress"].lower()
    assert src_profile["count"] == 1
    assert src_profile["crs"] == {"init": LAYER.grid.crs.srs}
    assert src_profile["crs"].is_valid
    assert src_profile["driver"] == "GTiff"
    assert src_profile["dtype"] == LAYER.dst_profile["dtype"]
    assert src_profile["height"] == LAYER.grid.cols
    assert src_profile["interleave"] == "band"
    assert src_profile["nodata"] == LAYER.dst_profile["nodata"]
    assert src_profile["tiled"] is True
    assert src_profile["width"] == LAYER.grid.rows
    # assert src_profile["nbits"] == nbits # Not exposed in rasterio API

    assert not hasattr(src_profile, "compress")

    os.remove(tile.local_dst[tile.default_format].uri)


def test_transform_final_wm():
    layer_dict_wm = deepcopy(LAYER_DICT)
    layer_dict_wm["grid"] = "zoom_0"
    layer_dict_wm["source_uri"] = [f"s3://{BUCKET}/{GEOJSON_2_NAME}"]

    layer_wm = layers.layer_factory(LayerModel(**layer_dict_wm))

    assert isinstance(layer_wm, layers.RasterSrcLayer)
    tile = RasterSrcTile("000R_000C", layer_wm.grid, layer_wm)

    assert tile.dst[tile.default_format].crs.is_valid
    tile.transform()

    LOGGER.debug(tile.local_dst[tile.default_format].uri)
    with rasterio.Env(**GDAL_ENV), rasterio.open(
        tile.local_dst[tile.default_format].uri
    ) as src:
        src_profile = src.profile
        output = src.read(1)

    LOGGER.debug(src_profile)

    assert output.shape == (256, 256)

    assert src_profile["blockxsize"] == layer_wm.grid.blockxsize
    assert src_profile["blockysize"] == layer_wm.grid.blockysize
    assert src_profile["compress"].lower() == layer_wm.dst_profile["compress"].lower()
    assert src_profile["count"] == 1
    assert src_profile["crs"] == {"init": layer_wm.grid.crs.srs}
    assert src_profile["crs"].is_valid
    assert src_profile["driver"] == "GTiff"
    assert src_profile["dtype"] == layer_wm.dst_profile["dtype"]
    assert src_profile["height"] == layer_wm.grid.cols
    assert src_profile["interleave"] == "band"
    assert src_profile["nodata"] == layer_wm.dst_profile["nodata"]
    assert src_profile["tiled"] is True
    assert src_profile["width"] == layer_wm.grid.rows
    # assert src_profile["nbits"] == nbits # Not exposed in rasterio API

    assert not hasattr(src_profile, "compress")
    os.remove(tile.local_dst[tile.default_format].uri)


def test_transform_final_multi(LAYER_MULTI, LAYER):

    assert isinstance(LAYER_MULTI, layers.RasterSrcLayer)
    tile = RasterSrcTile("10N_010E", LAYER_MULTI.grid, LAYER_MULTI)
    assert tile.dst[tile.default_format].crs.is_valid

    with rasterio.Env(**GDAL_ENV), rasterio.open(tile.src.uri) as tile_src:
        assert tile_src.profile["count"] == 2
        window = rasterio.windows.from_bounds(
            10, 9, 11, 10, transform=tile_src.transform
        )
        input = tile_src.read(1, window=window)

    tile.transform()

    LOGGER.debug(tile.local_dst[tile.default_format].uri)
    with rasterio.Env(**GDAL_ENV), rasterio.open(
        tile.local_dst[tile.default_format].uri
    ) as src:
        src_profile = src.profile
        output = src.read(1)

    LOGGER.debug(src_profile)

    assert input.shape == output.shape
    np.testing.assert_array_equal(input + input, output)

    assert src_profile["blockxsize"] == LAYER.grid.blockxsize
    assert src_profile["blockysize"] == LAYER.grid.blockysize
    assert src_profile["compress"].lower() == LAYER.dst_profile["compress"].lower()
    assert src_profile["count"] == 1
    assert src_profile["crs"] == {"init": LAYER.grid.crs.srs}
    assert src_profile["crs"].is_valid
    assert src_profile["driver"] == "GTiff"
    assert src_profile["dtype"] == LAYER.dst_profile["dtype"]
    assert src_profile["height"] == LAYER.grid.cols
    assert src_profile["interleave"] == "band"
    assert src_profile["nodata"] == LAYER.dst_profile["nodata"]
    assert src_profile["tiled"] is True
    assert src_profile["width"] == LAYER.grid.rows
    # assert src_profile["nbits"] == nbits # Not exposed in rasterio API

    assert not hasattr(src_profile, "compress")

    os.remove(tile.local_dst[tile.default_format].uri)


def test__calc(LAYER):
    window = Window(0, 0, 1, 3)
    assert isinstance(LAYER, layers.RasterSrcLayer)
    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)

    tile.layer.calc = "A+1"
    data = np.zeros((1, 1, 3))
    result = tile._calc(data, window)
    assert result.sum() == 3

    tile.layer.calc = "A+1*5"
    data = np.zeros((1, 1, 3))
    result = tile._calc(data, window)
    assert result.sum() == 15

    tile.layer.calc = "A*5+1"
    data = np.zeros((1, 1, 3))
    result = tile._calc(data, window)
    assert result.sum() == 3

    tile.layer.calc = "A+B"
    data = np.ones((2, 1, 3))
    result = tile._calc(data, window)
    assert result.sum() == 6

    tile.layer.calc = "(A+B)*(C+2)"
    data = np.ones((3, 1, 3))
    result = tile._calc(data, window)
    assert result.sum() == 18


def test__set_dtype(LAYER):
    window = Window(0, 0, 10, 10)
    data = np.random.randint(4, size=(10, 10))
    masked_data = np.ma.masked_values(data, 0)
    count = masked_data.mask.sum()
    if isinstance(LAYER, layers.RasterSrcLayer):
        tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)
        tile.dst[tile.default_format].nodata = 5
        result = tile._set_dtype(masked_data, window)
        masked_result = np.ma.masked_values(result, 5)
        assert count == masked_result.mask.sum()

    else:
        raise ValueError("Not a RasterSrcLayer")


def test__snap_coordinates(LAYER):
    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)

    lat = 9.777
    lng = 10.111
    top, left = tile.grid.snap_coordinates(lat, lng)
    assert isclose(top, lat)
    assert isclose(left, lng)

    lat = 9.7777
    lng = 10.1117
    top, left = tile.grid.snap_coordinates(lat, lng)
    assert isclose(top, 9.77775)
    assert isclose(left, 10.1115)


def test__vrt_transform(LAYER):
    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)

    transform, width, height = tile._vrt_transform(9.1, 9.1, 9.2, 9.2)

    assert transform.almost_equals(rasterio.Affine(0.00025, 0, 9.1, 0, -0.00025, 9.2))
    assert isclose(width, 400)
    assert isclose(height, 400)


def test_download_files(LAYER):
    layer = deepcopy(LAYER)
    layer.process_locally = True
    tile = RasterSrcTile("10N_010E", layer.grid, layer)
    _ = tile.src  # trigger download

    assert os.path.isfile(
        os.path.join(tile.work_dir, "input/gfw-data-lake-test/10N_010E.tif")
    )


def test__block_byte_size(LAYER, LAYER_MULTI):
    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)
    assert tile._block_byte_size() == 1 * 2 * 400 * 400

    tile = RasterSrcTile("10N_010E", LAYER_MULTI.grid, LAYER_MULTI)
    assert tile._block_byte_size() == 2 * 2 * 400 * 400
