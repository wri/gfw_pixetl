import os
from copy import deepcopy
from math import isclose

import numpy as np
import rasterio
from rasterio.enums import ColorInterp

from gfw_pixetl import get_module_logger, layers
from gfw_pixetl.models.enums import PhotometricType
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


def test_transform_final_multi_in(LAYER_MULTI, LAYER):
    assert isinstance(LAYER_MULTI, layers.RasterSrcLayer)

    tile = RasterSrcTile("10N_010E", LAYER_MULTI.grid, LAYER_MULTI)
    assert tile.dst[tile.default_format].crs.is_valid

    with rasterio.Env(**GDAL_ENV), rasterio.open(tile.src.uri) as tile_src:
        assert tile_src.profile["count"] == 2
        window = rasterio.windows.from_bounds(
            10, 9, 11, 10, transform=tile_src.transform
        )
        input = tile_src.read(window=window)

    assert input.shape == (2, 4000, 4000)

    tile.transform()

    LOGGER.debug(tile.local_dst[tile.default_format].uri)

    with rasterio.Env(**GDAL_ENV), rasterio.open(
        tile.local_dst[tile.default_format].uri
    ) as src:
        src_profile = src.profile
        output = src.read()

    LOGGER.debug(src_profile)

    assert output.shape == (1, 4000, 4000)

    np.testing.assert_array_equal(input[0] + input[1], output[0])

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


def test_transform_final_multi_out(LAYER_MULTI, LAYER):
    assert isinstance(LAYER_MULTI, layers.RasterSrcLayer)
    LAYER_MULTI.calc = "np.ma.array([A, B, A+B])"
    LAYER_MULTI.band_count = 3
    LAYER_MULTI.photometric = PhotometricType.rgb

    tile = RasterSrcTile("10N_010E", LAYER_MULTI.grid, LAYER_MULTI)
    assert tile.dst[tile.default_format].crs.is_valid

    with rasterio.Env(**GDAL_ENV), rasterio.open(tile.src.uri) as tile_src:
        assert tile_src.profile["count"] == 2
        window = rasterio.windows.from_bounds(
            10, 9, 11, 10, transform=tile_src.transform
        )
        input = tile_src.read(window=window)
    assert input.shape == (2, 4000, 4000)

    tile.transform()

    LOGGER.debug(tile.local_dst[tile.default_format].uri)
    with rasterio.Env(**GDAL_ENV), rasterio.open(
        tile.local_dst[tile.default_format].uri
    ) as src:
        src_profile = src.profile
        output = src.read()
        colorinterp = src.colorinterp

    LOGGER.debug(src_profile)

    assert output.shape == (3, 4000, 4000)
    np.testing.assert_array_equal(input[0], output[0])
    np.testing.assert_array_equal(input[1], output[1])
    np.testing.assert_array_equal(input[0] + input[1], output[2])

    assert src_profile["blockxsize"] == LAYER.grid.blockxsize
    assert src_profile["blockysize"] == LAYER.grid.blockysize
    assert src_profile["compress"].lower() == LAYER.dst_profile["compress"].lower()
    assert src_profile["count"] == 3
    assert src_profile["crs"] == {"init": LAYER.grid.crs.srs}
    assert src_profile["crs"].is_valid
    assert src_profile["driver"] == "GTiff"
    assert src_profile["dtype"] == LAYER.dst_profile["dtype"]
    assert src_profile["height"] == LAYER.grid.cols
    assert src_profile["interleave"] == "pixel"
    assert src_profile["nodata"] == LAYER.dst_profile["nodata"]
    assert src_profile["tiled"] is True
    assert src_profile["width"] == LAYER.grid.rows
    assert colorinterp == (ColorInterp.red, ColorInterp.green, ColorInterp.blue)
    # assert src_profile["nbits"] == nbits # Not exposed in rasterio API

    assert not hasattr(src_profile, "compress")

    os.remove(tile.local_dst[tile.default_format].uri)


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
    _ = RasterSrcTile("10N_010E", layer.grid, layer)

    assert os.path.isfile("/tmp/input/source0/gfw-data-lake-test/10N_010E.tif")


def test__block_byte_size_single(LAYER):
    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)
    assert tile._block_byte_size() == 1 * 2 * 400 * 400


def test__block_byte_size_multi(LAYER_MULTI):
    tile = RasterSrcTile("10N_010E", LAYER_MULTI.grid, LAYER_MULTI)
    assert tile._block_byte_size() == 2 * 2 * 400 * 400
