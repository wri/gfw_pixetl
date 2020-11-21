import os
import shutil
from copy import deepcopy
from time import sleep
from typing import Any, Dict, Optional
from unittest import mock
from unittest.mock import patch

import numpy as np
import pytest
import rasterio
from rasterio import Affine
from rasterio.crs import CRS
from rasterio.enums import ColorInterp
from rasterio.windows import Window
from shapely.geometry import box

from gfw_pixetl import get_module_logger, layers
from gfw_pixetl.models import LayerModel
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.aws import get_s3_client
from tests import minimal_layer_dict
from tests.conftest import BUCKET, TILE_1_PATH, TILE_4_PATH

os.environ["ENV"] = "test"

LAYER_DICT = {
    **minimal_layer_dict,
    "dataset": "whrc_aboveground_biomass_stock_2000",
    "version": "v201911",
    "pixel_meaning": "Mg_ha-1",
    "data_type": "uint16",
    "no_data": 0,
}
LAYER = layers.layer_factory(LayerModel.parse_obj(LAYER_DICT))
LOGGER = get_module_logger(__name__)
TILE = Tile("10N_010E", LAYER.grid, LAYER)


class Img(object):
    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        pass

    @staticmethod
    def read_masks(band: int = 0, window: Optional[Window] = None) -> np.ndarray:
        return np.array([[0, 0, 0], [0, 1, 0], [0, 0, 0]])

    @staticmethod
    def block_windows(idx: int):
        for i in range(0, 2):
            yield (0, i), Window(i, 0, 1, 1)

    profile: Dict[str, Any] = {
        "transform": Affine(0, 2, 0, 0, -2, 0),
        "width": 3,
        "height": 3,
        "crs": CRS.from_epsg(4326),
        "blockxsize": 16,
        "blockysize": 16,
        "dtype": np.dtype("uint"),
    }
    bounds: box = box(1, 1, 0, 0)


class EmptyImg(Img):
    @staticmethod
    def read_masks(band: int = 0, window: Optional[Window] = None) -> np.ndarray:
        return np.array([[0, 0, 0], [0, 0, 0], [0, 0, 0]])


def test_tile():
    assert isinstance(TILE, Tile)


def test_dst_exists():
    assert TILE.dst[TILE.default_format].exists()


def test_set_local_src():
    try:
        TILE.set_local_dst(TILE.default_format)
    except FileNotFoundError as e:
        assert (
            str(e)
            == f"File does not exist: whrc_aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/{TILE.default_format}/10N_010E.tif"
        )

    with mock.patch("os.remove", return_value=None):
        with mock.patch("rasterio.open", return_value=Img()):
            TILE.set_local_dst(TILE.default_format)
            assert isinstance(TILE.local_dst[TILE.default_format], RasterSource)
            assert (
                TILE.local_dst[TILE.default_format].uri
                == f"whrc_aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/{TILE.default_format}/10N_010E.tif"
            )


def test_get_local_dst_uri():
    assert (
        TILE.get_local_dst_uri(TILE.default_format)
        == f"whrc_aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/{TILE.default_format}/10N_010E.tif"
    )
    assert (
        TILE.get_local_dst_uri("gdal-geotiff")
        == "whrc_aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/gdal-geotiff/10N_010E.tif"
    )


def test_upload():
    s3_client = get_s3_client()
    resp = s3_client.list_objects_v2(
        Bucket=BUCKET, Prefix="whrc_aboveground_biomass_stock_2000"
    )
    assert resp["KeyCount"] == 1
    os.makedirs(
        "whrc_aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/geotiff/",  # pragma: allowlist secret
        exist_ok=True,
    )
    with open(
        "whrc_aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/geotiff/20N_010E.tif",
        "w+",
    ):
        pass
    tile = Tile("20N_010E", LAYER.grid, LAYER)
    with mock.patch("rasterio.open", return_value=EmptyImg()):
        tile.set_local_dst(TILE.default_format)
        tile.upload()

    resp = s3_client.list_objects_v2(
        Bucket=BUCKET, Prefix="whrc_aboveground_biomass_stock_2000"
    )
    assert resp["KeyCount"] == 2


@mock.patch("gfw_pixetl.tiles.tile.os")
def test_rm_local_src(mocked_os):
    with mock.patch("rasterio.open", return_value=EmptyImg()):
        TILE.set_local_dst(TILE.default_format)
        uri = TILE.local_dst[TILE.default_format].uri
        TILE.rm_local_src(TILE.default_format)
        mocked_os.remove.assert_called_with(uri)


def test__dst_has_no_data():
    print(LAYER.dst_profile)
    assert TILE.dst[TILE.default_format].has_no_data()


def test_gradient_symbology():
    layer_dict = {
        "dataset": "whrc_aboveground_biomass_stock_2000",
        "version": "v4",
        "pixel_meaning": "Mg_ha-1",
        "data_type": "uint16",
        "grid": "1/4000",
        "source_type": "raster",
        "no_data": 0,
        "symbology": {
            "type": "gradient",
            "colormap": {
                "1": {"red": 255, "green": 0, "blue": 0},
                "5": {"red": 0, "green": 0, "blue": 255},
            },
        },
    }

    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))

    tile = Tile("01N_001E", layer.grid, layer)

    test_file = os.path.join(TILE.tmp_dir, "test_gradient_color.tif")
    shutil.copyfile(TILE_4_PATH, test_file)

    # monkey patch method to point to test file
    # then initialize local destination
    tile.get_local_dst_uri = lambda x: test_file
    tile.set_local_dst(tile.default_format)

    assert tile.local_dst[tile.default_format].profile["count"] == 1

    tile.add_symbology()
    sleep(60)
    assert (
        os.path.basename(tile.local_dst[tile.default_format].uri)
        == f"{tile.tile_id}_colored.tif"
    )
    assert tile.local_dst[tile.default_format].profile["count"] == 4
    assert (
        tile.local_dst[tile.default_format].blockxsize
        == layer.dst_profile["blockxsize"]
    )
    assert (
        tile.local_dst[tile.default_format].blockysize
        == layer.dst_profile["blockysize"]
    )


def test_discrete_symbology():
    layer_dict = {
        "dataset": "whrc_aboveground_biomass_stock_2000",
        "version": "v4",
        "pixel_meaning": "Mg_ha-1",
        "data_type": "uint16",
        "grid": "1/4000",
        "source_type": "raster",
        "no_data": 0,
        "symbology": {
            "type": "discrete",
            "colormap": {
                "1": {"red": 255, "green": 0, "blue": 0},
                "2": {"red": 255, "green": 255, "blue": 0},
                "3": {"red": 0, "green": 255, "blue": 0},
                "4": {"red": 0, "green": 255, "blue": 255},
                "5": {"red": 0, "green": 0, "blue": 255},
            },
        },
    }

    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))

    tile = Tile("01N_001E", layer.grid, layer)

    test_file = os.path.join(TILE.tmp_dir, "test_discrete_color.tif")
    shutil.copyfile(TILE_4_PATH, test_file)

    # monkey patch method to point to test file
    # then initialize local destination
    tile.get_local_dst_uri = lambda x: test_file
    tile.set_local_dst(tile.default_format)

    assert tile.local_dst[tile.default_format].profile["count"] == 1
    with rasterio.open(tile.local_dst[tile.default_format].uri) as src, pytest.raises(
        ValueError
    ):
        src.colormap(1)

    tile.add_symbology()

    assert tile.local_dst[tile.default_format].uri == test_file
    assert tile.local_dst[tile.default_format].profile["count"] == 1

    _colormap = layer.symbology.colormap
    colormap = {0: (0, 0, 0, 255)}
    for pixel_value in _colormap:
        colormap[int(pixel_value)] = tuple(_colormap[pixel_value].dict().values())

    for i in range(6, 256):
        colormap[i] = (0, 0, 0, 255)

    with rasterio.open(tile.local_dst[tile.default_format].uri) as src:
        assert src.colormap(1) == colormap
