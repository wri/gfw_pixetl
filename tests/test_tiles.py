import os
from typing import Any, Dict, Optional
from unittest import mock

import numpy as np
from rasterio.windows import Window
from rasterio import Affine
from rasterio.crs import CRS
from shapely.geometry import Point, box

from gfw_pixetl import layers
from gfw_pixetl.errors import GDALNoneTypeError
from gfw_pixetl.models import LayerModel
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.aws import get_s3_client
from tests import minimal_layer_dict
from tests.conftest import BUCKET

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

TILE = Tile(Point(10, 10), LAYER.grid, LAYER)


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
    tile = Tile(Point(10, 20), LAYER.grid, LAYER)
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


def test__run_gdal_subcommand():
    cmd = ["/bin/bash", "-c", "echo test"]
    assert TILE._run_gdal_subcommand(cmd) == ("test\n", "")

    try:
        cmd = ["/bin/bash", "-c", "exit 1"]
        TILE._run_gdal_subcommand(cmd)
    except GDALNoneTypeError as e:
        assert str(e) == ""


def test__dst_has_no_data():
    print(LAYER.dst_profile)
    assert TILE.dst[TILE.default_format].has_no_data()
