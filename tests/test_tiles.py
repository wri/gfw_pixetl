import os
from typing import Any, Dict, Optional
from unittest import mock

import numpy as np
from rasterio.windows import Window
from shapely.geometry import Point, box

from gfw_pixetl import layers
from gfw_pixetl.errors import GDALNoneTypeError
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.tiles import Tile

os.environ["ENV"] = "test"

GRID = grid_factory("10/40000")
RASTER_LAYER: Dict[str, Any] = {
    "name": "whrc_aboveground_biomass_stock_2000",
    "version": "v201911",
    "field": "Mg_ha-1",
    "grid": GRID,
}

LAYER_TYPE = layers._get_source_type(
    RASTER_LAYER["name"], RASTER_LAYER["field"], RASTER_LAYER["grid"].name
)

LAYER = layers.layer_factory(**RASTER_LAYER)

TILE = Tile(Point(10, 10), GRID, LAYER)


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

    profile: Dict[str, Any] = {}
    bounds: box = box(1, 1, 0, 0)


class EmptyImg(Img):
    @staticmethod
    def read_masks(band: int = 0, window: Optional[Window] = None) -> np.ndarray:
        return np.array([[0, 0, 0], [0, 0, 0], [0, 0, 0]])


def test_tile():
    assert isinstance(TILE, Tile)


def test_dst_exists():
    assert TILE.dst_exists()


def test_set_local_src():
    try:
        TILE.set_local_src("test")
    except FileNotFoundError as e:
        assert (
            str(e)
            == "File does not exist: whrc_aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/10N_010E__test.tif"
        )

    with mock.patch("os.remove", return_value=None):
        with mock.patch("rasterio.open", return_value=Img()):
            TILE.set_local_src("test")
            assert isinstance(TILE.local_src, RasterSource)
            assert (
                TILE.local_src.uri
                == "whrc_aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/10N_010E__test.tif"
            )


# def test_local_src_is_empty():
#     with mock.patch("os.remove", return_value=None):
#         with mock.patch("rasterio.open", return_value=Img()):
#             TILE.set_local_src("test")
#             assert not TILE.local_src_is_empty()
#
#         with mock.patch("rasterio.open", return_value=EmptyImg()):
#             TILE.set_local_src("test")
#             assert TILE.local_src_is_empty()


def test_get_stage_uri():
    assert (
        TILE.get_stage_uri("test")
        == "whrc_aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/10N_010E__test.tif"
    )


@mock.patch("gfw_pixetl.tiles.tile.os")
def test_upload(mocked_os):
    with mock.patch("rasterio.open", return_value=EmptyImg()):
        TILE.set_local_src("test")
        with mock.patch("boto3.client") as MockClient:
            mocked_client = MockClient.return_value
            mocked_client.upload_file.return_value = True

            TILE.upload()
            mocked_client.assrt_called_once_with("s3")
            mocked_client.upload_file.assert_called_once()


@mock.patch("gfw_pixetl.tiles.tile.os")
def test_rm_local_src(mocked_os):
    with mock.patch("rasterio.open", return_value=EmptyImg()):
        TILE.set_local_src("test")
        uri = TILE.local_src.uri
        TILE.rm_local_src()
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
    assert TILE._dst_has_no_data()
