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


@mock.patch("gfw_pixetl.tiles.tile.os")
def test_upload(mocked_os):
    with mock.patch("rasterio.open", return_value=EmptyImg()):
        TILE.set_local_dst(TILE.default_format)
        with mock.patch("boto3.client") as MockClient:
            mocked_client = MockClient.return_value
            mocked_client.upload_file.return_value = True

            TILE.upload()
            mocked_client.assrt_called_once_with("s3")
            mocked_client.upload_file.assert_called_once()


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
