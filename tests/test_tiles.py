import os
from typing import Any, Dict, Optional
from unittest import mock
from unittest.mock import call

import numpy as np
import pytest
from rasterio import Affine
from rasterio.crs import CRS
from rasterio.windows import Window
from shapely.geometry import box

from gfw_pixetl import get_module_logger
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.aws import get_s3_client
from tests.conftest import BUCKET, LAYER_DICT

LOGGER = get_module_logger(__name__)


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


def test_tile(TILE):
    assert isinstance(TILE, Tile)


def test_dst_exists(TILE):
    assert TILE.dst[TILE.default_format].exists()


def test_set_local_src(TILE):

    with pytest.raises(FileNotFoundError):
        TILE.set_local_dst(TILE.default_format)

    with mock.patch("os.remove", return_value=None):
        with mock.patch("rasterio.open", return_value=Img()):
            TILE.set_local_dst(TILE.default_format)
            assert isinstance(TILE.local_dst[TILE.default_format], RasterSource)
            assert (
                TILE.local_dst[TILE.default_format].uri
                == f"/tmp/10N_010E/{TILE.default_format}/10N_010E.tif"
            )


def test_get_local_dst_uri(TILE):
    assert (
        TILE.get_local_dst_uri(TILE.default_format)
        == f"/tmp/10N_010E/{TILE.default_format}/10N_010E.tif"
    )
    assert (
        TILE.get_local_dst_uri("gdal-geotiff")
        == "/tmp/10N_010E/gdal-geotiff/10N_010E.tif"
    )


def test_upload(LAYER, TILE):
    s3_client = get_s3_client()
    resp = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=LAYER_DICT["dataset"])
    count = resp["KeyCount"]
    os.makedirs(
        "/tmp/20N_010E/geotiff/",  # pragma: allowlist secret
        exist_ok=True,
    )
    with open("/tmp/20N_010E/geotiff/20N_010E.tif", "w+"):
        pass
    with open("/tmp/20N_010E/geotiff/20N_010E.tif.aux.xml", "w+"):
        pass
    tile = Tile("20N_010E", LAYER.grid, LAYER)
    with mock.patch("rasterio.open", return_value=EmptyImg()):
        tile.set_local_dst(TILE.default_format)
        tile.upload()

    resp = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=LAYER_DICT["dataset"])

    assert resp["KeyCount"] == count + 2


@mock.patch("gfw_pixetl.tiles.tile.os")
def test_rm_local_src(mocked_os, TILE):
    with mock.patch("rasterio.open", return_value=EmptyImg()):
        TILE.set_local_dst(TILE.default_format)
        uri = TILE.local_dst[TILE.default_format].uri
        stats_uri = uri + ".aux.xml"
        TILE.rm_local_src(TILE.default_format)
        mocked_os.remove.assert_has_calls([call(uri), call(stats_uri)])


def test_dst_has_no_data(LAYER, TILE):
    print(LAYER.dst_profile)
    assert TILE.dst[TILE.default_format].has_no_data()
