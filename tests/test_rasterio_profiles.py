from unittest.mock import MagicMock, patch

import numpy as np
from rasterio.windows import Window

from gfw_pixetl.tiles.utils.window_utils import write_window
from gfw_pixetl.utils.gdal import _copy_creation_profile


def test_copy_creation_profile_drops_dataset_fields():
    profile = {
        "driver": "GTiff",
        "width": 100,
        "height": 100,
        "count": 1,
        "transform": "transform",
        "crs": "EPSG:4326",
        "dtype": "uint8",
        "nodata": 0,
        "compress": "DEFLATE",
        "tiled": True,
    }
    assert _copy_creation_profile(profile) == {
        "driver": "GTiff",
        "compress": "DEFLATE",
        "tiled": True,
    }


def test_write_window_does_not_pass_creation_profile_when_reopening():
    dataset = MagicMock()
    cm = MagicMock()
    cm.__enter__.return_value = dataset
    with patch(
        "gfw_pixetl.tiles.utils.window_utils.rasterio.open", return_value=cm
    ) as opened:
        write_window(
            "tile",
            "/tmp",
            "/tmp/tile.tif",
            {"driver": "GTiff", "width": 1},
            np.zeros((1, 1, 1), dtype="uint8"),
            Window(0, 0, 1, 1),
            False,
        )
    opened.assert_called_once_with("/tmp/tile.tif", "r+")
