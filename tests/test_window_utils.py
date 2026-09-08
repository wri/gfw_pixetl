from unittest import mock

import numpy as np
from rasterio.windows import Window

from gfw_pixetl.tiles.utils.window_utils import _write_window_to_shared_file


def test_shared_file_open_does_not_pass_creation_profile():
    array = np.zeros((1, 1, 1), dtype="uint8")
    window = Window(0, 0, 1, 1)
    profile = {
        "driver": "GTiff",
        "compress": "DEFLATE",
        "blockxsize": 256,
        "blockysize": 256,
        "tiled": True,
    }

    dataset = mock.MagicMock()
    dataset.__enter__.return_value = dataset

    with (
        mock.patch("gfw_pixetl.tiles.utils.window_utils.rasterio.Env"),
        mock.patch(
            "gfw_pixetl.tiles.utils.window_utils.rasterio.open", return_value=dataset
        ) as rasterio_open,
    ):
        result = _write_window_to_shared_file(
            "/tmp/output.tif", profile, "00N_000E", array, window
        )

    rasterio_open.assert_called_once_with("/tmp/output.tif", "r+")
    dataset.write.assert_called_once()
    assert result == "/tmp/output.tif"
