import os
from copy import deepcopy

import numpy as np
import rasterio
from rasterio.windows import Window

from gfw_pixetl import get_module_logger
from gfw_pixetl.settings.gdal import GDAL_ENV

LOGGER = get_module_logger(__name__)


def _write_window_to_shared_file(
    uri, profile, tile_id, array: np.ndarray, dst_window: Window
) -> str:
    """Write blocks into output raster."""
    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(
            uri,
            "r+",
            **profile,
        ) as dst:
            LOGGER.debug(f"Write {dst_window} of tile {tile_id}")
            dst.write(array, window=dst_window)
            del array
    return uri


def _write_window_to_separate_file(
    tmp_dir, profile, tile_id, array: np.ndarray, dst_window: Window
) -> str:
    file_name = f"{tile_id}_{dst_window.col_off}_{dst_window.row_off}.tif"
    file_path = os.path.join(tmp_dir, file_name)

    profile = deepcopy(profile)
    transform = rasterio.windows.transform(dst_window, profile["transform"])
    profile.update(
        width=dst_window.width, height=dst_window.height, transform=transform
    )

    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(
            file_path,
            "w",
            **profile,
        ) as dst:
            LOGGER.debug(
                f"Write {dst_window} of tile {tile_id} to separate file {file_path}"
            )
            dst.write(array)
            del array
    return file_path


def write_window(
    tile_id,
    temp_dir,
    uri,
    profile,
    array: np.ndarray,
    dst_window: Window,
    write_to_separate_files: bool,
) -> str:
    if write_to_separate_files:
        out_file: str = _write_window_to_separate_file(
            temp_dir, profile, tile_id, array, dst_window
        )
    else:
        out_file = _write_window_to_shared_file(
            uri, profile, tile_id, array, dst_window
        )
    return out_file
