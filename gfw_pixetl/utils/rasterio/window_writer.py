import os
from copy import deepcopy

import numpy as np
import rasterio
from rasterio.windows import Window

from gfw_pixetl import get_module_logger
from gfw_pixetl.settings.gdal import GDAL_ENV

LOGGER = get_module_logger(__name__)


def _write_window_to_shared_file(uri, profile, array: np.ndarray,
                                 dst_window: Window) -> str:
    """Write blocks into output raster."""
    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(
                uri,
                "r+",
                **profile,
        ) as dst:
            dst.write(array, window=dst_window)
    return uri


def _write_window_to_separate_file(file_path, profile, array: np.ndarray) -> str:
    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(
                file_path,
                "w",
                **profile,
        ) as dst:
            dst.write(array)
    return file_path


def _update_profile(profile, dst_window):
    profile_copy = deepcopy(profile)
    transform = rasterio.windows.transform(dst_window, profile_copy["transform"])
    profile_copy.update(width=dst_window.width, height=dst_window.height,
                        transform=transform)
    return profile_copy


def _create_file_path(dst_window, tile_id, tmp_dir):
    file_name = f"{tile_id}_{dst_window.col_off}_{dst_window.row_off}.tif"
    return os.path.join(tmp_dir, file_name)


def write_window(tile_id, tmp_dir, profile, uri,
                 array: np.ndarray, dst_window: Window, write_to_separate_files: bool
                 ) -> str:
    if write_to_separate_files:
        file_path = _create_file_path(dst_window, tile_id, tmp_dir)
        updated_profile = _update_profile(profile, dst_window)
        LOGGER.debug(
            f"Write {dst_window} of tile {tile_id} to separate file {file_path}")
        out_file: str = _write_window_to_separate_file(file_path,
                                                       updated_profile, array,
                                                       dst_window)
    else:
        LOGGER.debug(f"Write {dst_window} of tile {tile_id}")
        out_file = _write_window_to_shared_file(uri, profile, array, dst_window)

    del array
    return out_file
