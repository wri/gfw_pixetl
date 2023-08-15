import os
from copy import deepcopy

import numpy as np
import rasterio
from numpy.ma import MaskedArray
from rasterio.vrt import WarpedVRT
from rasterio.warp import transform_bounds
from rasterio.windows import Window, bounds
from retrying import retry

from gfw_pixetl import get_module_logger
from gfw_pixetl.errors import retry_if_rasterio_io_error
from gfw_pixetl.models.types import Bounds
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


@retry(
    retry_on_exception=retry_if_rasterio_io_error,
    stop_max_attempt_number=7,
    wait_exponential_multiplier=1000,
    wait_exponential_max=300000,
)  # Wait 2^x * 1000 ms between retries by to 300 sec, then 300 sec afterwards.
def read_window(
    vrt: WarpedVRT,
    dst_window: Window,
    transform,
    source_crs,
    destination_crs,
    input_bands,
    tile_id,
) -> MaskedArray:
    """Read window of input raster."""
    dst_bounds: Bounds = bounds(dst_window, transform)
    window = vrt.window(*dst_bounds)

    src_bounds = transform_bounds(destination_crs, source_crs, *dst_bounds)

    LOGGER.debug(
        f"Read {dst_window} for Tile {tile_id} - this corresponds to bounds {src_bounds} in source"
    )

    shape = (
        len(input_bands),
        int(round(dst_window.height)),
        int(round(dst_window.width)),
    )

    try:
        return vrt.read(
            window=window,
            out_shape=shape,
            masked=True,
        )
    except rasterio.RasterioIOError as e:
        if "Access window out of range" in str(e) and (shape[1] == 1 or shape[2] == 1):
            LOGGER.warning(
                f"Access window out of range while reading {dst_window} for Tile {tile_id}. "
                "This is most likely due to subpixel misalignment. "
                "Returning empty array instead."
            )
            return np.ma.array(data=np.zeros(shape=shape), mask=np.ones(shape=shape))

        else:
            LOGGER.warning(
                f"RasterioIO error while reading {dst_window} for Tile {tile_id}. "
                "Will make attempt to retry."
            )
            raise
