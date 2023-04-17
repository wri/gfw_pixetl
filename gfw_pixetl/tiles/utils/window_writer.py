import numpy as np
import rasterio
from rasterio.windows import Window

from gfw_pixetl import get_module_logger
from gfw_pixetl.settings.gdal import GDAL_ENV

LOGGER = get_module_logger(__name__)


def write_window_to_shared_file(local_dst, default_format, dst1, tile_id,
                                array: np.ndarray, dst_window: Window
                                ) -> str:
    """Write blocks into output raster."""
    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(
                local_dst[default_format].uri,
                "r+",
                **dst1[default_format].profile,
        ) as dst:
            LOGGER.debug(f"Write {dst_window} of tile {tile_id}")
            dst.write(array, window=dst_window)
            del array
    return local_dst[default_format].uri
