from typing import Optional

import numpy as np
from numpy.ma import MaskedArray
from rasterio.windows import Window

from gfw_pixetl import get_module_logger
from gfw_pixetl.tiles.utils.array_utils import block_has_data, calc, set_datatype
from gfw_pixetl.tiles.utils.named_tuples import Destination, Layer, Source
from gfw_pixetl.tiles.utils.window_utils import read_window, write_window

LOGGER = get_module_logger(__name__)


def transform(
    tile_id, window: Window, layer: Layer, source: Source, destination: Destination
) -> Optional[str]:
    """Read windows from input VRT, reproject, resample, transform and write to
    destination."""
    out_file: Optional[str] = None

    def m_bytes(arr):
        return arr.nbytes / 1000000

    masked_array: MaskedArray = read_window(
        source.vrt,
        window,
        destination.transform,
        source.crs,
        destination.crs,
        layer.input_bands,
        tile_id,
    )
    LOGGER.debug(
        f"Masked Array size for tile {tile_id} when read: {m_bytes(masked_array)} MB"
    )

    if not block_has_data(masked_array, tile_id):
        LOGGER.debug(f"{window} of tile {tile_id} has no data - skip")
        del masked_array
        return out_file

    LOGGER.debug(f"{window} of tile {tile_id} has data - continue")

    masked_array = calc(
        masked_array, window, layer.calc_string, destination.count, tile_id
    )
    LOGGER.debug(
        f"Masked Array size for tile {tile_id} after calc: {m_bytes(masked_array)} MB"
    )
    array: np.ndarray = set_datatype(
        masked_array, window, destination.no_data, destination.datatype, tile_id
    )
    LOGGER.debug(
        f"Array size for tile {tile_id} after set dtype: {m_bytes(masked_array)} MB"
    )
    del masked_array
    out_file = write_window(
        tile_id,
        destination.tmp_dir,
        destination.uri,
        destination.profile,
        array,
        window,
        destination.write_to_separate_files,
    )
    del array
    return out_file
