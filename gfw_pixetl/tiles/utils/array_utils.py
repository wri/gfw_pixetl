from typing import cast

import numpy as np
from numpy.ma import MaskedArray

from gfw_pixetl import get_module_logger

LOGGER = get_module_logger(__name__)


def set_datatype(
    array: MaskedArray,
    dst_window: str,
    nodata_value,
    datatype: str,
    tile_id: str,
) -> np.ndarray:
    """Update data type to desired output datatype Update nodata value to
    desired nodata value (current no data values will be updated and any values
    which already has new no data value will stay as is)"""
    if nodata_value is None:
        LOGGER.debug(f"Set datatype for {dst_window} of tile {tile_id}")
        array = array.data.astype(datatype)
    elif isinstance(nodata_value, list):
        LOGGER.debug(
            f"Set datatype for entire array and no data value for each band for {dst_window} of tile {tile_id}"
        )
        # make mypy happy. not sure why the isinstance check above alone doesn't do it
        nodata_list = cast(list, nodata_value)
        array = np.array(
            [np.ma.filled(array[i], nodata) for i, nodata in enumerate(nodata_list)]
        ).astype(datatype)

    else:
        LOGGER.debug(
            f"Set datatype and no data value for {dst_window} of tile {tile_id}"
        )
        array = np.ma.filled(array, nodata_value).astype(datatype)

    return array


def block_has_data(band_arrays: MaskedArray, tile_id) -> bool:
    """Check if current block has any data."""
    size = 0
    for i, masked_array in enumerate(band_arrays):
        msk = np.invert(masked_array.mask.astype(bool))
        data_pixels = msk[msk].size
        size += data_pixels
        LOGGER.debug(
            f"Block of tile {tile_id}, band {i+1} has {data_pixels} data pixels"
        )
    return band_arrays.shape[1] > 0 and band_arrays.shape[2] > 0 and size != 0
