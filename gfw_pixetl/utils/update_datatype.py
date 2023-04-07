from typing import cast
import numpy as np
from numpy.ma import MaskedArray

from gfw_pixetl import get_module_logger

LOGGER = get_module_logger(__name__)


def update_datatype(array: MaskedArray, nodata_value, datatype) -> np.ndarray:
    """Update data type to desired output datatype Update nodata value to
    desired nodata value (current no data values will be updated and any
    values which already has new no data value will stay as is)"""
    if nodata_value is None:
        LOGGER.debug(f"Setting datatype only")
        array = array.data.astype(datatype)
    elif isinstance(nodata_value, list):
        LOGGER.debug(
            f"Setting datatype for entire array and NODATA value for each band"
        )
        # make mypy happy. not sure why the isinstance check above alone doesn't do it
        nodata_list = cast(list, nodata_value)
        array = np.array(
            [np.ma.filled(array[i], nodata) for i, nodata in enumerate(nodata_list)]
        ).astype(datatype)

    else:
        LOGGER.debug(
            f"Setting datatype and NODATA value"
        )
        array = np.ma.filled(array, nodata_value).astype(
            datatype
        )

    return array
