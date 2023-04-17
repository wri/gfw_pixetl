from numpy.ma import MaskedArray
from rasterio.windows import Window
from gfw_pixetl import get_module_logger
from gfw_pixetl.utils.utils import enumerate_bands

LOGGER = get_module_logger(__name__)


def calc(layer, tile_id, dst, default_format, array: MaskedArray,
         dst_window: Window) -> MaskedArray:
    """Apply user defined calculation on array."""
    if layer.calc:
        # Assign a variable name to each band
        band_names = ", ".join(enumerate_bands(len(array)))
        funcstr = (
            f"def f({band_names}) -> MaskedArray:\n    return {layer.calc}"
        )
        LOGGER.debug(
            f"Apply function {funcstr} on block {dst_window} of tile {tile_id}"
        )
        exec(funcstr, globals())
        array = f(*array)  # type: ignore # noqa: F821

        # assign band index
        if len(array.shape) == 2:
            array = array.reshape(1, *array.shape)
        else:
            if array.shape[0] != dst[default_format].profile["count"]:
                raise RuntimeError(
                "Output band count does not match desired count. Calc function must be wrong."
                )
    else:
        LOGGER.debug(
            f"No user defined formula provided. Skip calculating values for {dst_window} of tile {tile_id}"
        )
    return array
