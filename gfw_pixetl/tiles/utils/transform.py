from time import perf_counter
from typing import Optional, Sequence

import numpy as np
from numpy.ma import MaskedArray
from rasterio.windows import Window

from gfw_pixetl import get_module_logger
from gfw_pixetl.tiles.utils.array_utils import block_has_data, calc, set_datatype
from gfw_pixetl.tiles.utils.named_tuples import Destination, Layer, Source
from gfw_pixetl.tiles.utils.window_utils import read_window, write_window

LOGGER = get_module_logger(__name__)


def transform(
    tile_id,
    window: Window,
    layer: Layer,
    source: Source,
    destination: Destination,
    additional_destinations: Sequence[Destination] = (),
) -> Optional[str]:
    """Read, transform, and write one window, with coarse phase timings.

    One INFO record is emitted per window so CloudWatch can distinguish
    time spent reading/reprojecting source data from CPU-side
    calculation and local output writes without logging every low-level
    raster operation.
    """
    total_started = perf_counter()
    out_file: Optional[str] = None

    def m_bytes(arr):
        return arr.nbytes / 1000000

    phase_started = perf_counter()
    masked_array: MaskedArray = read_window(
        source.vrt,
        window,
        destination.transform,
        source.crs,
        destination.crs,
        layer.input_bands,
        tile_id,
    )
    read_seconds = perf_counter() - phase_started
    LOGGER.debug(
        f"Masked Array size for tile {tile_id} when read: {m_bytes(masked_array)} MB"
    )

    phase_started = perf_counter()
    has_data = block_has_data(masked_array, tile_id)
    data_check_seconds = perf_counter() - phase_started
    if not has_data:
        LOGGER.debug(f"{window} of tile {tile_id} has no data - skip")
        del masked_array
        LOGGER.info(
            "PERF window "
            f"tile={tile_id} read_s={read_seconds:.3f} "
            f"data_check_s={data_check_seconds:.3f} calc_s=0.000 "
            f"dtype_s=0.000 write_s=0.000 "
            f"total_s={perf_counter() - total_started:.3f} has_data=false"
        )
        return out_file

    LOGGER.debug(f"{window} of tile {tile_id} has data - continue")

    phase_started = perf_counter()
    masked_array = calc(
        masked_array, window, layer.calc_string, destination.count, tile_id
    )
    calc_seconds = perf_counter() - phase_started
    LOGGER.debug(
        f"Masked Array size for tile {tile_id} after calc: {m_bytes(masked_array)} MB"
    )

    phase_started = perf_counter()
    array: np.ndarray = set_datatype(
        masked_array, window, destination.no_data, destination.datatype, tile_id
    )
    dtype_seconds = perf_counter() - phase_started
    LOGGER.debug(
        f"Array size for tile {tile_id} after set dtype: {m_bytes(masked_array)} MB"
    )
    del masked_array

    phase_started = perf_counter()
    out_file = write_window(
        tile_id,
        destination.tmp_dir,
        destination.uri,
        destination.profile,
        array,
        window,
        destination.write_to_separate_files,
    )
    for extra_destination in additional_destinations:
        write_window(
            tile_id,
            extra_destination.tmp_dir,
            extra_destination.uri,
            extra_destination.profile,
            array,
            window,
            extra_destination.write_to_separate_files,
        )
    write_seconds = perf_counter() - phase_started
    del array

    LOGGER.info(
        "PERF window "
        f"tile={tile_id} read_s={read_seconds:.3f} "
        f"data_check_s={data_check_seconds:.3f} calc_s={calc_seconds:.3f} "
        f"dtype_s={dtype_seconds:.3f} write_s={write_seconds:.3f} "
        f"total_s={perf_counter() - total_started:.3f} has_data=true"
    )
    return out_file
