from typing import Dict

from aenum import Enum, extend_enum

from gfw_pixetl import get_module_logger

from . import Grid
from .lat_lng_grid import LatLngGrid
from .wm_grid import WebMercatorGrid

LOGGER = get_module_logger(__name__)


grids: Dict[str, Grid] = {
    "1/4000": LatLngGrid(1, 4000),  # TEST grid
    "3/33600": LatLngGrid(3, 33600),  # RAAD alerts, ~10m pixel
    "10/40000": LatLngGrid(10, 40000),  # UMD alerts, ~30m pixel
    "8/32000": LatLngGrid(8, 32000),  # UMD alerts, ~30m pixel, data cube optimized Grid
    "90/27008": LatLngGrid(90, 27008),  # VIIRS Fire alerts, ~375m pixel
    "90/9984": LatLngGrid(90, 9984),  # MODIS Fire alerts, ~1000m pixel
}

for zoom in range(0, 23):
    grids[f"zoom_{zoom}"] = WebMercatorGrid(zoom)


class GridEnum(str, Enum):
    pass


# Dynamically create string enum for grid keys
for key in grids.keys():
    extend_enum(GridEnum, key, key)


def grid_factory(grid_name) -> Grid:
    """Different Grid layout used for this project."""

    try:
        grid = grids[grid_name]
    except KeyError:

        message = f"Unknown grid name: {grid_name}"
        LOGGER.exception(message)
        raise ValueError(message)

    return grid
