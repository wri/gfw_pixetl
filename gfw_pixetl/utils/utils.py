import datetime
import multiprocessing
import os
from typing import NamedTuple, Optional, Tuple

from pyproj import CRS, Transformer
from rasterio.windows import Window

from gfw_pixetl import get_module_logger
from gfw_pixetl.settings.globals import SETTINGS

LOGGER = get_module_logger(__name__)

TOKEN_EXPIRATION: Optional[datetime.datetime] = None
AWS_ACCESS_KEY_ID: Optional[str] = None
AWS_SECRET_ACCESS_KEY: Optional[str] = None
AWS_SESSION_TOKEN: Optional[str] = None
AVAILABLE_MEMORY: Optional[int] = None
WORKERS: int = 1

Bounds = Tuple[float, float, float, float]


class AreaOfUse(NamedTuple):
    """Area Of Use for projections.

    Copied from pyproj.aoi.AreaOfUse version 3.0 PyProj Version 2.6 does
    not expose this class.
    """

    #: West bound of area of use.
    west: float
    #: South bound of area of use.
    south: float
    #: East bound of area of use.
    east: float
    #: North bound of area of use.
    north: float
    #: Name of area of use.
    name: Optional[str] = None

    @property
    def bounds(self):
        return self.west, self.south, self.east, self.north

    def __str__(self):
        return f"- name: {self.name}\n" f"- bounds: {self.bounds}"


def get_bucket(env: Optional[str] = None) -> str:
    """compose bucket name based on environment."""

    if not env and "ENV" in os.environ:
        env = os.environ["ENV"]
    else:
        env = "dev"

    bucket = "gfw-data-lake"
    if env != "production":
        bucket += f"-{env}"
    return bucket


def set_workers(workers: int) -> int:
    """Set environment variable with number of workers Cannot exceed number of
    cores and must be at least one."""
    global WORKERS
    WORKERS = max(min(multiprocessing.cpu_count(), workers), 1)
    LOGGER.info(f"Set workers to {WORKERS}")
    return WORKERS


def get_workers() -> int:
    """Return number of workers for parallel jobs."""
    return WORKERS


# def set_available_memory() -> int:
#     global AVAILABLE_MEMORY
#     if not AVAILABLE_MEMORY:
#         AVAILABLE_MEMORY = psutil.virtual_memory()[1]
#         LOGGER.info(f"Total available memory set to {AVAILABLE_MEMORY}")
#     return AVAILABLE_MEMORY  # type: ignore


def available_memory_per_process() -> float:
    mem = SETTINGS.max_mem * 1000 / get_workers()  # Memory in bytes
    """Snapshot of currently available memory per core or process."""
    LOGGER.info(f"Available memory per worker set to {mem}")
    return mem


def snapped_window(window):
    """Make sure window is snapped to grid and contains full pixels to avoid
    missing rows and columns."""
    col_off, row_off, width, height = window.flatten()

    return Window(
        col_off=round(col_off),
        row_off=round(row_off),
        width=round(width),
        height=round(height),
    )


def world_bounds(crs: CRS) -> Bounds:
    """Get world bounds got given CRT."""

    from_crs = CRS(4326)

    proj = Transformer.from_crs(from_crs, crs, always_xy=True)

    _left, _bottom, _right, _top = crs.area_of_use.bounds

    # Get World Extent in Source Projection
    # Important: We have to get each top, left, right, bottom separately.
    # We cannot get them using the corner coordinates.
    # For some projections such as Goode (epsg:54052) this would cause strange behavior
    top = proj.transform(0, _top)[1]
    left = proj.transform(_left, 0)[0]
    bottom = proj.transform(0, _bottom)[1]
    right = proj.transform(_right, 0)[0]

    LOGGER.debug(f"World Extent of CRS {crs}: {left}, {bottom}, {right}, {top}")

    return left, bottom, right, top
