import datetime
import os
from math import floor
from typing import Optional

from pyproj import CRS, Transformer
from rasterio.windows import Window
from shapely.geometry import MultiPolygon

from gfw_pixetl import get_module_logger
from gfw_pixetl.models.types import Bounds
from gfw_pixetl.settings.globals import GLOBALS

LOGGER = get_module_logger(__name__)

TOKEN_EXPIRATION: Optional[datetime.datetime] = None
AWS_ACCESS_KEY_ID: Optional[str] = None
AWS_SECRET_ACCESS_KEY: Optional[str] = None
AWS_SESSION_TOKEN: Optional[str] = None
AVAILABLE_MEMORY: Optional[int] = None
WORKERS: int = 1


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


def available_memory_per_process_bytes() -> float:
    return available_memory_per_process_mb() * 1000000


def available_memory_per_process_mb() -> float:
    mem = GLOBALS.max_mem / GLOBALS.workers
    LOGGER.info(f"Available memory per worker set to {mem}")
    return mem


def get_co_workers() -> int:
    return floor(GLOBALS.cores / GLOBALS.workers)


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


def intersection(a: MultiPolygon, b: Optional[MultiPolygon]) -> Optional[MultiPolygon]:
    if b:
        geom = None
        _geom = a.intersection(b)
        if _geom.type == "GeometryCollection":
            for g in _geom.geoms:
                if g.type == "MultiPolygon" or g.type == "Polygon":
                    geom = g
                    break
        else:
            geom = _geom
        return geom
    else:
        return a
