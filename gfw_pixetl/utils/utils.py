import datetime
import itertools
import os
import string
import uuid
from functools import lru_cache
from math import floor
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import rasterio
from affine import Affine
from pyproj import CRS, Transformer
from rasterio.coords import BoundingBox
from rasterio.windows import Window
from retrying import retry
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union

from gfw_pixetl import get_module_logger
from gfw_pixetl.errors import _file_does_not_exist, retry_if_rasterio_error
from gfw_pixetl.models.types import Bounds
from gfw_pixetl.settings.gdal import GDAL_ENV
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.utils.path import create_dir

LOGGER = get_module_logger(__name__)

TOKEN_EXPIRATION: Optional[datetime.datetime] = None
AWS_ACCESS_KEY_ID: Optional[str] = None
AWS_SECRET_ACCESS_KEY: Optional[str] = None
AWS_SESSION_TOKEN: Optional[str] = None


class DummyTile(object):
    """A dummy tile."""

    def __init__(self, dst: Dict) -> None:
        self.dst: Dict = dst
        self.metadata: Dict = {}


def create_empty_file(work_dir, src_profile: Dict[str, Any]):
    local_file_path = os.path.join(work_dir, "input", f"{uuid.uuid1()}.tif")

    dtype = src_profile["dtype"]
    band_count = src_profile["count"]
    crs = src_profile["crs"]
    src_transform = src_profile["transform"]

    size_x = 1
    size_y = 1

    # Reminder of affine arguments:
    # a = width of a pixel
    # b = row rotation (typically zero)
    # c = x-coordinate of the upper-left corner of the upper-left pixel
    # d = column rotation (typically zero)
    # e = height of a pixel (typically negative)
    # f = y-coordinate of the upper-left corner of the upper-left pixel

    profile = {
        "driver": "GTiff",
        "dtype": dtype,
        "count": band_count,
        "nodata": 0,
        "width": size_x,
        "height": size_y,
        "crs": crs,
        "transform": Affine(
            size_x * src_profile["width"],
            src_transform.b,
            src_transform.c,
            src_transform.d,
            -(size_y * src_profile["height"]),
            src_transform.f,
        ),
    }

    LOGGER.info(f"Creating empty file with profile {profile}")

    data = np.zeros((band_count, size_x, size_y), dtype=dtype)

    create_dir(os.path.join(work_dir, "input"))

    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(local_file_path, "w", **profile) as dst:
            dst.write(data)

    return local_file_path


@retry(
    retry_on_exception=retry_if_rasterio_error,
    stop_max_attempt_number=7,
    wait_exponential_multiplier=1000,
    wait_exponential_max=300000,
)
def fetch_metadata(src_uri) -> Tuple[BoundingBox, Dict[str, Any]]:
    """Open file to fetch metadata."""
    LOGGER.debug(f"Fetch metadata for file {src_uri} if exists")

    try:
        with rasterio.Env(**GDAL_ENV), rasterio.open(src_uri) as src:
            # LOGGER.info(f"File {src_uri} exists")
            return src.bounds, src.profile

    except Exception as e:

        if _file_does_not_exist(e):
            # LOGGER.info(f"File does not exist {src_uri}")
            raise FileNotFoundError(f"File does not exist: {src_uri}")
        elif isinstance(e, rasterio.RasterioIOError):
            LOGGER.warning(
                f"RasterioIO Error while opening {src_uri}. Will make attempts to retry"
            )
            raise
        else:
            LOGGER.exception(f"Cannot open file {src_uri}")
            raise


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
    LOGGER.info(f"Available memory per worker set to {mem} MB")
    return mem


def get_co_workers() -> int:
    return max(1, floor(GLOBALS.num_processes / GLOBALS.workers))


def snapped_window(window: Window):
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
    """Get world bounds for given CRT."""

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

    # LOGGER.debug(f"World Extent of CRS {crs}: {left}, {bottom}, {right}, {top}")

    return left, bottom, right, top


def intersection(a: MultiPolygon, b: Optional[MultiPolygon]) -> MultiPolygon:
    if not b:
        geom: MultiPolygon = a
    else:
        _geom = a.intersection(b)
        # Sometimes the intersection results in a GeometryCollection and
        # includes things like LineStrings (like when two polygons both share
        # an edge and overlap elsewhere), which we don't care about. Filter
        # that stuff out to return a MultiPolygon.
        if _geom.type == "GeometryCollection":
            geom_pieces: List[Union[MultiPolygon, Polygon]] = list()
            for g in _geom.geoms:
                if g.type == "MultiPolygon" or g.type == "Polygon":
                    geom_pieces.append(g)
            geom = unary_union(geom_pieces)
        else:
            geom = _geom

    if geom.type == "Polygon":
        geom = MultiPolygon([geom])

    return geom


def union(
    a: Optional[MultiPolygon], b: Optional[MultiPolygon]
) -> Optional[MultiPolygon]:
    if not a and not b:
        geom: Optional[MultiPolygon] = None
    elif not a:
        geom = b
    elif not b:
        geom = a
    else:
        geom = unary_union([a, b])
        if geom.type == "Polygon":
            geom = MultiPolygon([geom])

    return geom


def _count_with_letters():
    """Generate an infinite sequence of strings of uppercase letters
    corresponding to numbers in base 26.

    Taken from https://stackoverflow.com/a/29351603.
    """
    for size in itertools.count(1):
        for letters in itertools.product(string.ascii_uppercase, repeat=size):
            yield "".join(letters)


@lru_cache(typed=False)
def enumerate_bands(num_bands: int) -> List[str]:
    """Return a variable name for each of num_bands."""

    if not isinstance(num_bands, int):
        raise ValueError(
            "num_bands must be an int... you're asking for an infinite loop!"
        )

    band_names: List[str] = list()
    for s in itertools.islice(_count_with_letters(), num_bands):
        band_names.append(s)
    return band_names
