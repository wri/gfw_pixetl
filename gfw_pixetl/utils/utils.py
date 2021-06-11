import datetime
import os
from math import floor
from typing import Dict, List, Optional, Union

import numpy
import rasterio
from affine import Affine
from pyproj import CRS, Transformer
from rasterio.windows import Window
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union

from gfw_pixetl import get_module_logger
from gfw_pixetl.models.types import Bounds
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


def create_empty_file(work_dir, dst_profile):
    local_file_path = os.path.join(work_dir, "input", "empty_file.tif")
    profile = {
        "driver": "GTiff",
        "dtype": dst_profile.get("dtype", rasterio.uint16),
        "nodata": dst_profile.get("no_data", 0),
        "count": 1,
        "width": 360,
        "height": 180,
        # "blockxsize": 100,
        # "blockysize": 100,
        "crs": dst_profile.get("crs", CRS.from_epsg(4326)),
        "transform": Affine(1, 0, -180, 0, -1, 90),
    }

    # FIXME: Make work with any nodata/dtype
    data = numpy.zeros((360, 180), dst_profile.get("dtype", rasterio.uint16))

    create_dir(os.path.join(work_dir, "input"))

    with rasterio.Env():
        with rasterio.open(local_file_path, "w", **profile) as dst:
            dst.write(data, 1)

    return local_file_path


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
    return floor(GLOBALS.num_processes / GLOBALS.workers)


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
