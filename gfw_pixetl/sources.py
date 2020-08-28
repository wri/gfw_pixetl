from functools import lru_cache
from typing import Any, Dict, Optional, Tuple, Union

import rasterio
from numpy import dtype as ndtype
from pyproj import CRS, Transformer
from rasterio.coords import BoundingBox
from rasterio.crs import CRS as rCRS
from rasterio.errors import RasterioIOError
from rasterio.windows import Window
from retrying import retry
from shapely.geometry import Polygon

from gfw_pixetl import get_module_logger
from gfw_pixetl.connection import PgConn
from gfw_pixetl.decorators import lazy_property
from gfw_pixetl.errors import retry_if_rasterio_error
from gfw_pixetl.settings.globals import GDAL_ENV
from gfw_pixetl.utils import get_bucket, replace_inf_nan, utils

LOGGER = get_module_logger(__name__)

Windows = Tuple[Window, Window]
Bounds = Tuple[float, float, float, float]


class Source:
    pass


class VectorSource(Source):
    def __init__(self, name: str, version: str) -> None:
        self.conn: PgConn = PgConn()
        self.schema: str = name
        self.table: str = version


class RasterSource(Source):
    def __init__(self, uri: str) -> None:

        self.profile: Dict[str, Any]
        self.bounds: BoundingBox

        self.uri: str = uri
        self.url: str = uri
        self.bounds, self.profile = self.fetch_meta()

    @lazy_property
    def geom(self) -> Polygon:
        left, bottom, right, top = self.reproject_bounds(CRS.from_epsg(4326))
        return Polygon(
            [[left, top], [right, top], [right, bottom], [left, bottom], [left, top]]
        )

    @property
    def transform(self) -> rasterio.Affine:
        return self.profile["transform"]

    @transform.setter
    def transform(self, v: rasterio.Affine) -> None:
        self.profile["transform"] = v

    @property
    def crs(self) -> rCRS:
        return self.profile["crs"]

    @crs.setter
    def crs(self, v: rCRS) -> None:
        self.profile["crs"] = v

    @property
    def height(self) -> float:
        return self.profile["height"]

    @height.setter
    def height(self, v: float) -> None:
        self.profile["height"] = v

    @property
    def width(self) -> float:
        return self.profile["width"]

    @width.setter
    def width(self, v: float) -> None:
        self.profile["width"] = v

    @property
    def nodata(self) -> Optional[Union[int, float]]:
        return self.profile["nodata"] if "nodata" in self.profile.keys() else None

    @nodata.setter
    def nodata(self, v: Union[int, float]) -> None:
        self.profile["nodata"] = v

    @property
    def blockxsize(self) -> int:
        return self.profile["blockxsize"]

    @blockxsize.setter
    def blockxsize(self, v: int) -> None:
        self.profile["blockxsize"] = v

    @property
    def blockysize(self) -> int:
        return self.profile["blockysize"]

    @blockysize.setter
    def blockysize(self, v: int) -> None:
        self.profile["blockysize"] = v

    @property
    def dtype(self) -> ndtype:
        return self.profile["dtype"]

    @dtype.setter
    def dtype(self, v: ndtype) -> None:
        self.profile["dtype"] = v

    def has_no_data(self) -> bool:
        return self.nodata is not None

    @lru_cache(maxsize=2, typed=False)
    def reproject_bounds(self, crs: CRS) -> Bounds:
        """Reproject src bounds to dst CRT.

        Make sure that coordinates fall within real world coordinates
        system
        """

        left, bottom, right, top = self.bounds

        LOGGER.debug(
            "SRC Extent: {}, {}, {}, {}".format(
                left,
                bottom,
                right,
                top,
            )
        )

        min_lng, min_lat, max_lng, max_lat = utils.world_bounds(crs)

        proj = Transformer.from_crs(self.crs, crs, always_xy=True)

        reproject_top = replace_inf_nan(round(proj.transform(0, top)[1], 8), max_lat)
        reproject_left = replace_inf_nan(round(proj.transform(left, 0)[0], 8), min_lng)
        reproject_bottom = replace_inf_nan(
            round(proj.transform(0, bottom)[1], 8), min_lat
        )
        reproject_right = replace_inf_nan(
            round(proj.transform(right, 0)[0], 8), max_lng
        )

        LOGGER.debug(
            "Reprojected, cropped Extent: {}, {}, {}, {}".format(
                reproject_left, reproject_bottom, reproject_right, reproject_top
            )
        )

        return reproject_left, reproject_bottom, reproject_right, reproject_top

    @retry(
        retry_on_exception=retry_if_rasterio_error,
        stop_max_attempt_number=7,
        wait_exponential_multiplier=1000,
        wait_exponential_max=300000,
    )
    def fetch_meta(self) -> Tuple[BoundingBox, Dict[str, Any]]:
        """Open file to fetch metadata."""
        LOGGER.debug(f"Fetch metadata data for file {self.url} if exists")

        try:
            with rasterio.Env(**GDAL_ENV):
                with rasterio.open(self.url) as src:
                    LOGGER.info(f"File {self.url} exists")
                    return src.bounds, src.profile

        except Exception as e:

            if _file_does_not_exist(e):
                LOGGER.info(f"File does not exist {self.url}")
                raise FileNotFoundError(f"File does not exist: {self.url}")
            elif isinstance(e, rasterio.RasterioIOError):
                LOGGER.warning(
                    f"RasterioIO Error while opening {self.url}. Will make attempts to retry"
                )
                raise
            else:
                LOGGER.exception(f"Cannot open file {self.url}")
                raise


class Destination(RasterSource):
    def __init__(self, uri: str, profile: Dict[str, Any], bounds: BoundingBox):
        # super().__init__(uri)  # we don't want to invoke __init__ from RasterSource here
        self.uri: str = uri
        self.url: str = f"/vsis3/{self.bucket}/{uri}"
        self.profile = profile
        self.bounds = bounds

    @property
    def bucket(self):
        return get_bucket()

    @property
    def geom(self) -> Polygon:
        left, bottom, right, top = self.reproject_bounds(CRS.from_epsg(4326))
        return Polygon(
            [[left, top], [right, top], [right, bottom], [left, bottom], [left, top]]
        )

    @property
    def filename(self) -> str:
        return self.uri.split("/")[-1]

    @property
    def prefix(self) -> str:
        return "/".join(self.uri.split("/")[:-1])

    def exists(self) -> bool:
        if not self.url:
            raise Exception("Tile URL is not set")
        try:
            self.fetch_meta()
            LOGGER.debug(f"File {self.url} exists")
            return True
        except FileNotFoundError:
            LOGGER.debug(f"File {self.url} does not exist")
            return False


def _file_does_not_exist(e: Exception) -> bool:
    """Check if RasterIO can access file.

    If file is inaccessible or does not exist, rasterio will always
    raise RasterioIOError. Error messages will differ, depending on the
    access method, if file exists or is inaccessible. However, end
    result should always be the same.
    """

    errors = [
        "does not exist in the file system, and is not recognized as a supported dataset name",
        "The specified key does not exist",
        "No such file or directory",
        "not recognized as a supported file format",
        "Access Denied",
    ]

    return isinstance(e, RasterioIOError) and any(error in str(e) for error in errors)
