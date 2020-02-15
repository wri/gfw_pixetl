import json
import os
from functools import lru_cache
from typing import Any, Dict, Optional, Tuple, Union
from urllib.parse import urlparse

import boto3
import numpy as np
import rasterio
from pyproj import Transformer, CRS
from rasterio.crs import CRS as rCRS
from rasterio.coords import BoundingBox
from rasterio.errors import RasterioIOError
from rasterio.windows import Window
from shapely.geometry import shape, MultiPolygon, Polygon

from gfw_pixetl import get_module_logger
from gfw_pixetl.connection import PgConn
from gfw_pixetl.decorators import lazy_property
from gfw_pixetl.utils import get_bucket

LOGGER = get_module_logger(__name__)

Windows = Tuple[Window, Window]
Bounds = Tuple[float, float, float, float]


class Source(object):
    pass


class VectorSource(Source):
    def __init__(self, table_name) -> None:
        self.conn: PgConn = PgConn()
        self.table_name: str = table_name


class _RasterSource(Source):
    def __init__(self, profile: Dict[str, Any], bounds: BoundingBox, uri: str) -> None:
        self.profile: Dict[str, Any] = profile
        self.bounds: BoundingBox = bounds
        self.uri: str = uri

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
    def dtype(self) -> np.dtype:
        return self.profile["dtype"]

    @dtype.setter
    def dtype(self, v: np.dtype) -> None:
        self.profile["dtype"] = v

    def has_no_data(self) -> bool:
        if self.nodata == 0 or self.nodata:
            return True
        else:
            return False


class RasterSource(_RasterSource):
    @lazy_property
    def geom(self) -> MultiPolygon:
        o = urlparse(self.uri, allow_fragments=False)
        bucket: str = o.netloc
        prefix: str = o.path.lstrip("/")
        prefix = os.path.join(os.path.dirname(prefix), "extent.geojson")
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if response["KeyCount"] and os.path.splitext(self.uri)[1] == ".vrt":
            # if we have a geojson representation of the exact extent we use it
            s3 = boto3.resource("s3")
            obj = s3.Object(bucket, prefix)
            body = obj.get()["Body"].read()
            feature = json.loads(body.decode("utf-8"))["features"][0]
            src_extent = MultiPolygon(shape(feature["geometry"]))
        else:
            # otherwise we use the bbox of the VRT (excepting that there will be a lot of empty tiles)
            left, bottom, right, top = self.reproject_bounds(CRS.from_epsg(4326))
            src_extent = MultiPolygon(
                [
                    Polygon(
                        [
                            (left, top),
                            (right, top),
                            (right, bottom),
                            (left, bottom),
                            (left, top),
                        ]
                    )
                ]
            )
        return src_extent

    @lru_cache(maxsize=2, typed=False)
    def reproject_bounds(self, crs: CRS) -> Bounds:
        """
        Reproject src bounds to dst CRT.
        Make sure that coordinates fall within real world coordinates system
        """

        LOGGER.debug(
            "SRC Extent: {}, {}, {}, {}".format(
                self.bounds.left,
                self.bounds.top,
                self.bounds.right,
                self.bounds.bottom,
            )
        )

        world_bounds = self._world_bounds(crs)

        clamped_bounds = self._clamp_bounds(*world_bounds)

        return self._reproject_bounds(crs, *clamped_bounds)

    def _world_bounds(self, crs: CRS) -> Bounds:
        """
        Get world bounds in src CRT
        """
        proj = Transformer.from_crs(crs, self.profile["crs"], always_xy=True)

        # Get World Extent in Source Projection
        # Important: We have to get each top, left, right, bottom seperately.
        # We cannot get them using the corner coordinates.
        # For some projections such as Goode (epsg:54052) this would cause strange behavior
        top = proj.transform(0, 90)[1]
        left = proj.transform(-180, 0)[0]
        bottom = proj.transform(0, -90)[1]
        right = proj.transform(180, 0)[0]

        LOGGER.debug("World Extent: {}, {}, {}, {}".format(left, bottom, right, top))

        return left, bottom, right, top

    def _clamp_bounds(self, left, bottom, right, top) -> Bounds:
        """
        Make sure src bounds are within world extent
        """

        # Crop SRC Bounds to World Extent:
        clamp_left = max(left, self.bounds.left)
        clamp_top = min(top, self.bounds.top)
        clamp_right = min(right, self.bounds.right)
        clamp_bottom = max(bottom, self.bounds.bottom)

        LOGGER.debug(
            "Cropped Extent: {}, {}, {}, {}".format(
                clamp_left, clamp_bottom, clamp_right, clamp_top
            )
        )

        return clamp_left, clamp_bottom, clamp_right, clamp_top

    def _reproject_bounds(
        self, crs: CRS, left: float, bottom: float, right: float, top: float
    ) -> Bounds:
        """
        Reproject bounds to dst CRT
        """

        proj = Transformer.from_crs(self.crs, crs, always_xy=True)

        reproject_top = round(proj.transform(0, top)[1], 8)
        reproject_left = round(proj.transform(left, 0)[0], 8)
        reproject_bottom = round(proj.transform(0, bottom)[1], 8)
        reproject_right = round(proj.transform(right, 0)[0], 8)

        LOGGER.debug(
            "Inverted Copped Extent: {}, {}, {}, {}".format(
                reproject_left, reproject_bottom, reproject_right, reproject_top
            )
        )

        return reproject_left, reproject_bottom, reproject_right, reproject_top


class Destination(_RasterSource):
    @property
    def bucket(self):
        return get_bucket()

    @property
    def geom(self) -> Polygon:
        left, bottom, right, top = self.bounds
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
        if not self.uri:
            raise Exception("Tile URI is not set")
        try:
            get_src(f"s3://{self.bucket}/{self.uri}")
            return True
        except FileNotFoundError:
            return False


def get_src(uri: str) -> RasterSource:
    LOGGER.debug("Check if tile {} exists".format(uri))

    try:
        with rasterio.open(uri) as src:
            LOGGER.info(f"File {uri} exists")
            return RasterSource(uri=uri, profile=src.profile, bounds=src.bounds)

    except Exception as e:

        if _file_does_not_exist(e, uri):
            LOGGER.info(f"File does not exist {uri}")
            raise FileNotFoundError(f"File does not exist: {uri}")
        else:
            LOGGER.exception(f"Cannot open {uri}")
            raise


def _file_does_not_exist(e: Exception, uri: str) -> bool:
    return isinstance(e, RasterioIOError) and (
        "does not exist in the file system, and is not recognized as a supported dataset name."
        in str(e)
        or str(e) == "The specified key does not exist."
        or "No such file or directory" in str(e)
    )
