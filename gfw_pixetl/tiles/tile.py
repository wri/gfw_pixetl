import copy
import os
import subprocess as sp
from typing import List, Tuple, Dict

import rasterio
from rasterio.coords import BoundingBox
from rasterio.crs import CRS
from rasterio.shutil import copy as raster_copy
from retrying import retry
from shapely.geometry import Point

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.errors import (
    GDALError,
    GDALAWSConfigError,
    GDALNoneTypeError,
    retry_if_none_type_error,
)
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import Layer
from gfw_pixetl.sources import Destination, RasterSource
from gfw_pixetl.utils.aws import get_s3_client

LOGGER = get_module_logger(__name__)
Bounds = Tuple[float, float, float, float]
S3 = get_s3_client()


class Tile(object):
    """
    A tile object which represents a single tile within a given grid
    """

    def __str__(self):
        return self.tile_id

    def __repr__(self):
        return f"Tile(tile_id={self.tile_id}, grid={self.grid.name})"

    def __hash__(self):
        return hash((self.tile_id, self.grid))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.tile_id == other.tile_id and self.grid == other.grid

    def __init__(self, origin: Point, grid: Grid, layer: Layer) -> None:

        self.grid: Grid = grid
        self.layer: Layer = layer

        self.local_dst: Dict[str, RasterSource] = dict()

        self.tile_id: str = grid.point_grid_id(origin)
        self.bounds: BoundingBox = BoundingBox(
            left=origin.x,
            bottom=origin.y - grid.height,
            right=origin.x + grid.width,
            top=origin.y,
        )

        gdal_profile = {
            "driver": "GTiff",
            "width": grid.cols,
            "height": grid.rows,
            "count": 1,
            "transform": rasterio.transform.from_origin(
                origin.x, origin.y, grid.xres, grid.yres
            ),
            "crs": CRS.from_string(
                grid.srs.to_string()
            ),  # Need to convert from ProjPy CRS to RasterIO CRS
            "sparse_ok": "TRUE",
            "interleave": "BAND",
        }
        gdal_profile.update(self.layer.dst_profile)

        # Drop GDAL specific optimizations which might not be readable by other applications
        geotiff_profile = copy.deepcopy(gdal_profile)
        geotiff_profile.pop("nbits", None)
        geotiff_profile.pop("sparse_ok", None)
        geotiff_profile.pop("interleave", None)
        geotiff_profile["compress"] = "DEFLATE"

        self.dst: Dict[str, Destination] = {
            "gdal-geotiff": Destination(
                uri=os.path.join(layer.prefix, "gdal-geotiff", f"{self.tile_id}.tif"),
                profile=gdal_profile,
                bounds=self.bounds,
            ),
            "geotiff": Destination(
                uri=os.path.join(layer.prefix, "geotiff", f"{self.tile_id}.tif"),
                profile=geotiff_profile,
                bounds=self.bounds,
            ),
        }

        self.default_format = "geotiff"
        self.status = "pending"

    def set_local_dst(self, dst_format) -> None:
        if hasattr(self, "local_src"):
            self.rm_local_src(dst_format)

        uri = self.get_local_dst_uri(dst_format)
        LOGGER.debug(f"Set Local Source URI: {uri}")
        self.local_dst[dst_format] = RasterSource(uri)

    def get_local_dst_uri(self, dst_format) -> str:

        prefix = f"{self.layer.prefix}/{dst_format}"
        LOGGER.debug(f"Attempt to create local folder {prefix} if not already exists")
        os.makedirs(f"{prefix}", exist_ok=True)

        uri = os.path.join(prefix, f"{self.tile_id}.tif")

        LOGGER.debug(f"Local Source URI: {uri}")
        return uri

    def create_gdal_geotiff(self) -> None:
        dst_format = "gdal-geotiff"
        if self.default_format != dst_format:
            LOGGER.info(
                f"Create copy of local file as Gdal Geotiff for tile {self.tile_id}"
            )

            raster_copy(
                self.local_dst[self.default_format].uri,
                self.get_local_dst_uri(dst_format),
                strict=False,
                **self.dst[dst_format].profile,
            )
            self.set_local_dst(dst_format)
        else:
            LOGGER.warning(
                f"Local file already Gdal Geotiff. Skip copying as Gdal Geotiff for tile {self.tile_id}"
            )

    def upload(self) -> None:

        try:
            for dst_format in self.local_dst.keys():
                LOGGER.info(f"Upload {dst_format} tile {self.tile_id} to s3")
                S3.upload_file(
                    self.local_dst[dst_format].uri,
                    utils.get_bucket(),
                    self.dst[dst_format].uri,
                )
        except Exception as e:
            LOGGER.error(f"Could not upload file {self.tile_id}")
            LOGGER.exception(str(e))
            self.status = "failed"

    def rm_local_src(self, dst_format) -> None:
        if dst_format in self.local_dst.keys() and os.path.isfile(
            self.local_dst[dst_format].uri
        ):
            LOGGER.info(f"Delete local file {self.local_dst[dst_format].uri}")
            os.remove(self.local_dst[dst_format].uri)

    @staticmethod
    @retry(
        retry_on_exception=retry_if_none_type_error,
        stop_max_attempt_number=7,
        wait_fixed=2000,
    )
    def _run_gdal_subcommand(cmd: List[str]) -> Tuple[str, str]:

        env = os.environ.copy()  # utils.set_aws_credentials()
        LOGGER.debug(f"RUN subcommand, using env {env}")
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, env=env)
        o, e = p.communicate()

        if p.returncode != 0 and not e:
            raise GDALNoneTypeError(e.decode("utf-8"))
        elif (
            p.returncode != 0
            and e
            == b"ERROR 15: AWS_SECRET_ACCESS_KEY and AWS_NO_SIGN_REQUEST configuration options not defined, and /root/.aws/credentials not filled\n"
        ):
            raise GDALAWSConfigError(e.decode("utf-8"))
        elif p.returncode != 0:
            raise GDALError(e.decode("utf-8"))

        return o.decode("utf-8"), e.decode("utf-8")
