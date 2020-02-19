import os
import subprocess as sp
from typing import List, Tuple

import boto3
import rasterio
from botocore.exceptions import ClientError
from rasterio.coords import BoundingBox
from rasterio.crs import CRS
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

LOGGER = get_module_logger(__name__)
Bounds = Tuple[float, float, float, float]


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

        self.local_dst: RasterSource

        self.tile_id: str = grid.point_grid_id(origin)
        self.bounds: BoundingBox = BoundingBox(
            left=origin.x,
            bottom=origin.y - grid.height,
            right=origin.x + grid.width,
            top=origin.y,
        )

        tile_profile = {
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
        }
        tile_profile.update(self.layer.dst_profile)

        self.dst = Destination(
            uri=f"{layer.prefix}/{self.tile_id}.tif",
            profile=tile_profile,
            bounds=self.bounds,
        )

    def set_local_dst(self) -> None:
        if hasattr(self, "local_src"):
            self.rm_local_src()

        uri = self.get_local_dst_uri()
        self.local_dst = RasterSource(uri)

    def get_local_dst_uri(self) -> str:
        uri = f"{self.layer.prefix}/{self.tile_id}.tif"
        LOGGER.debug(f"Local Source URI: {uri}")
        return uri

    def upload(self) -> None:

        s3 = boto3.client("s3")

        try:
            LOGGER.info(f"Upload tile {self.tile_id} to s3")
            s3.upload_file(self.local_dst.uri, utils.get_bucket(), self.dst.uri)
        except ClientError:
            LOGGER.exception(f"Could not upload file {self.tile_id}")
            raise

    def rm_local_src(self) -> None:
        LOGGER.info(f"Delete local file {self.local_dst.uri}")
        os.remove(self.local_dst.uri)

    @staticmethod
    @retry(
        retry_on_exception=retry_if_none_type_error,
        stop_max_attempt_number=7,
        wait_fixed=2000,
    )
    def _run_gdal_subcommand(cmd: List[str]) -> Tuple[str, str]:

        env = utils.set_aws_credentials()
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
