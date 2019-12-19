import os
import subprocess as sp
from typing import List, Tuple

import boto3
import rasterio
from botocore.exceptions import ClientError
from rasterio.coords import BoundingBox
from retrying import retry
from shapely.geometry import Point

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.errors import GDALError, GDALNoneTypeError, retry_if_none_type_error
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import Layer
from gfw_pixetl.sources import Destination, RasterSource, get_src

logger = get_module_logger(__name__)


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

        self.local_src: RasterSource

        self.tile_id: str = grid.point_grid_id(origin)
        self.bounds: BoundingBox = BoundingBox(
            left=origin.x,
            bottom=origin.y - grid.height,
            right=origin.x + grid.width,
            top=origin.y,
        )
        self.dst = Destination(
            uri=f"{layer.prefix}/{self.tile_id}.tif",
            profile=self.layer.dst_profile,
            bounds=self.bounds,
        )

    def dst_exists(self) -> bool:
        if not self.dst.uri:
            raise Exception("Tile URI is not set")
        try:
            get_src(f"s3://{utils.get_bucket()}/{self.dst.uri}")
            return True
        except FileNotFoundError:
            return False

    def set_local_src(self, stage: str) -> None:
        if hasattr(self, "local_src"):
            self.rm_local_src()

        uri = f"{self.layer.prefix}/{self.tile_id}__{stage}.tif"
        self.local_src = get_src(uri)

    def local_src_is_empty(self) -> bool:
        logger.debug(f"Check if tile {self.local_src.uri} is empty")
        with rasterio.open(self.local_src.uri) as img:
            msk = img.read_masks(1).astype(bool)
        if msk[msk].size == 0:
            logger.debug(f"Tile {self.local_src.uri} is empty")
            return True
        else:
            logger.debug(f"Tile {self.local_src.uri} is not empty")
            return False

    def get_stage_uri(self, stage) -> str:
        return f"{self.layer.prefix}/{self.tile_id}__{stage}.tif"

    def upload(self) -> None:

        s3 = boto3.client("s3")

        try:
            logger.info(f"Upload tile {self.tile_id} to s3")
            s3.upload_file(self.local_src.uri, utils.get_bucket(), self.dst.uri)
        except ClientError:
            logger.exception(f"Could not upload file {self.tile_id}")
            raise

    def rm_local_src(self) -> None:
        logger.info(f"Delete local file {self.local_src.uri}")
        os.remove(self.local_src.uri)

    @staticmethod
    @retry(
        retry_on_exception=retry_if_none_type_error,
        stop_max_attempt_number=7,
        wait_fixed=2000,
    )
    def _run_gdal_subcommand(cmd: List[str]) -> Tuple[str, str]:
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        o, e = p.communicate()

        if p.returncode != 0 and not e:
            raise GDALNoneTypeError(e.decode("utf-8"))
        elif p.returncode != 0:
            raise GDALError(e)

        return o.decode("utf-8"), e.decode("utf-8")

    def _dst_has_no_data(self):
        return self.dst.profile["nodata"] == 0 or self.dst.profile["nodata"]
