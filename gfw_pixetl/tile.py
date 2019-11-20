import os
import subprocess as sp
from typing import List, Optional

import boto3
import numpy as np
import psycopg2
import rasterio
from botocore.exceptions import ClientError
from rasterio.coords import BoundingBox
from retrying import retry
from pyproj import Transformer
from shapely.geometry import Point

from gfw_pixetl import get_module_logger
from gfw_pixetl import utils
from gfw_pixetl.errors import GDALError, GDALNoneTypeError, retry_if_none_type_error
from gfw_pixetl.grid import Grid
from gfw_pixetl.layers import Layer, RasterSrcLayer, VectorSrcLayer
from gfw_pixetl.source import Destination, RasterSource, VectorSource

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

        self.tile_id: str = grid.pointGridId(origin)
        self.bounds: BoundingBox = BoundingBox(
            left=origin.x,
            bottom=origin.y - grid.height,
            right=origin.x + grid.width,
            top=origin.y,
        )
        self.dst = Destination(
            uri=f"{layer.prefix}/{self.tile_id}",
            profile=self.layer.dst_profile,
            bounds=self.bounds,
        )

    def dst_exists(self) -> bool:
        if not self.dst.uri:
            raise Exception("Tile URI is not set")
        try:
            utils.get_src(self.dst.uri)
            return True
        except FileExistsError:
            return False

    def set_local_src(self, stage: str) -> None:
        if self.local_src.uri:
            self.rm_local_src()

        uri = f"{self.layer.prefix}/{self.tile_id}__{stage}.tif"
        self.local_src = utils.get_src(uri)

    # def src_exists(self) -> bool:
    #
    #     if not self.src:
    #         raise ValueError("Tile source URI needs to be set")
    #     try:
    #         self._get_src(self.src.uri)
    #         return True
    #     except FileExistsError:
    #         return False

    def local_src_is_empty(self) -> bool:
        logger.debug(f"Check if tile {self.local_src.uri} is empty")
        with rasterio.open() as img:
            msk = img.read_masks(1).astype(bool)
        if msk[msk].size == 0:
            logger.debug(f"Tile {self.local_src.uri} is empty")
            return True
        else:
            logger.debug(f"Tile {self.local_src.uri} is not empty")
            return False

    def get_stage_uri(self, stage):
        return f"{self.layer.prefix}/{self.tile_id}__{stage}.tif"

    def upload(self, env):

        s3 = boto3.client("s3")

        try:
            logger.info(f"Upload tile {self.tile_id} to s3")
            s3.upload_file(self.local_src.uri, self.dst.get_bucket(env), self.dst.uri)
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
    def _run_gdal_subcommand(cmd):
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        o, e = p.communicate()

        if p.returncode != 0 and not e:
            raise GDALNoneTypeError(e)
        elif p.returncode != 0:
            raise GDALError(e)


class VectorSrcTile(Tile):
    def __init__(self, origin: Point, grid: Grid, layer: VectorSrcLayer) -> None:
        super().__init__(origin, grid, layer)
        self.src: VectorSource = layer.src

    def src_vector_intersects(self) -> bool:

        try:
            logger.debug("Check if tile intersects with postgis table")
            conn = psycopg2.connect(
                dbname=self.src.conn.db_name,
                user=self.src.conn.db_user,
                password=self.src.conn.db_password,
                host=self.src.conn.db_host,
                port=self.src.conn.db_port,
            )
            cursor = conn.cursor()
            exists_query = f"SELECT exists (SELECT 1 FROM {self.src.table_name}__{self.grid.name} WHERE tile_id__{self.grid.name} = '{self.tile_id}' LIMIT 1)"
            cursor.execute(exists_query)
            exists = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        except psycopg2.Error:
            logger.exception(
                "There was an issue when trying to connect to the database"
            )
            raise

        if exists:
            logger.info(
                f"Tile id {self.tile_id} exists in database table {self.src.table_name}"
            )
        else:
            logger.info(
                f"Tile id {self.tile_id} does not exists in database table {self.src.table_name}"
            )
        return exists

    def rasterize(self) -> None:

        stage = "rasterize"

        if self.layer.rasterize_method == "count":
            cmd_method: List[str] = ["-burn", "1", "-add"]
        else:
            cmd_method = ["-a", self.layer.field]

        if self.dst.profile["no_data"]:
            cmd_no_data: List[str] = ["-a_nodata", str(self.dst.profile["no_data"])]
        else:
            cmd_no_data = list()

        dst = self.get_stage_uri(stage)
        logger.info(f"Create raster {dst}")

        cmd: List[str] = (
            ["gdal_rasterize"]
            + cmd_method
            + [
                "-sql",
                f"select * from {self.layer.name}_{self.layer.version}__{self.grid.name} where tile_id__{self.grid.name} = '{self.tile_id}'",
                "-te",
                str(self.bounds.left),
                str(self.bounds.bottom),
                str(self.bounds.right),
                str(self.bounds.top),
                "-tr",
                str(self.grid.xres),
                str(self.grid.yres),
                "-a_srs",
                "EPSG:4326",
                "-ot",
                self.dst.profile["data_type"],
            ]
            + cmd_no_data
            + [
                "-co",
                f"COMPRESS={self.dst.profile['compression']}",
                "-co",
                "TILED=YES",
                "-co",
                f"BLOCKXSIZE={self.grid.blockxsize}",
                "-co",
                f"BLOCKYSIZE={self.grid.blockxsize}",
                # "-co", "SPARSE_OK=TRUE",
                "-q",
                self.src.conn.pg_conn(),
                dst,
            ]
        )

        logger.info("Rasterize tile " + self.tile_id)

        try:
            self._run_gdal_subcommand(cmd)
        except GDALError as e:
            logger.error(f"Could not rasterize tile {self.tile_id}")
            logger.exception(e)
            raise
        else:
            self.set_local_src(stage)


class RasterSrcTile(Tile):
    def __init__(self, origin: Point, grid: Grid, layer: RasterSrcLayer) -> None:
        super().__init__(origin, grid, layer)
        self.src: RasterSource = layer.src

    def src_tile_intersects(self) -> bool:
        """
        Check if target tile extent intersects with source extent.
        """

        proj = Transformer.from_crs(
            self.grid.srs, self.src.profile["crs"], always_xy=True
        )
        inverse = Transformer.from_crs(
            self.src.profile["crs"], self.grid.srs, always_xy=True
        )

        # Get World Extent in Source Projection
        # Important: We have to get each top, left, right, bottom seperately.
        # We cannot get them using the corner coordinates.
        # For some projections such as Goode (epsg:54052) this would cause strange behavior
        world_top = proj.transform(0, 90)[1]
        world_left = proj.transform(-180, 0)[0]
        world_bottom = proj.transform(0, -90)[1]
        world_right = proj.transform(180, 0)[0]

        # Crop SRC Bounds to World Extent:
        left = max(world_left, self.src.bounds.left)
        top = min(world_top, self.src.bounds.top)
        right = min(world_right, self.src.bounds.right)
        bottom = max(world_bottom, self.src.bounds.bottom)

        # Convert back to Target Projection
        cropped_top = inverse.transform(0, top)[1]
        cropped_left = inverse.transform(left, 0)[0]
        cropped_bottom = inverse.transform(0, bottom)[1]
        cropped_right = inverse.transform(right, 0)[0]

        logger.debug(
            "World Extent: {}, {}, {}, {}".format(
                world_left, world_top, world_right, world_bottom
            )
        )
        logger.debug(
            "SRC Extent: {}, {}, {}, {}".format(
                self.src.bounds.left,
                self.src.bounds.top,
                self.src.bounds.right,
                self.src.bounds.bottom,
            )
        )
        logger.debug("Cropped Extent: {}, {}, {}, {}".format(left, top, right, bottom))
        logger.debug(
            "Inverted Copped Extent: {}, {}, {}, {}".format(
                cropped_left, cropped_top, cropped_right, cropped_bottom
            )
        )

        src_bbox = BoundingBox(
            left=cropped_left,
            top=cropped_top,
            right=cropped_right,
            bottom=cropped_bottom,
        )

        return not rasterio.coords.disjoint_bounds(src_bbox, self.bounds)

    def transform(self, is_final=True) -> None:
        stage = "transform"
        dst = self.get_stage_uri(stage)

        if (
            self.dst.profile["no_data"] == 0 or self.dst.profile["no_data"]
        ):  # 0 evaluate as false, so need to list it here
            no_data_cmd: List[str] = ["-dstnodata", str(self.dst.profile["no_data"])]
        else:
            no_data_cmd = list()

        if is_final:
            final_cmd = [
                "-ot",
                self.dst.profile["data_type"],
                "-co",
                f"NBITS={self.dst.profile['nbits']}",
            ] + no_data_cmd
        else:
            final_cmd = list()

        cmd: List[str] = (
            [
                "gdalwarp",
                "-s_srs",
                self.src.profile["crs"].to_proj4(),
                "-t_srs",
                self.grid.srs.srs,
                "-tr",
                str(self.grid.xres),
                str(self.grid.yres),
                "-te",
                str(self.bounds.left),
                str(self.bounds.bottom),
                str(self.bounds.right),
                str(self.bounds.top),
                "-te_srs",
                self.grid.srs.srs,
                "-ovr",
                "NONE",
                "-co",
                f"COMPRESS=NONE",  # {self.data_type.compression}",
                "-co",
                "TILED=YES",
                "-co",
                f"BLOCKXSIZE={self.grid.blockxsize}",
                "-co",
                f"BLOCKYSIZE={self.grid.blockysize}",
                # "-co", "SPARSE_OK=TRUE",
                "-r",
                self.layer.resampling,
                "-q",
                "-overwrite",
            ]
            + final_cmd
            + [self.src.uri, dst]
        )

        logger.info(f"Transform tile {self.tile_id}")

        try:
            self._run_gdal_subcommand(cmd)
        except GDALError as e:
            logger.error(f"Could not transform file {dst}")
            logger.exception(e)
            raise
        else:
            self.set_local_src(stage)

    def compress(self):
        stage = "compress"
        dst = self.get_stage_uri(stage)

        cmd = [
            "gda_translate",
            "-co",
            f"COMPRESS={self.dst.profile['compression']}",
            self.local_src.uri,
            dst,
        ]

        logger.info(f"Compress tile {self.tile_id}")

        try:
            self._run_gdal_subcommand(cmd)
        except GDALError as e:
            logger.error(f"Could not compress file {dst}")
            logger.exception(e)
            raise
        else:
            self.set_local_src(stage)

    def update_values(self):
        stage = "update_values"
        dst = self.get_stage_uri(stage)

        with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
            src = rasterio.open(self.local_src.uri)

            dst = rasterio.open(dst, "w", **self.dst.profile)

            for block_index, window in src.block_windows(1):
                data = src.read(window=window, masked=True)
                data = self._apply_calc(data)
                data = self._set_no_data_calc(data)
                dst.write(data, window=window)
            src.close()
            dst.close()

    def _apply_calc(
        self, data
    ):  # can use type hints here b/c of the way we create function f from string. Mypy would thow an error
        # apply user submitted calculation

        funcstr = f"def f(A: np.ndarray) -> np.ndarray:\n    return {self.layer.calc}"
        exec(funcstr, globals())
        return f(data)  # noqa: F821

    def _set_no_data_calc(self, data):
        # update no data value if wanted
        if self.dst.profile["no_data"] == 0 or self.dst.profile["no_data"]:
            data = np.ma.filled(data, self.dst.profile["no_data"]).astype(
                self.dst.profile[
                    "data_type"
                ].to_numpy_dt()  # TODO find new home for to_numpy_dt(
            )

        else:
            data = data.data.astype(self.dst.profile["data_type"].to_numpy_dt())
        return data
