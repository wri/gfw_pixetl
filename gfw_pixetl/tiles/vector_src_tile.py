from typing import List

import psycopg2
from shapely.geometry import Point

from gfw_pixetl import get_module_logger
from gfw_pixetl.data_type import to_gdal_dt
from gfw_pixetl.errors import GDALError
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import VectorSrcLayer
from gfw_pixetl.sources import VectorSource
from gfw_pixetl.tiles import Tile


logger = get_module_logger(__name__)


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

        # stage = "rasterize"

        dst = self.get_local_dst_uri(self.default_format)
        logger.info(f"Create raster {dst}")

        cmd: List[str] = ["gdal_rasterize"]

        if self.layer.rasterize_method == "count":
            cmd += ["-burn", "1", "-add"]
        else:
            cmd += ["-a", self.layer.field]

        if self.dst[self.default_format].profile["no_data"]:
            cmd += ["-a_nodata", str(self.dst[self.default_format].profile["no_data"])]

        cmd += [
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
            to_gdal_dt(self.dst[self.default_format].profile["data_type"]),
            "-co",
            f"COMPRESS={self.dst[self.default_format].profile['compression']}",
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

        logger.info("Rasterize tile " + self.tile_id)

        try:
            self._run_gdal_subcommand(cmd)
        except GDALError as e:
            logger.error(f"Could not rasterize tile {self.tile_id}")
            logger.exception(e)
            raise
        else:
            self.set_local_dst(self.default_format)
