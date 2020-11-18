from typing import List

import psycopg2
from psycopg2._psycopg import ProgrammingError
from sqlalchemy import Column, Table, select, table, text
from sqlalchemy.sql.elements import TextClause, literal_column

from gfw_pixetl import get_module_logger
from gfw_pixetl.data_type import to_gdal_data_type
from gfw_pixetl.errors import GDALError
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import VectorSrcLayer
from gfw_pixetl.sources import VectorSource
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.gdal import run_gdal_subcommand

logger = get_module_logger(__name__)


class VectorSrcTile(Tile):
    def __init__(self, tile_id: str, grid: Grid, layer: VectorSrcLayer) -> None:
        super().__init__(tile_id, grid, layer)
        self.src: VectorSource = layer.src

    def intersect_filter(self) -> TextClause:
        return text(
            f"""ST_Intersects(
                        geom,
                        ST_MakeEnvelope(
                            {self.bounds.left},
                            {self.bounds.bottom},
                            {self.bounds.right},
                            {self.bounds.top},
                            4326)
                    )"""
        )

    def intersection(self) -> TextClause:
        return text(
            f"""
            st_intersection(
                geom,
                ST_MakeEnvelope(
                    {self.bounds.left},
                    {self.bounds.bottom},
                    {self.bounds.right},
                    {self.bounds.top},
                    4326)
            )"""
        )

    def intersection_geom(self) -> TextClause:
        return text(
            f"""CASE
                        WHEN st_geometrytype({str(self.intersection())}) = 'ST_GeometryCollection'::text
                        THEN st_collectionextract({str(self.intersection())}, 3)
                        ELSE st_intersection(geom, {str(self.intersection())})
                END"""
        )

    def order_column(self, val) -> Column:
        if self.layer.order == "desc":
            order: Column = val.desc()
        elif self.layer.order == "asc":
            order = val.asc()
        else:
            order = val
        return order

    def src_table(self) -> Table:
        src_table: Table = table(self.src.table)
        src_table.schema = self.src.schema
        return src_table

    def src_vector_intersects(self) -> bool:

        try:
            logger.debug(f"Check if tile {self.tile_id} intersects with postgis table")
            conn = psycopg2.connect(
                dbname=self.src.conn.db_name,
                user=self.src.conn.db_user,
                password=self.src.conn.db_password,
                host=self.src.conn.db_host,
                port=self.src.conn.db_port,
            )

            cursor = conn.cursor()

            sql = (
                select([literal_column("1")])
                .select_from(self.src_table())
                .where(self.intersect_filter())
            )
            # exists_query = select([literal_column("exists")]).select_from(select_1)

            logger.debug(str(sql))

            cursor.execute(str(sql))

            try:
                exists = bool(cursor.fetchone()[0])
            except (ProgrammingError, TypeError):
                exists = False

            cursor.close()
            conn.close()
        except psycopg2.Error:
            logger.exception(
                "There was an issue when trying to connect to the database"
            )
            raise

        logger.debug(f"EXISTS: {exists}")

        if exists:
            logger.info(
                f"Tile id {self.tile_id} exists in database table {self.src.schema}.{self.src.table}"
            )
        else:
            logger.info(
                f"Tile id {self.tile_id} does not exists in database table {self.src.schema}.{self.src.table}"
            )
        return exists

    def rasterize(self) -> None:

        # stage = "rasterize"

        dst = self.get_local_dst_uri(self.default_format)
        logger.info(f"Create raster {dst}")

        cmd: List[str] = ["gdal_rasterize"]

        val_column = literal_column(str(self.layer.calc))
        geom_column = literal_column(str(self.intersection_geom()))

        sql = (
            select([val_column.label(self.layer.field), geom_column.label("geom")])
            .select_from(self.src_table())
            .where(self.intersect_filter())
            .order_by(self.order_column(val_column))
        )

        print(str(sql))

        if self.layer.rasterize_method == "count":
            cmd += ["-burn", "1", "-add"]
        else:
            cmd += ["-a", self.layer.field]

        if self.dst[self.default_format].has_no_data():
            cmd += ["-a_nodata", str(self.dst[self.default_format].nodata)]

        cmd += [
            "-sql",
            str(sql),
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
            to_gdal_data_type(self.dst[self.default_format].dtype),
            "-co",
            f"COMPRESS={self.dst[self.default_format].profile['compress']}",  # TODO: make compress property
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
            run_gdal_subcommand(cmd)
        except GDALError:
            logger.error(f"Could not rasterize tile {self.tile_id}")
            raise
        else:
            self.set_local_dst(self.default_format)

            # invoking gdal-geotiff and compute stats here
            # instead of in a separate stage to assure we don't run out of memory
            # the transform stage uses all available memory for concurrent processes.
            # Having another stage which needs a lot of memory might cause the process to crash
            self.postprocessing()
