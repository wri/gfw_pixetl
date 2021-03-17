from typing import List

import psycopg2
from psycopg2._psycopg import ProgrammingError
from sqlalchemy import Column, Table, select, table, text
from sqlalchemy.sql.elements import TextClause, literal_column

from gfw_pixetl import get_module_logger
from gfw_pixetl.data_type import to_gdal_data_type
from gfw_pixetl.errors import GDALError, RecordNotFoundError
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import VectorSrcLayer
from gfw_pixetl.sources import VectorSource
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.gdal import run_gdal_subcommand

logger = get_module_logger(__name__)


class VectorSrcTile(Tile):
    def __init__(self, tile_id: str, grid: Grid, layer: VectorSrcLayer) -> None:
        self.layer: VectorSrcLayer = layer
        super().__init__(tile_id, grid)
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

    def intersection_geom(self, geometry_type: int) -> TextClause:
        return text(
            f"""CASE
                        WHEN st_geometrytype({str(self.intersection())}) = 'ST_GeometryCollection'::text
                        THEN st_collectionextract({str(self.intersection())}, {geometry_type})
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

    def get_geometry_type(self) -> int:

        geometry_number = {
            "POINT": 1,
            "MULTIPOINT": 1,
            "LINESTRING": 2,
            "MULTILINESTRING": 2,
            "POLYGON": 3,
            "MULTIPOLYGON": 3,
        }

        sql = (
            select([literal_column("type")])
            .select_from(table("geometry_columns"))
            .where(text(f"f_table_schema = '{self.layer.name}'"))
            .where(text(f"f_table_name = '{self.layer.version}'"))
            .where(text("f_geometry_column = 'geom'"))
        )
        try:
            geometry_type = self._make_query(sql, fetch_one=True)[0]
        except TypeError:
            raise RecordNotFoundError(
                f"Dataset {self.layer.name}.{self.layer.version} has no geometry column or does not exist."
            )

        try:
            result = geometry_number[geometry_type]
        except KeyError:
            ValueError(f"Geometry type {geometry_type} not supported.")

        return result

    def src_vector_intersects(self) -> bool:

        logger.debug(f"Check if tile {self.tile_id} intersects with postgis table")

        sql = (
            select([literal_column("1")])
            .select_from(self.src_table())
            .where(self.intersect_filter())
        )

        try:
            exists = bool(self._make_query(sql, fetch_one=True)[0])
        except (ProgrammingError, TypeError):
            exists = False

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

        sql = self.compose_query()

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
            f"COMPRESS={self.dst[self.default_format].compress}",
            "-co",
            "TILED=YES",
            "-co",
            f"BLOCKXSIZE={self.grid.blockxsize}",
            "-co",
            f"BLOCKYSIZE={self.grid.blockxsize}",
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

    def compose_query(self):

        geometry_type: int = self.get_geometry_type()

        val_column = literal_column(str(self.layer.calc.field))
        geom_column = literal_column(str(self.intersection_geom(geometry_type)))

        sql = (
            select([val_column.label(self.layer.field), geom_column.label("geom")])
            .select_from(self.src_table())
            .where(self.intersect_filter())
            .order_by(self.order_column(val_column))
        )

        if self.layer.calc.where:
            sql = sql.where(text(self.layer.calc.where))

        if self.layer.calc.group_by:
            sql = sql.group_by(text(self.layer.calc.group_by))

        logger.debug(str(sql))

        return sql

    def _make_query(self, sql, fetch_one=False):
        try:
            conn = psycopg2.connect(
                dbname=self.src.conn.db_name,
                user=self.src.conn.db_user,
                password=self.src.conn.db_password,
                host=self.src.conn.db_host,
                port=self.src.conn.db_port,
            )

            cursor = conn.cursor()
            logger.debug(str(sql))
            cursor.execute(str(sql))

            if fetch_one:
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()
            cursor.close()
            conn.close()
        except psycopg2.Error:
            logger.exception(
                "There was an issue when trying to connect to the database"
            )
            raise
        else:
            return result
