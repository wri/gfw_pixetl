import os
from typing import List

import geopandas
from retrying import retry
from sqlalchemy import Column, Table, select, table, text
from sqlalchemy.engine import ResultProxy, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.sql.elements import TextClause, literal_column

from gfw_pixetl import get_module_logger
from gfw_pixetl.data_type import to_gdal_data_type
from gfw_pixetl.errors import GDALError, retry_if_db_fell_over
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import VectorSrcLayer
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.sources import VectorSource
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.gdal import run_gdal_subcommand

logger = get_module_logger(__name__)

GEOMETRY_COLUMN = "geom"


class VectorSrcTile(Tile):
    def __init__(self, tile_id: str, grid: Grid, layer: VectorSrcLayer) -> None:
        super().__init__(tile_id, grid, layer)
        self.src: VectorSource = layer.src

    def intersect_filter(self) -> TextClause:
        return text(
            f"""ST_Intersects(
                        {GEOMETRY_COLUMN},
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
                {GEOMETRY_COLUMN},
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

    @retry(
        retry_on_exception=retry_if_db_fell_over,
        stop_max_attempt_number=7,
        wait_random_min=60000,
        wait_random_max=180000,
    )  # Wait 60-180s between retries
    def src_vector_intersects(self) -> bool:
        db_url: URL = URL(
            "postgresql+psycopg2",
            host=GLOBALS.db_host,
            port=GLOBALS.db_port,
            username=GLOBALS.db_username,
            password=GLOBALS.db_password,
            database=GLOBALS.db_name,
        )
        engine = create_engine(db_url)

        sql = (
            select([literal_column("gfw_fid")])
            .select_from(self.src_table())
            .where(self.intersect_filter())
            .limit(1)
        )

        with engine.begin() as conn:
            result: ResultProxy = conn.execute(sql)
            exists: bool = False if result.fetchone() is None else True

        logger.debug(
            f"Tile id {self.tile_id} "
            f"{'exists' if exists else 'does not exist'} "
            f"in database table {self.src.schema}.{self.src.table}"
        )
        return exists

    @retry(
        retry_on_exception=retry_if_db_fell_over,
        stop_max_attempt_number=7,
        wait_random_min=60000,
        wait_random_max=180000,
    )  # Wait 60-180s between retries
    def fetch_data(self) -> None:
        """Download all intersecting features to a local file."""
        prefix = f"{self.work_dir}"
        os.makedirs(f"{prefix}", exist_ok=True)

        dst = os.path.join(prefix, f"{self.tile_id}.parquet")

        db_url: URL = URL(
            "postgresql+psycopg2",
            host=GLOBALS.db_host,
            port=GLOBALS.db_port,
            username=GLOBALS.db_username,
            password=GLOBALS.db_password,
            database=GLOBALS.db_name,
        )
        engine = create_engine(db_url)

        val_column = literal_column(str(self.layer.calc))
        # geom_column = literal_column(GEOMETRY_COLUMN)
        geom_column = literal_column(str(self.intersection_geom()))

        sql = (
            select([val_column.label(self.layer.field), geom_column.label(GEOMETRY_COLUMN)])
            .select_from(self.src_table())
            .where(self.intersect_filter())
            .order_by(self.order_column(val_column))
        )

        # Read the rows into memory and then dump them into a local file
        # for processing in the next stage
        # Why store as GeoParquet? Could be almost anything, but
        # GeoParquet is both faster and more compact (without extra
        # processing) than GeoPackage, Shapefiles, GeoJSON, CSV.
        geodataframe = geopandas.read_postgis(sql, engine)
        geodataframe.set_crs("EPSG:4326")
        geodataframe.to_parquet(dst, compression="snappy")

    def rasterize(self) -> None:
        """Rasterize all features from data fetched in previous stage."""
        src = f"{self.work_dir}/{self.tile_id}.parquet"
        dst = self.get_local_dst_uri(self.default_format)
        logger.info(f"Rasterizing {src} to {dst}")

        cmd: List[str] = ["gdal_rasterize"]

        if self.layer.rasterize_method == "count":
            cmd += ["-burn", "1", "-add"]
        else:
            cmd += ["-a", self.layer.field]

        if self.dst[self.default_format].nodata is not None:
            cmd += ["-a_nodata", str(self.dst[self.default_format].nodata)]

        cmd += [
            "-te",
            str(self.bounds.left),
            str(self.bounds.bottom),
            str(self.bounds.right),
            str(self.bounds.top),
            "-tr",
            str(self.grid.xres),
            str(self.grid.yres),
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
            "-oo",
            f"GEOM_POSSIBLE_NAMES={GEOMETRY_COLUMN}",
            src,
            dst,
        ]

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
