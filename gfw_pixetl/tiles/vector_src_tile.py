import os
from typing import List, Optional

import geopandas
from retrying import retry
from shapely import get_parts, unary_union
from shapely.geometry import Polygon, box
from shapely.geometry.base import BaseGeometry
from sqlalchemy import Column, Table, select, table, text
from sqlalchemy.engine import CursorResult, Engine, create_engine
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

# One engine (and its small connection pool) per worker *process*, created
# lazily and reused for every tile that process handles. Previously each of
# src_vector_intersects()/fetch_data() called create_engine() fresh on every
# single tile -- a brand new TCP+SSL handshake for a single query, with the
# connection then abandoned rather than returned to a pool -- which both
# wasted time and multiplied the number of connections the source DB had to
# service under load. Because worker processes are started with `spawn`,
# this module-level cache is naturally private to each process; there is no
# risk of sharing a connection across processes.
_ENGINE: Optional[Engine] = None


def _get_engine() -> Engine:
    """Return this worker process's cached SQLAlchemy engine, creating it on
    first use."""
    global _ENGINE
    if _ENGINE is None:
        db_url: URL = URL.create(
            "postgresql+psycopg2",
            host=GLOBALS.db_host,
            port=GLOBALS.db_port,
            username=GLOBALS.db_username,
            password=str(GLOBALS.db_password) if GLOBALS.db_password else None,
            database=GLOBALS.db_name,
        )
        _ENGINE = create_engine(
            db_url,
            pool_size=GLOBALS.db_pool_size,
            max_overflow=GLOBALS.db_pool_max_overflow,
            # Detect and transparently discard connections the DB (or a
            # proxy) has silently dropped, instead of failing the query.
            pool_pre_ping=True,
            pool_recycle=GLOBALS.db_pool_recycle_seconds,
            connect_args={
                "connect_timeout": GLOBALS.db_connect_timeout_seconds,
                # Cap how long any single query may hold this connection, so
                # one expensive intersection can't monopolize DB resources
                # (or this worker) indefinitely.
                "options": f"-c statement_timeout={GLOBALS.db_statement_timeout_ms}",
            },
        )
    return _ENGINE


def _clip_to_polygonal(geom: BaseGeometry, tile_box: BaseGeometry) -> BaseGeometry:
    """Clip *geom* to *tile_box*, keeping only its polygonal parts.

    Computed locally with Shapely/GEOS instead of in Postgres. Mirrors the
    PostGIS expression this replaces::

        CASE
            WHEN st_geometrytype(st_intersection(geom, envelope))
                 = 'ST_GeometryCollection'
            THEN st_collectionextract(st_intersection(geom, envelope), 3)
            ELSE st_intersection(geom, envelope)
        END

    A feature that only grazes the tile edge can intersect the envelope in
    a mix of dimensions (a sliver polygon plus a point or line where two
    edges just touch); rasterizing only cares about the polygonal part, so
    non-polygonal pieces of a GeometryCollection result are dropped, same
    as st_collectionextract(..., 3) did.
    """
    clipped = geom.intersection(tile_box)
    if clipped.geom_type == "GeometryCollection":
        polygons = [
            part
            for part in get_parts(clipped)
            if part.geom_type in ("Polygon", "MultiPolygon")
        ]
        clipped = unary_union(polygons) if polygons else Polygon()
    return clipped


class VectorSrcTile(Tile):
    def __init__(self, tile_id: str, grid: Grid, layer: VectorSrcLayer) -> None:
        super().__init__(tile_id, grid, layer)
        self.src: VectorSource = layer.src

    def intersect_filter(self) -> TextClause:
        return text(f"""ST_Intersects(
                        {GEOMETRY_COLUMN},
                        ST_MakeEnvelope(
                            {self.bounds.left},
                            {self.bounds.bottom},
                            {self.bounds.right},
                            {self.bounds.top},
                            4326)
                    )""")

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
        stop_max_attempt_number=12,
        wait_random_min=5000,
        wait_random_max=30000,
    )  # Wait 5-30s between retries (jittered, so concurrent workers don't
    # all hammer the DB again at the same instant once it recovers)
    def src_vector_intersects(self) -> bool:
        engine = _get_engine()

        sql = (
            select(literal_column("gfw_fid"))
            .select_from(self.src_table())
            .where(self.intersect_filter())
            .limit(1)
        )

        with engine.begin() as conn:
            result: CursorResult = conn.execute(sql)
            exists: bool = False if result.fetchone() is None else True

        logger.debug(
            f"Tile id {self.tile_id} "
            f"{'exists' if exists else 'does not exist'} "
            f"in database table {self.src.schema}.{self.src.table}"
        )
        return exists

    @retry(
        retry_on_exception=retry_if_db_fell_over,
        stop_max_attempt_number=12,
        wait_random_min=5000,
        wait_random_max=30000,
    )  # Wait 5-30s between retries (jittered, so concurrent workers don't
    # all hammer the DB again at the same instant once it recovers)
    def fetch_data(self) -> None:
        """Download all intersecting features to a local file, clipping
        them to the tile locally instead of in the database.

        ST_Intersects still runs in Postgres, in the WHERE clause, so the
        DB's GiST index does the (cheap) row-pruning it's good at. What
        used to also run in Postgres -- ST_Intersection actually clipping
        every matched geometry to the tile envelope, a much more expensive,
        per-row computation -- now happens here instead, after the raw
        geometry has been fetched, using the EC2 host's own idle CPU rather
        than the shared database's.
        """
        prefix = f"{self.work_dir}"
        os.makedirs(f"{prefix}", exist_ok=True)

        dst = os.path.join(prefix, f"{self.tile_id}.parquet")

        engine = _get_engine()

        val_column = literal_column(str(self.layer.calc))

        sql = (
            select(
                val_column.label(self.layer.field),
                literal_column(GEOMETRY_COLUMN).label(GEOMETRY_COLUMN),
            )
            .select_from(self.src_table())
            .where(self.intersect_filter())
            .order_by(self.order_column(val_column))
        )

        # Read the rows into memory and then dump them into a local file
        # for processing in the next stage
        # Why store as GeoParquet? Could be almost anything, but
        # GeoParquet is both faster and more compact (without extra
        # processing) than GeoPackage, Shapefiles, GeoJSON, CSV.
        geodataframe = geopandas.read_postgis(sql, engine, geom_col=GEOMETRY_COLUMN)
        geodataframe = geodataframe.set_crs("EPSG:4326")

        tile_box = box(
            self.bounds.left, self.bounds.bottom, self.bounds.right, self.bounds.top
        )
        geodataframe[GEOMETRY_COLUMN] = geodataframe[GEOMETRY_COLUMN].apply(
            lambda geom: _clip_to_polygonal(geom, tile_box)
        )

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
            "-a_srs",
            "EPSG:4326",
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
