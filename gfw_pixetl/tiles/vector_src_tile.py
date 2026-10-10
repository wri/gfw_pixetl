import os
from dataclasses import dataclass
from time import perf_counter
from typing import List, Optional, Tuple

import geopandas
from retrying import retry
from shapely import get_parts, unary_union
from shapely.geometry import Polygon, box
from shapely.geometry.base import BaseGeometry
from sqlalchemy import Column, Table, select, table, text
from sqlalchemy.engine import Engine, create_engine
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
from gfw_pixetl.utils.gdal import get_gdal_env, run_gdal_subcommand

logger = get_module_logger(__name__)

RASTER_SUBDIVISIONS = 4
RASTER_PART_COUNT = RASTER_SUBDIVISIONS**2

GEOMETRY_COLUMN = "geom"


@dataclass
class VectorRasterWorkItem:
    """One spatial subdivision of a vector tile's rasterization work."""

    parent: "VectorSrcTile"
    index: int
    bounds: Tuple[float, float, float, float]
    src: str
    dst: str

    @property
    def work_id(self) -> str:
        return f"{self.parent.tile_id}-part-{self.index}"


@dataclass
class VectorRasterAssemblyItem:
    """A parent tile whose four raster work items are complete."""

    parent: "VectorSrcTile"
    parts: List[VectorRasterWorkItem]


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
        db_url: URL = URL(
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
        # Vector rasterization is write-oriented and gains little from a large
        # GDAL cache, so bound each concurrent operation's cache explicitly.
        self.gdal_cachemax_mb: int = 512
        # Populated by src_vector_intersects(); zero for tiles it never ran
        # on (subset-filtered, or already existing at the destination) so
        # sorting by complexity_score never trips over a missing attribute.
        self.feature_count: int = 0
        self.total_vertices: int = 0
        self.complexity_score: int = 0

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
            select(
                [
                    literal_column("count(*)").label("feature_count"),
                    literal_column("coalesce(sum(st_npoints(geom)), 0)").label(
                        "total_vertices"
                    ),
                ]
            )
            .select_from(self.src_table())
            .where(self.intersect_filter())
        )

        with engine.begin() as conn:
            result = conn.execute(sql)
            row = result.fetchone()

        self.feature_count = int(row.feature_count) if row else 0
        self.total_vertices = int(row.total_vertices) if row else 0
        self.complexity_score = self.feature_count + self.total_vertices

        exists = self.feature_count > 0

        logger.debug(
            f"Tile id {self.tile_id} "
            f"{'exists' if exists else 'does not exist'} "
            f"in database table {self.src.schema}.{self.src.table} "
            f"(feature_count={self.feature_count}, "
            f"total_vertices={self.total_vertices})"
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
        """Download all intersecting features to a local file, clipping them to
        the tile locally instead of in the database.

        Postgres uses ST_Intersects and the GiST index to select
        candidate features. Geometry clipping then runs locally so
        expensive per-feature intersection work consumes worker CPU
        rather than shared database CPU.
        """
        prefix = f"{self.work_dir}"
        os.makedirs(f"{prefix}", exist_ok=True)

        dst = os.path.join(prefix, f"{self.tile_id}.parquet")

        engine = _get_engine()

        val_column = literal_column(str(self.layer.calc))

        sql = (
            select(
                [
                    val_column.label(self.layer.field),
                    literal_column(GEOMETRY_COLUMN).label(GEOMETRY_COLUMN),
                ]
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
        total_started = perf_counter()

        phase_started = perf_counter()
        logger.info(
            f"PERF vector_fetch_db_start tile={self.tile_id} "
            f"bounds=({self.bounds.left}, {self.bounds.bottom}, "
            f"{self.bounds.right}, {self.bounds.top}) "
            f"features={self.feature_count} vertices={self.total_vertices} "
            f"complexity={self.complexity_score}"
        )
        try:
            geodataframe = geopandas.read_postgis(sql, engine, geom_col=GEOMETRY_COLUMN)
        except Exception:
            logger.exception(
                f"PERF vector_fetch_db_failed tile={self.tile_id} "
                f"bounds=({self.bounds.left}, {self.bounds.bottom}, "
                f"{self.bounds.right}, {self.bounds.top}) "
                f"features={self.feature_count} vertices={self.total_vertices} "
                f"complexity={self.complexity_score} "
                f"elapsed_s={perf_counter() - phase_started:.3f}"
            )
            raise
        geodataframe = geodataframe.set_crs("EPSG:4326")
        db_read_seconds = perf_counter() - phase_started
        fetched_features = len(geodataframe)

        phase_started = perf_counter()
        tile_box = box(
            self.bounds.left, self.bounds.bottom, self.bounds.right, self.bounds.top
        )
        geodataframe[GEOMETRY_COLUMN] = geodataframe[GEOMETRY_COLUMN].apply(
            lambda geom: _clip_to_polygonal(geom, tile_box)
        )
        clip_seconds = perf_counter() - phase_started

        phase_started = perf_counter()
        geodataframe.to_parquet(dst, compression="snappy")
        parquet_write_seconds = perf_counter() - phase_started
        parquet_bytes = os.path.getsize(dst)

        phase_started = perf_counter()
        self._prepare_raster_work_items(geodataframe)
        raster_parts_seconds = perf_counter() - phase_started

        logger.info(
            f"PERF vector_fetch tile={self.tile_id} "
            f"features={self.feature_count} vertices={self.total_vertices} "
            f"complexity={self.complexity_score} fetched_features={fetched_features} "
            f"db_read_s={db_read_seconds:.3f} clip_s={clip_seconds:.3f} "
            f"parquet_write_s={parquet_write_seconds:.3f} "
            f"raster_parts_s={raster_parts_seconds:.3f} "
            f"parquet_bytes={parquet_bytes} "
            f"total_s={perf_counter() - total_started:.3f}"
        )

    def _raster_part_bounds(self) -> List[Tuple[float, float, float, float]]:
        """Return fixed compute-cell bounds in north-to-south row-major
        order."""
        width = (self.bounds.right - self.bounds.left) / RASTER_SUBDIVISIONS
        height = (self.bounds.top - self.bounds.bottom) / RASTER_SUBDIVISIONS
        bounds = []
        for row in range(RASTER_SUBDIVISIONS):
            top = self.bounds.top - row * height
            bottom = top - height
            for column in range(RASTER_SUBDIVISIONS):
                left = self.bounds.left + column * width
                right = left + width
                bounds.append((left, bottom, right, top))
        return bounds

    def _prepare_raster_work_items(self, geodataframe) -> None:
        """Write spatially clipped GeoParquets while source data is
        resident."""
        part_dir = os.path.join(self.work_dir, "raster_parts")
        os.makedirs(part_dir, exist_ok=True)

        for index, part_bounds in enumerate(self._raster_part_bounds()):
            part_box = box(*part_bounds)
            part = geodataframe[geodataframe.intersects(part_box)].copy()
            if not part.empty:
                part[GEOMETRY_COLUMN] = part[GEOMETRY_COLUMN].apply(
                    lambda geom: _clip_to_polygonal(geom, part_box)
                )
                part = part[~part[GEOMETRY_COLUMN].is_empty]
            part.to_parquet(
                os.path.join(part_dir, f"part-{index}.parquet"),
                compression="snappy",
            )

    def raster_work_items(self) -> List[VectorRasterWorkItem]:
        """Return the prepared rasterization work items."""
        part_dir = os.path.join(self.work_dir, "raster_parts")
        return [
            VectorRasterWorkItem(
                self,
                index,
                part_bounds,
                os.path.join(part_dir, f"part-{index}.parquet"),
                os.path.join(part_dir, f"part-{index}.tif"),
            )
            for index, part_bounds in enumerate(self._raster_part_bounds())
        ]

    def rasterize_work_item(self, work_item: VectorRasterWorkItem) -> None:
        """Rasterize one spatial work item without parent postprocessing."""
        left, bottom, right, top = work_item.bounds
        logger.info(
            f"Rasterizing {work_item.src} to {work_item.dst} "
            f"for {work_item.work_id}"
        )
        started = perf_counter()
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
            str(left),
            str(bottom),
            str(right),
            str(top),
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
            work_item.src,
            work_item.dst,
        ]
        env = {**get_gdal_env(), "GDAL_CACHEMAX": str(self.gdal_cachemax_mb)}
        run_gdal_subcommand(cmd, env=env)
        logger.info(
            f"PERF vector_rasterize_part tile={self.tile_id} "
            f"part={work_item.index} bounds={work_item.bounds} "
            f"parquet_bytes={os.path.getsize(work_item.src)} "
            f"rasterize_s={perf_counter() - started:.3f}"
        )

    def assemble_raster_work_items(
        self, work_items: List[VectorRasterWorkItem]
    ) -> None:
        """Mosaic four completed work items and finalize the parent tile."""
        started = perf_counter()
        part_dir = os.path.join(self.work_dir, "raster_parts")
        vrt = os.path.join(part_dir, "assembled.vrt")
        dst = self.get_local_dst_uri(self.default_format)
        input_list = os.path.join(part_dir, "parts.txt")
        with open(input_list, "w") as fp:
            ordered = sorted(work_items, key=lambda item: item.index)
            fp.write("\n".join(item.dst for item in ordered))

        run_gdal_subcommand(
            [
                "gdalbuildvrt",
                "-input_file_list",
                input_list,
                "-resolution",
                "highest",
                vrt,
            ]
        )
        cmd = [
            "gdal_translate",
            "-of",
            "GTiff",
            "-co",
            f"COMPRESS={self.dst[self.default_format].compress}",
            "-co",
            "TILED=YES",
            "-co",
            f"BLOCKXSIZE={self.grid.blockxsize}",
            "-co",
            f"BLOCKYSIZE={self.grid.blockxsize}",
        ]
        if self.dst[self.default_format].nodata is not None:
            cmd += ["-a_nodata", str(self.dst[self.default_format].nodata)]
        cmd += [vrt, dst]
        env = {**get_gdal_env(), "GDAL_CACHEMAX": str(self.gdal_cachemax_mb)}
        run_gdal_subcommand(cmd, env=env)
        self.set_local_dst(self.default_format)
        assemble_seconds = perf_counter() - started

        postprocess_started = perf_counter()
        self.postprocessing()
        postprocess_seconds = perf_counter() - postprocess_started
        logger.info(
            f"PERF vector_rasterize_assembled tile={self.tile_id} "
            f"parts={len(work_items)} assemble_s={assemble_seconds:.3f} "
            f"postprocess_s={postprocess_seconds:.3f} "
            f"total_s={perf_counter() - started:.3f}"
        )

    def rasterize(self) -> None:
        """Rasterize all features from data fetched in previous stage.

        Kept as the single-tile path for direct callers and focused
        tests. VectorPipe uses the 2x2 work-item path instead.
        """
        src = f"{self.work_dir}/{self.tile_id}.parquet"
        dst = self.get_local_dst_uri(self.default_format)
        parquet_bytes = os.path.getsize(src)
        logger.info(f"Rasterizing {src} to {dst}")
        total_started = perf_counter()

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
            # Small, fixed cache for this subprocess specifically -- see
            # Tile.gdal_cachemax_mb; a string here is fine (unlike
            # just_copy_geotiff's rasterio.Env call), since this dict
            # feeds a subprocess's OS environment, which is str-only
            # anyway.
            gdal_rasterize_env = {
                **get_gdal_env(),
                "GDAL_CACHEMAX": str(self.gdal_cachemax_mb),
            }
            rasterize_started = perf_counter()
            run_gdal_subcommand(cmd, env=gdal_rasterize_env)
            rasterize_seconds = perf_counter() - rasterize_started
        except GDALError:
            logger.error(f"Could not rasterize tile {self.tile_id}")
            raise
        else:
            self.set_local_dst(self.default_format)

            # invoking gdal-geotiff and compute stats here
            # instead of in a separate stage to assure we don't run out of memory
            # the transform stage uses all available memory for concurrent processes.
            # Having another stage which needs a lot of memory might cause the process to crash
            postprocess_started = perf_counter()
            self.postprocessing()
            postprocess_seconds = perf_counter() - postprocess_started

            logger.info(
                f"PERF vector_rasterize tile={self.tile_id} "
                f"features={self.feature_count} vertices={self.total_vertices} "
                f"complexity={self.complexity_score} "
                f"parquet_bytes={parquet_bytes} "
                f"rasterize_s={rasterize_seconds:.3f} "
                f"postprocess_s={postprocess_seconds:.3f} "
                f"total_s={perf_counter() - total_started:.3f}"
            )
