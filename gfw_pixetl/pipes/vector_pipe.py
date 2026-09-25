from typing import Iterator, List, Optional, Set, Tuple

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import VectorSrcLayer
from gfw_pixetl.memory_admission import GIB, MEMORY_ADMISSION, AdmissionSharedState
from gfw_pixetl.parallelpipe import Pipeline, Stage, stage
from gfw_pixetl.pipes import Pipe
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.tiles import Tile, VectorSrcTile

LOGGER = get_module_logger(__name__)


class VectorPipe(Pipe):
    def create_tiles(
        self, overwrite
    ) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
        """Vector Pipe."""
        LOGGER.debug("Start Vector Pipe")
        tiles = self.collect_tiles(overwrite=overwrite)

        workers = max(min(self.tiles_to_process, GLOBALS.workers), 1)
        result = self._process_pipe(self._build_pipe(tiles, workers))

        LOGGER.debug("Finished Vector Pipe")
        return result

    def _build_pipe(self, tiles: List[Tile], workers: int) -> Pipeline:
        """Construct the vector pipeline for a given tile list and worker
        count.

        ``tiles`` have already been classified by ``collect_tiles()``:
        filter_subset_tiles, filter_target_tiles, and filter_src_tiles all
        ran there (see Pipe.collect_tiles). This pipe must NOT repeat those
        stages -- filter_src_tiles in particular makes one DB round trip per
        tile ("Limited to be nice to DB"), and re-running it here used to
        silently double the number of queries hitting the source database on
        every single run, for tiles whose status was already decided.
        RasterPipe's _build_pipe does not repeat its filters either; this
        now matches that pattern.

        ``workers`` controls the parallelism of the memory-intensive
        ``rasterize`` stage.
        """
        # Configure the shared controller before ParallelPipe spawns the
        # rasterize workers -- same call RasterPipe makes before its
        # transform workers start, and the same GLOBALS.memory_admission_*
        # settings, since the mechanism (reserve memory for a per-tile
        # step, throttle new admissions under cgroup pressure) is generic.
        MEMORY_ADMISSION.configure(
            enabled=GLOBALS.memory_admission_enabled,
            high_watermark=GLOBALS.memory_admission_high_watermark,
            resume_watermark=GLOBALS.memory_admission_resume_watermark,
            stats_workers=GLOBALS.memory_admission_stats_workers,
            reservation_bytes=int(GLOBALS.memory_admission_reservation_gib * GIB),
            window_reservation_bytes=int(
                GLOBALS.memory_admission_window_reservation_gib * GIB
            ),
            poll_seconds=GLOBALS.memory_admission_poll_seconds,
        )

        # Spawned workers do not inherit the configured module singleton, so
        # pass its shared state explicitly and bind it in each rasterize
        # worker.
        admission_state = MEMORY_ADMISSION.snapshot_shared_state()

        return (
            tiles
            | self.fetch_tile_data
            | Stage(self.rasterize, admission_state).setup(workers=workers)
            | self.upload_file
            | self.delete_work_dir
        )

    def get_grid_tiles(self) -> Set[VectorSrcTile]:  # type: ignore
        """Seed all available tiles within given grid.

        Use 1x1 degree tiles covering all land area as starting point.
        Then see in which target grid cell it would fall. Remove
        duplicated grid cells.
        """
        tiles: Set[VectorSrcTile] = set()
        for tile_id in self.grid.get_tile_ids():
            tiles.add(self._get_grid_tile(tile_id))

        tile_count: int = len(tiles)
        LOGGER.info(f"Found {tile_count} tiles inside grid")

        return tiles

    def _get_grid_tile(self, tile_id: str) -> VectorSrcTile:
        assert isinstance(self.layer, VectorSrcLayer)
        return VectorSrcTile(tile_id=tile_id, grid=self.grid, layer=self.layer)

    @staticmethod
    @stage(workers=GLOBALS.db_fetch_workers)  # Budget for the source DB, see GLOBALS.db_fetch_workers
    def filter_src_tiles(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """Only include tiles which intersect input vector extent."""
        for tile in tiles:
            if tile.status == "pending" and not tile.src_vector_intersects():
                tile.status = "skipped (does not intersect)"
            yield tile

    @staticmethod
    @stage(workers=GLOBALS.db_fetch_workers)  # Budget for the source DB, see GLOBALS.db_fetch_workers
    def fetch_tile_data(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """Download vector data from the database."""
        for tile in tiles:
            if tile.status == "pending":
                tile.fetch_data()
            yield tile

    @staticmethod
    def rasterize(
        tiles: Iterator[VectorSrcTile],
        admission_state: Optional[AdmissionSharedState] = None,
    ) -> Iterator[VectorSrcTile]:
        """Convert vector source to raster tiles.

        Gated by MEMORY_ADMISSION the same way RasterPipe.transform() is:
        a spike in cgroup memory (concurrent gdal_rasterize calls at high
        resolution, say) throttles new admissions here instead of running
        every configured worker regardless of actual headroom. Unlike
        transform(), there's no windowed sub-step to commit a reservation
        early for -- rasterize() is one bounded gdal_rasterize subprocess
        call per tile, so the whole call holds its reservation and
        tile_slot() releases it on exit.
        """
        # Bind the worker-local controller before any admission-gated work.
        if admission_state is not None:
            MEMORY_ADMISSION.bind_shared_state(admission_state)

        for tile in tiles:
            if tile.status == "pending":
                with MEMORY_ADMISSION.tile_slot(tile.tile_id):
                    tile.rasterize()
            yield tile
