from typing import Iterator, List, Optional, Set, Tuple

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.memory_admission import GIB, MEMORY_ADMISSION, AdmissionSharedState
from gfw_pixetl.parallelpipe import Pipeline, Stage, stage
from gfw_pixetl.pipes import Pipe
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.tiles import RasterSrcTile, Tile

LOGGER = get_module_logger(__name__)


class RasterPipe(Pipe):
    def get_grid_tiles(self) -> Set[RasterSrcTile]:  # type: ignore
        """Seed all available tiles within given grid.

        Use 1x1 degree tiles covering all land area as starting point.
        Then see in which target grid cell it would fall. Remove
        duplicated grid cells.
        """
        tiles: Set[RasterSrcTile] = set()
        for tile_id in self.grid.get_tile_ids():
            tiles.add(self._get_grid_tile(tile_id))

        tile_count: int = len(tiles)
        LOGGER.info(f"Found {tile_count} tile(s) inside grid")

        return tiles

    def _get_grid_tile(self, tile_id: str) -> RasterSrcTile:
        assert isinstance(self.layer, RasterSrcLayer)
        return RasterSrcTile(tile_id=tile_id, grid=self.grid, layer=self.layer)

    def _build_pipe(self, tiles: List[Tile], workers: int) -> Pipeline:
        """Construct the raster pipeline for a given tile list and worker
        count.

        ``workers`` controls the parallelism of the memory-intensive
        ``transform`` stage. Upload and cleanup use their own bounded
        worker counts so they do not each reserve a full
        ``num_processes`` pool.
        """
        # Configure the shared controller before ParallelPipe spawns the
        # transform workers.
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

        # Transform workers are started with the ``spawn`` start method, so
        # they do NOT inherit this configured, shared-memory-backed
        # controller merely by re-importing gfw_pixetl.memory_admission -
        # that re-import creates a brand new, unconfigured instance in the
        # fresh interpreter. Snapshot the now-configured shared primitives
        # here, in the parent, and pass the snapshot explicitly into
        # ``self.transform`` as an extra argument so each worker can rebind
        # its own local MEMORY_ADMISSION onto the real shared state before
        # it processes any tiles. See memory_admission.py's module
        # docstring for the full explanation.
        admission_state = MEMORY_ADMISSION.snapshot_shared_state()

        return (
            tiles
            | Stage(self.transform, admission_state).setup(workers=workers)
            | self.upload_file
            | self.delete_work_dir
        )

    def create_tiles(
        self, overwrite: bool
    ) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
        """Raster Pipe."""
        LOGGER.info("Start Raster Pipe")

        tiles = self.collect_tiles(overwrite=overwrite)

        # Use as many transform workers as there are tiles to process, capped
        # at the configured maximum. Memory admission controls how much of that
        # capacity may be active under pressure.
        workers = max(min(self.tiles_to_process, GLOBALS.workers), 1)
        result = self._process_pipe(self._build_pipe(tiles, workers))

        LOGGER.info("Finished Raster Pipe")
        return result

    @staticmethod
    @stage(workers=GLOBALS.num_processes)
    def filter_src_tiles(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """Only process tiles which intersect with source raster."""
        for tile in tiles:
            if tile.status == "pending" and not tile.within():
                LOGGER.info(
                    f"Tile {tile.tile_id} does not intersect with source raster - skip"
                )
                tile.status = "skipped (does not intersect)"
            yield tile

    # We cannot use the @stage decorator here
    # but need to create a Stage instance directly in the pipe.
    # When using the decorator, number of workers get set during RasterPipe class instantiation
    # and cannot be changed anymore. The Stage class gives us more flexibility.
    @staticmethod
    def transform(
        tiles: Iterator[RasterSrcTile],
        admission_state: Optional[AdmissionSharedState] = None,
    ) -> Iterator[RasterSrcTile]:
        """Transform input raster to match new tile grid and projection."""
        # This runs inside a freshly spawned worker process, where
        # MEMORY_ADMISSION (re-imported from scratch) is NOT the same
        # object as the parent's configured controller. Rebind it onto the
        # parent's real shared state before touching any admission-gated
        # code path. Must happen before the loop below, and only once per
        # worker process.
        if admission_state is not None:
            MEMORY_ADMISSION.bind_shared_state(admission_state)

        for tile in tiles:
            if tile.status == "pending":
                with MEMORY_ADMISSION.transform_slot(tile.tile_id):
                    if not tile.transform():
                        tile.status = "skipped (has no data)"
                        LOGGER.info(f"Tile {tile.tile_id} has no data - skip")
            yield tile
