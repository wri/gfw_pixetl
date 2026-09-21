from typing import Iterator, List, Set, Tuple

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.memory_admission import GIB, MEMORY_ADMISSION
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
        # Configure/reset the shared controller before ParallelPipe forks this
        # attempt's workers. This also clears stale reservations after an OOM
        # retry where a killed worker could not run its ``finally`` block.
        MEMORY_ADMISSION.configure(
            enabled=GLOBALS.memory_admission_enabled,
            high_watermark=GLOBALS.memory_admission_high_watermark,
            resume_watermark=GLOBALS.memory_admission_resume_watermark,
            critical_watermark=GLOBALS.memory_admission_critical_watermark,
            critical_resume_watermark=(
                GLOBALS.memory_admission_critical_resume_watermark
            ),
            reservation_bytes=int(GLOBALS.memory_admission_reservation_gib * GIB),
            poll_seconds=GLOBALS.memory_admission_poll_seconds,
        )

        return (
            tiles
            | Stage(self.transform).setup(workers=workers)
            | self.upload_file
            | self.delete_work_dir
        )

    def create_tiles(
        self, overwrite: bool
    ) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
        """Raster Pipe."""
        LOGGER.info("Start Raster Pipe")

        tiles = self.collect_tiles(overwrite=overwrite)

        # Start with as many workers as there are tiles to process, capped at
        # GLOBALS.workers.  The retry logic will halve this on each OOM kill.
        initial_workers = max(min(self.tiles_to_process, GLOBALS.workers), 1)
        GLOBALS.workers = initial_workers

        result = self._process_pipe_with_oom_retry(
            tiles=tiles,
            workers=initial_workers,
            build_pipe=self._build_pipe,
        )

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
    def transform(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """Transform input raster to match new tile grid and projection."""
        for tile in tiles:
            if tile.status == "pending":
                with MEMORY_ADMISSION.transform_slot(tile.tile_id):
                    if not tile.transform():
                        tile.status = "skipped (has no data)"
                        LOGGER.info(f"Tile {tile.tile_id} has no data - skip")
            yield tile
