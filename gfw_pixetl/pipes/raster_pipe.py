from typing import Iterator, List, Set, Tuple

from parallelpipe import Stage, stage

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import RasterSrcLayer
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

        # tile_ids = self.grid.get_tile_ids()
        #
        # with get_context("spawn").Pool(processes=GLOBALS.num_processes) as pool:
        #     tiles: Set[RasterSrcTile] = set(pool.map(self._get_grid_tile, tile_ids))

        tile_count: int = len(tiles)
        LOGGER.info(f"Found {tile_count} tile(s) inside grid")

        return tiles

    def _get_grid_tile(self, tile_id: str) -> RasterSrcTile:
        assert isinstance(self.layer, RasterSrcLayer)
        return RasterSrcTile(tile_id=tile_id, grid=self.grid, layer=self.layer)

    def create_tiles(
        self, overwrite: bool
    ) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
        """Raster Pipe."""

        LOGGER.info("Start Raster Pipe")

        tiles = self.collect_tiles(overwrite=overwrite)

        GLOBALS.workers = max(self.tiles_to_process, 1)

        pipe = (
            tiles
            | Stage(self.transform).setup(workers=GLOBALS.workers)
            | self.upload_file
            | self.delete_work_dir
        )

        tiles, skipped_tiles, failed_tiles, existing_tiles = self._process_pipe(pipe)

        LOGGER.info("Finished Raster Pipe")
        return tiles, skipped_tiles, failed_tiles, existing_tiles

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

    # We cannot use the @stage decorate here
    # but need to create a Stage instance directly in the pipe.
    # When using the decorator, number of workers get set during RasterPipe class instantiation
    # and cannot be changed afterwards anymore. The Stage class gives us more flexibility.
    @staticmethod
    def transform(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """Transform input raster to match new tile grid and projection."""
        for tile in tiles:
            if tile.status == "pending" and not tile.transform():
                tile.status = "skipped (has no data)"
                LOGGER.info(f"Tile {tile.tile_id} has no data - skip")
            yield tile
