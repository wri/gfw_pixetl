import multiprocessing
from typing import Iterator, List, Set, Tuple

from parallelpipe import Stage, stage

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.pipes import Pipe
from gfw_pixetl.tiles import RasterSrcTile, Tile

LOGGER = get_module_logger(__name__)
CORES = multiprocessing.cpu_count()


class RasterPipe(Pipe):
    def get_grid_tiles(self, min_x=-180, min_y=-90, max_x=180, max_y=90) -> Set[RasterSrcTile]:  # type: ignore
        """Seed all available tiles within given grid.

        Use 1x1 degree tiles covering all land area as starting point.
        Then see in which target grid cell it would fall. Remove
        duplicated grid cells.
        """

        assert isinstance(self.layer, RasterSrcLayer)
        LOGGER.debug("Get Grid Tiles")
        tiles: Set[RasterSrcTile] = set()

        for i in range(min_y + 1, max_y + 1):
            for j in range(min_x, max_x):
                origin = self.grid.xy_grid_origin(j, i)
                tiles.add(
                    RasterSrcTile(origin=origin, grid=self.grid, layer=self.layer)
                )

        tile_count = len(tiles)
        LOGGER.info(f"Found {tile_count} tile inside grid")
        # utils.set_workers(tile_count)

        return tiles

    def create_tiles(
        self, overwrite: bool
    ) -> Tuple[List[Tile], List[Tile], List[Tile]]:
        """Raster Pipe."""

        LOGGER.info("Start Raster Pipe")

        tiles = self.collect_tiles(overwrite=overwrite)

        workers = utils.set_workers(self.tiles_to_process)

        pipe = (
            tiles
            | Stage(self.transform).setup(workers=workers)
            | self.upload_file
            | self.delete_file
        )

        tiles, skipped_tiles, failed_tiles = self._process_pipe(pipe)

        LOGGER.info("Finished Raster Pipe")
        return tiles, skipped_tiles, failed_tiles

    @staticmethod
    @stage(workers=CORES)
    def filter_src_tiles(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """Only process tiles which intersect with source raster."""
        for tile in tiles:
            if tile.status == "pending" and not tile.within():
                LOGGER.info(
                    f"Tile {tile.tile_id} does not intersects with source raster - skip"
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
