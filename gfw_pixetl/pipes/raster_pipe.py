import multiprocessing
from math import ceil
from typing import Iterator, List, Set

from parallelpipe import Stage, stage

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.tiles import RasterSrcTile, Tile
from gfw_pixetl.pipes import Pipe

LOGGER = get_module_logger(__name__)
CORES = multiprocessing.cpu_count()


class RasterPipe(Pipe):
    def get_grid_tiles(self) -> Set[RasterSrcTile]:  # type: ignore
        """
        Seed all available tiles within given grid.
        Use 1x1 degree tiles covering all land area as starting point.
        Then see in which target grid cell it would fall.
        Remove duplicated grid cells.
        """

        assert isinstance(self.layer, RasterSrcLayer)
        LOGGER.debug("Get Grid Tiles")
        tiles: Set[RasterSrcTile] = set()

        for i in range(-89, 91):
            for j in range(-180, 180):
                origin = self.grid.xy_grid_origin(j, i)
                tiles.add(
                    RasterSrcTile(origin=origin, grid=self.grid, layer=self.layer)
                )

        tile_count = len(tiles)
        LOGGER.info(f"Found {tile_count} tile inside grid")
        # utils.set_workers(tile_count)

        return tiles

    def create_tiles(self, overwrite=True) -> List[Tile]:
        """
        Raster Pipe
        """

        LOGGER.info("Start Raster Pipe")

        tiles = self.collect_tiles()
        workers = utils.set_workers(len(tiles))

        pipe = (
            tiles
            | Stage(self.transform).setup(workers=workers, qsize=workers)
            | self.upload_file
            | self.delete_file
        )

        tiles = self._process_pipe(pipe)

        LOGGER.info("Finished Raster Pipe")
        return tiles

    @staticmethod
    @stage(workers=ceil(CORES / 4), qsize=ceil(CORES / 4))
    def filter_src_tiles(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """
        Only process tiles which intersect with source raster
        """
        for tile in tiles:
            if tile.src_tile_intersects():
                LOGGER.info(
                    f"Tile {tile.tile_id} intersects with source raster - proceed"
                )
                yield tile
            else:
                LOGGER.info(
                    f"Tile {tile.tile_id} does not intersects with source raster - skip"
                )

    # We cannot use the @stage decorate here
    # but need to create a Stage instance directly in the pipe.
    # When using the decorator, number of workers get set during RasterPipe class instantiation
    # and cannot be changed afterwards anymore. The Stage class gives us more flexibility.
    @staticmethod
    def transform(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """
        Transform input raster to match new tile grid and projection
        """
        for tile in tiles:
            if tile.transform():
                LOGGER.info(f"Tile {tile.tile_id} has data - proceed")
                yield tile
            else:
                LOGGER.info(f"Tile {tile.tile_id} has no data - skip")
