from typing import Iterator, List, Set

from parallelpipe import stage

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.tiles import RasterSrcTile, Tile
from gfw_pixetl.pipes import Pipe

LOGGER = get_module_logger(__name__)
WORKERS = utils.get_workers()


class RasterPipe(Pipe):
    def get_grid_tiles(self) -> Set[Tile]:
        """
        Seed all available tiles within given grid.
        Use 1x1 degree tiles covering all land area as starting point.
        Then see in which target grid cell it would fall.
        Remove duplicated grid cells.
        """

        assert isinstance(self.layer, RasterSrcLayer)
        LOGGER.debug("Get grid Tiles")
        tiles: Set[Tile] = set()

        for i in range(-89, 91):
            for j in range(-180, 180):
                origin = self.grid.xy_grid_origin(j, i)
                tiles.add(
                    RasterSrcTile(origin=origin, grid=self.grid, layer=self.layer)
                )

        LOGGER.info(f"Found {len(tiles)} tile inside grid")
        # logger.debug(tiles)

        return tiles

    def create_tiles(self, overwrite=True) -> List[Tile]:
        """
        Raster Pipe
        """

        LOGGER.info("Start Raster Pipe")

        pipe = (
            self.get_grid_tiles()
            | self.filter_subset_tiles()
            | self.filter_src_tiles()
            | self.filter_target_tiles(overwrite=overwrite)
            | self.transform()
            | self.upload_file()
            | self.delete_file()
        )

        tiles = self.process_pipe(pipe)

        LOGGER.info("Finished Raster Pipe")
        return tiles

    @staticmethod
    @stage(workers=WORKERS)
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

    @staticmethod
    @stage(workers=WORKERS)
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
