from typing import Iterator, List, Set

from parallelpipe import Stage

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.tiles import RasterSrcTile, Tile
from gfw_pixetl.pipes import Pipe


LOGGER = get_module_logger(__name__)


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
            | Stage(self.filter_subset_tiles).setup(workers=self.workers)
            | Stage(self.filter_src_tiles).setup(workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite).setup(
                workers=self.workers
            )
            | Stage(self.transform).setup(
                workers=1, qsize=self.workers
            )  # We process blocks in parallel, not tiles
            | Stage(self.upload_file).setup(workers=self.workers)
            | Stage(self.delete_file).setup(workers=self.workers)
        )

        tile_uris: List[str] = list()
        tiles: List[Tile] = list()
        for tile in pipe.results():
            tiles.append(tile)
            tile_uris.append(tile.dst.uri)

        if len(tiles):
            self.upload_vrt(tile_uris)
            self.upload_extent(tiles)

        LOGGER.info("Finished Raster Pipe")
        return tiles

    @staticmethod
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
