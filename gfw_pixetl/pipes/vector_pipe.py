from typing import Iterator, List, Set, Tuple

from parallelpipe import stage

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import VectorSrcLayer
from gfw_pixetl.pipes import Pipe
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.tiles import Tile, VectorSrcTile

LOGGER = get_module_logger(__name__)


class VectorPipe(Pipe):
    def create_tiles(self, overwrite) -> Tuple[List[Tile], List[Tile], List[Tile]]:
        """Vector Pipe."""

        LOGGER.debug("Start Vector Pipe")
        tiles = self.collect_tiles(overwrite=overwrite)
        pipe = (
            tiles
            | self.filter_subset_tiles
            | self.filter_src_tiles
            | self.filter_target_tiles(overwrite=overwrite)
            | self.rasterize
            | self.upload_file
            | self.delete_work_dir
        )

        return self._process_pipe(pipe)

    def get_grid_tiles(self) -> Set[VectorSrcTile]:  # type: ignore
        """Seed all available tiles within given grid.

        Use 1x1 degree tiles covering all land area as starting point.
        Then see in which target grid cell it would fall. Remove
        duplicated grid cells.
        """

        tiles: Set[VectorSrcTile] = set()
        for tile_id in self.grid.get_tile_ids():
            tiles.add(self._get_grid_tile(tile_id))

        # tile_ids = self.grid.get_tile_ids()
        # with get_context("spawn").Pool(processes=GLOBALS.cores) as pool:
        #     tiles: Set[VectorSrcTile] = set(pool.map(self._get_grid_tile, tile_ids))

        tile_count: int = len(tiles)
        LOGGER.info(f"Found {tile_count} tile inside grid")

        return tiles

    def _get_grid_tile(self, tile_id: str) -> VectorSrcTile:
        assert isinstance(self.layer, VectorSrcLayer)
        return VectorSrcTile(tile_id=tile_id, grid=self.grid, layer=self.layer)

    @staticmethod
    @stage(workers=GLOBALS.workers)
    def filter_src_tiles(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """Only include tiles which intersect which input vector extent."""
        for tile in tiles:
            if tile.status == "pending" and not tile.src_vector_intersects():
                tile.status = "skipped (does not intersect)"
            yield tile

    @staticmethod
    @stage(workers=GLOBALS.workers)
    def rasterize(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """Convert vector source to raster tiles."""
        for tile in tiles:
            if tile.status == "pending":
                tile.rasterize()
            yield tile
