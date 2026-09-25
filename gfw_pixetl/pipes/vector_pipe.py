from typing import Iterator, List, Set, Tuple

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import VectorSrcLayer
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
        return (
            tiles
            | self.fetch_tile_data
            | Stage(self.rasterize).setup(workers=workers)
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
    def rasterize(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """Convert vector source to raster tiles."""
        for tile in tiles:
            if tile.status == "pending":
                tile.rasterize()
            yield tile
