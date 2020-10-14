import itertools
from multiprocessing import Pool, cpu_count
from multiprocessing.pool import Pool as PoolType
from typing import Iterable, Iterator, List, Set, Tuple

from parallelpipe import stage
from shapely.geometry import Point

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.layers import VectorSrcLayer
from gfw_pixetl.pipes import Pipe
from gfw_pixetl.tiles import Tile, VectorSrcTile

LOGGER = get_module_logger(__name__)
WORKERS = utils.get_workers()
CORES = cpu_count()


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
            # | self.create_gdal_geotiff
            | self.upload_file
            | self.delete_file
        )

        return self._process_pipe(pipe)

    def get_grid_tiles(self, min_x=-180, min_y=-90, max_x=180, max_y=90) -> Set[VectorSrcTile]:  # type: ignore
        """Seed all available tiles within given grid.

        Use 1x1 degree tiles covering all land area as starting point.
        Then see in which target grid cell it would fall. Remove
        duplicated grid cells.
        """

        x: Iterable[int] = range(min_x, max_x)
        y: Iterable[int] = range(min_y + 1, max_y + 1)

        x_y: List[Tuple[int, int]] = list(itertools.product(x, y))
        pool: PoolType = Pool(processes=CORES)
        tiles: Set[VectorSrcTile] = set(pool.map(self._get_grid_tile, x_y))

        tile_count: int = len(tiles)
        LOGGER.info(f"Found {tile_count} tile inside grid")

        return tiles

    def _get_grid_tile(self, x_y: Tuple[int, int]) -> VectorSrcTile:
        assert isinstance(self.layer, VectorSrcLayer)
        x: int = x_y[0]
        y: int = x_y[1]
        origin: Point = self.grid.xy_grid_origin(x, y)
        return VectorSrcTile(origin=origin, grid=self.grid, layer=self.layer)

    @staticmethod
    @stage(workers=WORKERS)
    def filter_src_tiles(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """Only include tiles which intersect which input vector extent."""
        for tile in tiles:
            if tile.status == "pending" and not tile.src_vector_intersects():
                tile.status = "skipped (does not intersect)"
            yield tile

    @staticmethod
    @stage(workers=WORKERS)
    def rasterize(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """Convert vector source to raster tiles."""
        for tile in tiles:
            if tile.status == "pending":
                tile.rasterize()
            yield tile
