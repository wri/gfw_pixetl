from typing import Iterator, List, Tuple

from parallelpipe import stage

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.tiles import VectorSrcTile, Tile
from gfw_pixetl.pipes import Pipe

LOGGER = get_module_logger(__name__)
WORKERS = utils.get_workers()


class VectorPipe(Pipe):
    def create_tiles(self, overwrite) -> Tuple[List[Tile], List[Tile], List[Tile]]:
        """
        Vector Pipe
        """

        LOGGER.debug("Start Vector Pipe")
        tiles = self.get_grid_tiles
        pipe = (
            tiles
            | self.filter_subset_tiles
            | self.filter_src_tiles
            | self.filter_target_tiles(overwrite=overwrite)
            | self.rasterize
            | self.create_gdal_geotiff
            | self.upload_file
            | self.delete_file
        )

        return self._process_pipe(pipe)

    @staticmethod
    @stage(workers=WORKERS)
    def filter_src_tiles(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """
        Only include tiles which intersect which input vector extent
        """
        for tile in tiles:
            if tile.status == "pending" and not tile.src_vector_intersects():
                tile.status = "skipped (does not intersect)"
            yield tile

    @staticmethod
    @stage(workers=WORKERS)
    def rasterize(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """
        Convert vector source to raster tiles
        """
        for tile in tiles:
            if tile.status == "pending":
                tile.rasterize()
            yield tile
