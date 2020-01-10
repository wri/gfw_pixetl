from typing import Iterator, List

from parallelpipe import Stage

from gfw_pixetl import get_module_logger
from gfw_pixetl.tiles import VectorSrcTile, Tile
from gfw_pixetl.pipes import Pipe


LOGGER = get_module_logger(__name__)


class VectorPipe(Pipe):
    def create_tiles(self, overwrite=True) -> List[Tile]:
        """
        Vector Pipe
        """

        LOGGER.debug("Start Vector Pipe")

        pipe = (
            self.get_grid_tiles()
            | Stage(self.filter_subset_tiles).setup(workers=self.workers)
            | Stage(self.filter_src_tiles).setup(workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite).setup(
                workers=self.workers
            )
            | Stage(self.rasterize).setup(workers=self.workers, qsize=self.workers)
            | Stage(self.delete_if_empty).setup(workers=self.workers)
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

        LOGGER.debug("Start Finished Pipe")
        return tiles

    @staticmethod
    def filter_src_tiles(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """
        Only include tiles which intersect which input vector extent
        """
        for tile in tiles:
            if tile.src_vector_intersects():
                yield tile

    @staticmethod
    def rasterize(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """
        Convert vector source to raster tiles
        """
        for tile in tiles:
            tile.rasterize()
            yield tile
