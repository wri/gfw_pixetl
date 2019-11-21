from typing import Iterator, List

from parallelpipe import Stage

from gfw_pixetl import get_module_logger
from gfw_pixetl.tiles import RasterSrcTile
from gfw_pixetl.pipes import Pipe


logger = get_module_logger(__name__)


class RasterPipe(Pipe):
    def create_tiles(self, overwrite=True) -> None:
        """
        Raster Pipe
        """

        logger.debug("Start Raster Pipe")

        pipe = (
            self.get_grid_tiles()
            | Stage(self.filter_subset_tiles).setup(workers=self.workers)
            | Stage(self.filter_src_tiles).setup(workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite).setup(
                workers=self.workers
            )
            | Stage(self.transform).setup(workers=self.workers, qsize=self.workers)
            | Stage(self.delete_if_empty).setup(workers=self.workers)
            | Stage(self.compress).setup(workers=self.workers)
            | Stage(self.upload_file).setup(workers=self.workers)
            | Stage(self.delete_file).setup(workers=self.workers)
        )

        tile_uris: List[str] = list()
        for tile in pipe.results():
            tile_uris.append(tile.uri)

        # vrt: str = self.create_vrt(tile_uris)
        # TODO upload vrt to s3

        logger.debug("Finished Raster Pipe")

    @staticmethod
    def filter_src_tiles(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """
        Only process tiles which intersect with source raster
        """
        for tile in tiles:
            if tile.src_tile_intersects():
                yield tile

    @staticmethod
    def transform(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """
        Transform input raster to match new tile grid and projection
        """
        for tile in tiles:
            tile.transform()
            yield tile

    @staticmethod
    def compress(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """
        Compress tiles
        """
        for tile in tiles:
            tile.compress()
            yield tile
