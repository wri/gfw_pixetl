import math
import multiprocessing
from typing import Iterator, List

from parallelpipe import Stage

from gfw_pixetl import get_module_logger
from gfw_pixetl.tiles import RasterSrcTile, Tile
from gfw_pixetl.pipes import RasterPipe


LOGGER = get_module_logger(__name__)


class CalcRasterPipe(RasterPipe):

    # We want fewer workers b/c we have to expensive stages
    # transform and value update
    workers: int = math.ceil(multiprocessing.cpu_count() / 3)

    def create_tiles(self, overwrite=True) -> List[Tile]:
        """
        Calc Raster Pipe
        """
        LOGGER.debug("Start Calc Raster Pipe")

        pipe = (
            self.get_grid_tiles()
            | Stage(self.filter_subset_tiles).setup(workers=self.workers)
            | Stage(self.filter_src_tiles).setup(workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite).setup(
                workers=self.workers
            )
            | Stage(self.transform).setup(workers=self.workers, qsize=self.workers)
            | Stage(self.delete_if_empty).setup(workers=self.workers)
            | Stage(self.calculate).setup(workers=self.workers, qsize=self.workers)
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

        LOGGER.debug("Finished Raster Pipe")
        return tiles

    @staticmethod
    def transform(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """
        Transform input raster to match new tile grid and projection.
        Make sure is_final is set to False.
        We will handel data types, no data values and compression in a later stage.
        """
        for tile in tiles:
            tile.transform(is_final=False)
            yield tile

    @staticmethod
    def calculate(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """
        Update pixel values
        """

        for tile in tiles:
            LOGGER.info(f"Calculate tile {tile.tile_id}")
            try:
                tile.update_values()
            except Exception:
                LOGGER.exception("Calculation failed")
                raise
            else:
                yield tile
