import logging
import subprocess as sp
from typing import Iterator, List

from parallelpipe import stage

from gfw_tile_prep.data_type import DataType
from gfw_tile_prep.grid import Grid
from gfw_tile_prep.tile import Tile
from gfw_tile_prep.layer import Layer
from gfw_tile_prep.source import RasterSource

logger = logging.getLogger(__name__)


class RasterLayer(Layer):
    type = "raster"

    def __init__(
        self,
        name: str,
        version: str,
        field: str,
        grid: Grid,
        data_type: DataType,
        src_path: str,
        resampling: str = "nearest",
        single_tile: bool = False,
    ):

        self.resampling = resampling

        if single_tile:
            src_type = "single_tile"
        else:
            src_type = "tiled"
        self.src = RasterSource(src_path, src_type)

        super().__init__(name, version, field, grid, data_type, self.src)

    def create_tiles(self, overwrite=True) -> None:

        pipe = (
            self.get_grid_tiles()
            | self.filter_src_tiles()
            | self.filter_target_tiles(overwrite)
            | self.translate()
            | self.delete_if_empty()
            | self.upload_file()
            | self.delete_file()
        )

        for output in pipe.results():
            pass

    @stage
    def filter_src_tiles(self, tiles: Iterator[Tile]) -> Iterator[Tile]:
        for tile in tiles:
            if self.src.type == "tiled" and tile.src_tile_exists():
                yield tile
            elif self.src.type == "single_tile" and tile.src_tile_intersects():
                yield tile

    @stage
    def translate(self, tiles: Iterator[Tile]) -> Iterator[Tile]:

        if self.data_type.no_data:
            cmd_no_data: List[str] = ["-a_nodata", str(self.data_type.no_data)]
        else:
            cmd_no_data = list()

        for tile in tiles:

            cmd: List[str] = (
                ["gdal_translate", "-strict", "-ot", self.data_type.data_type]
                + cmd_no_data
                + [
                    "-tr",
                    str(self.grid.xres),
                    str(self.grid.yres),
                    "-projwin",
                    str(tile.minx),
                    str(tile.maxy),
                    str(tile.maxx),
                    str(tile.miny),
                    "-co",
                    "COMPRESS={}".format(self.data_type.compression),
                    "-co",
                    "TILED=YES",
                    "-co",
                    "BLOCKXSIZE={}".format(self.grid.blockxsize),
                    "-co",
                    "BLOCKYSIZE={}".format(self.grid.blockysize),
                    # "-co", "SPARSE_OK=TRUE",
                    "-r",
                    self.resampling,
                    tile.src.uri,
                    tile.uri,
                ]
            )

            try:
                logger.info("Translate tile " + tile.tile_id)
                sp.check_call(cmd)
            except sp.CalledProcessError as e:
                logger.warning("Could not translate file " + tile.uri)
                logger.warning(e)
            else:
                yield tile
