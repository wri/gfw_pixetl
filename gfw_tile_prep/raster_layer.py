import logging
import subprocess as sp
from typing import Iterator, List, Optional

from parallelpipe import stage

from gfw_tile_prep.grid import Grid
from gfw_tile_prep.layer import Layer
from gfw_tile_prep.tile import Tile


logger = logging.getLogger(__name__)


class RasterLayer(Layer):
    type = "raster"

    def __init__(
        self,
        name: str,
        version: str,
        value: str,
        src_path: str,
        grid: Grid,
        data_type: str,
        no_data: int = 0,
        nbits: Optional[int] = None,
        single_tile: bool = False,
        resampling: str = "nearest",
    ):
        self.src = src_path
        self.resampling = resampling

        if single_tile:
            src_type = "single_tile"
        else:
            src_type = "tiled"
        super().__init__(
            name, version, value, src_type, src_path, grid, data_type, no_data, nbits
        )

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
            if self.src_type == "tiled" and tile.src_tile_exists():
                yield tile
            elif self.src_type == "single_tile" and tile.src_tile_intersects():
                yield tile

    @stage
    def translate(self, tiles: Iterator[Tile]) -> Iterator[Tile]:

        for tile in tiles:

            cmd: List[str] = [
                "gdal_translate",
                "-strict",
                "-ot",
                self.data_type.data_type,
                "-a_nodata",
                str(self.data_type.no_data),
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
                tile.src_uri,
                tile.uri,
            ]

            try:
                logger.info("Translate tile " + tile.tile_id)
                sp.check_call(cmd)
            except sp.CalledProcessError as e:
                logger.warning("Could not translate file " + tile.uri)
                logger.warning(e)
            else:
                yield tile
