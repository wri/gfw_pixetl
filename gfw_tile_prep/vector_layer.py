import logging
import subprocess as sp
from typing import Iterator, List

from parallelpipe import Stage

from gfw_tile_prep import get_module_logger
from gfw_tile_prep.data_type import DataType
from gfw_tile_prep.grid import Grid
from gfw_tile_prep.layer import Layer
from gfw_tile_prep.tile import Tile
from gfw_tile_prep.source import VectorSource

logger = get_module_logger(__name__)


class VectorLayer(Layer):

    type = "vector"

    def __init__(
        self,
        name: str,
        version: str,
        field: str,
        grid: Grid,
        data_type: DataType,
        order: str = "asc",
        rasterize_method: str = "value",
        env: str = "dev",
    ):
        logger.debug("Initializing Vector layer")
        self.field: str = field
        self.order: str = order
        self.rasterize_method = rasterize_method
        self.src = VectorSource("{}_{}".format(name, version))

        super().__init__(name, version, field, grid, data_type, self.src, env)
        logger.debug("Initialized Vector layer")

    def create_tiles(self, overwrite=True) -> None:

        logging.debug("Start Vector Pipe")

        pipe = (
            self.get_grid_tiles()
            | Stage(self.filter_src_tiles)
            | Stage(self.filter_target_tiles, overwrite=overwrite)
            | Stage(self.rasterize)
            | Stage(self.delete_if_empty)
            | Stage(self.upload_file)
            | Stage(self.delete_file)
        )

        for output in pipe.results():
            pass

        logger.debug("Start Finished Pipe")

    @staticmethod
    def filter_src_tiles(tiles: Iterator[Tile]) -> Iterator[Tile]:
        for tile in tiles:
            if tile.src_vector_intersects():
                yield tile

    def rasterize(self, tiles: Iterator[Tile]) -> Iterator[Tile]:

        if self.rasterize_method == "count":
            cmd_method: List[str] = ["-burn", "1", "-add"]
        else:
            cmd_method = ["-a", self.field]

        if self.data_type.no_data:
            cmd_no_data: List[str] = ["-a_nodata", str(self.data_type.no_data)]
        else:
            cmd_no_data = list()

        for tile in tiles:

            logger.info("Create raster " + tile.uri)

            cmd: List[str] = (
                ["gdal_rasterize"]
                + cmd_method
                + [
                    "-sql",
                    "select * from {name}_{version} where tile_id__{grid} = {tile_id}".format(
                        name=self.name,
                        version=self.version,
                        grid=self.grid.name,
                        tile_id=tile.tile_id,
                    ),
                    "-te",
                    str(tile.minx),
                    str(tile.miny),
                    str(tile.maxx),
                    str(tile.maxy),
                    "-tr",
                    str(self.grid.xres),
                    str(self.grid.yres),
                    "-a_srs",
                    "EPSG:4326",
                    "-ot",
                    self.data_type.data_type,
                ]
                + cmd_no_data
                + [
                    "-co",
                    "COMPRESS={}".format(self.data_type.compression),
                    "-co",
                    "TILED=YES",
                    "-co",
                    "BLOCKXSIZE={}".format(self.grid.blockxsize),
                    "-co",
                    "BLOCKYSIZE={}".format(self.grid.blockxsize),
                    # "-co", "SPARSE_OK=TRUE",
                    self.src.pg_conn,
                    tile.uri,
                ]
            )
            try:
                logger.info("Rasterize tile " + tile.tile_id)
                sp.check_call(cmd)
            except sp.CalledProcessError as e:
                logger.warning("Could not rasterize file " + tile.uri)
                logger.warning(e)
            else:
                yield tile
