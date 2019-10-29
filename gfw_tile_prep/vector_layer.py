import logging
import subprocess as sp
from typing import Iterator, List, Optional

from parallelpipe import stage

from gfw_tile_prep.grid import Grid
from gfw_tile_prep.layer import Layer
from gfw_tile_prep.tile import Tile

logger = logging.getLogger(__name__)


class VectorLayer(Layer):

    type = "vector"
    db_host = "localhost"
    db_port = 5432
    db_name = "gadm"
    db_user = "postgres"
    db_password = "postgres"  # TODO: make a secret call
    pg_conn = "PG:dbname={} port={} host={} user={} password={}".format(
        db_name, db_port, db_host, db_user, db_password
    )

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
        oid: str = "val",
        order: str = "asc",
        rasterize_method: str = "oid",
    ):

        self.oid: str = oid
        self.order: str = order
        self.rasterize_method = rasterize_method
        super().__init__(
            name, version, value, self.type, src_path, grid, data_type, no_data, nbits
        )

    def create_tiles(self) -> None:

        pipe = (
            self.get_grid_tiles()
            | self.filter_src_tiles()
            | self.filter_target_tiles()
            | self.rasterize()
            | self.delete_if_empty()
            | self.upload_file()
            | self.delete_file()
        )

        for output in pipe.results():
            pass

    @staticmethod
    @stage
    def filter_src_tiles(tiles: Iterator[Tile]) -> Iterator[Tile]:
        for tile in tiles:
            if tile.src_vector_intersects():
                yield tile

    @stage
    def rasterize(self, tiles: Iterator[Tile]) -> Iterator[Tile]:

        if self.rasterize_method == "count":
            cmd_method: List[str] = ["-burn", "1", "-add"]
        else:
            cmd_method = ["-a", self.oid]

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
                    "-a_nodata",
                    str(self.data_type.no_data),
                    "-co",
                    "COMPRESS={}".format(self.data_type.compression),
                    "-co",
                    "TILED=YES",
                    "-co",
                    "BLOCKXSIZE={}".format(self.grid.blockxsize),
                    "-co",
                    "BLOCKYSIZE={}".format(self.grid.blockxsize),
                    # "-co", "SPARSE_OK=TRUE",
                    self.pg_conn,
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
