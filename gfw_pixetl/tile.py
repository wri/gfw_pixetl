import subprocess as sp
import xml.etree.ElementTree as ET
from typing import List

import psycopg2
import rasterio
from shapely.geometry import Point

from gfw_pixetl import get_module_logger
from gfw_pixetl.grid import Grid
from gfw_pixetl.source import VectorSource, RasterSource


logger = get_module_logger(__name__)


class Tile(object):
    """
    A tile object which represents a single tile within a given grid
    """

    def __init__(
        self,
        minx: int,
        maxy: int,
        grid: Grid,
        uri: str,  # TODO figure out how to provide type hints for src
    ) -> None:
        self.minx: int = minx
        self.maxx: int = minx + grid.width
        self.maxy: int = maxy
        self.miny: int = maxy - grid.height
        self.tile_id: str = grid.pointGridId(Point(minx, maxy))
        self.grid = grid
        self.uri: str = uri.format(tile_id=self.tile_id)

    def uri_exists(self) -> bool:
        if not self.uri:
            raise Exception("Tile URI is not set")
        return self._tile_exists("/vsis3/" + self.uri)

    def is_empty(self) -> bool:
        return self._is_empty(self.uri)

    @staticmethod
    def _is_empty(f: str) -> bool:
        logger.debug("Check if tile {} is empty".format(f))
        with rasterio.open(f) as img:
            msk = img.read_masks(1).astype(bool)
        if msk[msk].size == 0:
            logger.debug("Tile {} is empty".format(f))
            return True
        else:
            logger.debug("Tile {} is not empty".format(f))
            return False

    @staticmethod
    def _tile_exists(uri: str) -> bool:

        logger.debug("Check if tile {} exists".format(uri))

        cmd = ["gdalinfo", uri]

        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        o, e = p.communicate()

        if p.returncode != 0 and e.decode("utf-8").split(" ")[1] != "13:":
            logger.exception(e)
            raise Exception(e)
        elif p.returncode != 0 and e.decode("utf-8").split(" ")[1] == "13:":
            logger.warning("Could not find tile file " + uri)
            return False
        else:
            logger.info("Found tile " + uri)
            return True


class VectorSrcTile(Tile):
    def __init__(
        self, minx: int, maxy: int, grid: Grid, src: VectorSource, uri: str
    ) -> None:

        self.src: VectorSource = src
        super().__init__(minx, maxy, grid, uri)

    def src_vector_intersects(self) -> bool:

        try:
            logger.debug("Check if tile intersects with postgis table")
            conn = psycopg2.connect(
                dbname=self.src.conn.db_name,
                user=self.src.conn.db_user,
                password=self.src.conn.db_password,
                host=self.src.conn.db_host,
                port=self.src.conn.db_port,
            )
            cursor = conn.cursor()
            exists_query = "SELECT exists (SELECT 1 FROM {name}__{grid} WHERE tile_id__{grid} = '{tile_id}' LIMIT 1)".format(
                name=self.src.table_name, grid=self.grid.name, tile_id=self.tile_id
            )
            cursor.execute(exists_query)
            exists = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        except psycopg2.Error:
            logger.exception(
                "There was an issue when trying to connect to the database"
            )
            raise

        if exists:
            logger.info(
                "Tile id {} exists in database table {}".format(
                    self.tile_id, self.src.table_name
                )
            )
        else:
            logger.info(
                "Tile id {} does not exists in database table {}".format(
                    self.tile_id, self.src.table_name
                )
            )
        return exists


class RasterSrcTile(Tile):
    def __init__(
        self, minx: int, maxy: int, grid: Grid, src: RasterSource, uri: str
    ) -> None:

        super().__init__(minx, maxy, grid, uri)

        self.calc_uri: str = uri.format(tile_id=self.tile_id + "__calc")

        self.src: RasterSource = src

        if src.type == "single_tile":
            self.src_uri: str = "/vsis3/" + src.uri
        else:
            self.src_uri: str = "/vsis3/" + src.uri.format(tile_id=self.tile_id)

    def src_tile_exists(self) -> bool:

        if not self.src_uri:
            raise ValueError("Tile source URI needs to be set")
        return self._tile_exists(self.src_uri)

    def src_tile_intersects(self) -> bool:

        if not self.uri or not self.src_uri:
            raise ValueError("Tile URI and Tile source URI need to be set")

        intersects = False
        for x in [self.minx, self.maxx]:
            for y in [self.miny, self.maxy]:
                logger.debug("Check if tile intersects with single tile")
                cmd: List[str] = [
                    "gdallocationinfo",
                    "-xml",
                    "-wgs84",
                    self.src_uri,
                    str(x),
                    str(y),
                ]
                p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
                o, e = p.communicate()
                if p.returncode == 0 and ET.fromstring(o)[0].tag == "BandReport":
                    intersects = True
        return intersects

    def calc_is_empty(self) -> bool:
        return self._is_empty(self.calc_uri)
