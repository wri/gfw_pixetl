import logging
import subprocess as sp
import xml.etree.ElementTree as ET
from typing import List

import psycopg2
import rasterio
from shapely.geometry import Point

from gfw_tile_prep.layer import Layer
from gfw_tile_prep.raster_layer import RasterLayer
from gfw_tile_prep.vector_layer import VectorLayer

logger = logging.getLogger(__name__)


class Tile(object):
    """
    A tile object which represents a single tile within a given grid
    """

    def __init__(self, minx: int, maxy: int, layer: Layer) -> None:
        self.minx: int = minx
        self.maxx: int = minx + layer.grid.width
        self.maxy: int = maxy
        self.miny: int = maxy - layer.grid.height
        self.tile_id: str = layer.grid.pointGridId(Point(minx, maxy))
        self.layer: Layer = layer
        if layer.src_type == "tiled":
            self.src_uri: str = "/vsis3/" + layer.src_path.format(self.tile_id)
        else:
            self.src_uri: str = "/vsis3/" + layer.src_path
        self.uri: str = layer.s3_path.format(tile_id=self.tile_id)

    def uri_exists(self) -> bool:
        if not self.uri:
            raise Exception("Tile URI needs to be set")
        return self._tile_exists("/vsis3/" + self.uri)

    def src_tile_exists(self) -> bool:
        if not isinstance(self.layer, RasterLayer):
            raise Exception("Must be Raster Layer")

        if not self.src_uri:
            raise Exception("Tile source URI needs to be set")
        return self._tile_exists(self.src_uri)

    def src_tile_intersects(self) -> bool:
        if not isinstance(self.layer, RasterLayer):
            raise Exception("Must be Raster Layer")

        if not self.uri or not self.src_uri:
            raise Exception("Tile URI and Tile source URI need to be set")

        intersects = False
        for x in [self.minx, self.maxx]:
            for y in [self.miny, self.maxy]:
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

    def src_vector_intersects(self) -> bool:
        if not isinstance(self.layer, VectorLayer):
            raise Exception("Must be Vector Layer")

        conn = psycopg2.connect(
            dbname=self.layer.db_name,
            user=self.layer.db_user,
            password=self.layer.db_password,
            host=self.layer.db_host,
            port=self.layer.db_port,
        )
        cursor = conn.cursor()
        exists_query = "select exists (select 1 from {name}_{version} where tile_id__{grid} = {tile_id})".format(
            name=self.layer.name,
            version=self.layer.version,
            grid=self.layer.grid.name,
            tile_id=self.tile_id,
        )
        cursor.execute(exists_query)
        exists = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        logger.info(self.tile_id, exists)
        return exists

    def is_empty(self) -> bool:

        with rasterio.open(self.uri) as src:
            msk = src.read_masks(1).astype(bool)
        if msk[msk].size == 0:
            return True
        else:
            return False

    @staticmethod
    def _tile_exists(uri: str) -> bool:

        cmd = ["gdalinfo", uri]

        try:
            logging.info("Check if tile exist " + uri)
            sp.check_call(cmd)
        except sp.CalledProcessError as pe:
            logging.warning("Could not find tile file " + uri)
            logging.warning(pe)
            return False
        else:
            return True
