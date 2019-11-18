import subprocess as sp
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import psycopg2
import rasterio
from rasterio.coords import BoundingBox
from rasterio.errors import RasterioIOError
from pyproj import Transformer
from shapely.geometry import Point

from gfw_pixetl import get_module_logger
from gfw_pixetl.errors import GDALAccessDeniedError
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

        self.src_profile: Dict[str, Any]
        self.src_bounds: BoundingBox

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

    def _tile_exists(self, uri: str) -> bool:

        logger.debug("Check if tile {} exists".format(uri))

        try:
            with rasterio.open(uri) as src:
                self.src_profile = src.profile
                self.src_bounds = src.bounds
        except RasterioIOError as e:
            if (
                str(e)
                == f"'{uri}' does not exist in the file system, and is not recognized as a supported dataset name."
                or str(e) == "The specified key does not exist."
            ):
                logger.info(f"File does not exist {uri}")
                return False

            else:
                logger.exception(f"Cannot open {uri}")
                raise
        except Exception:
            logger.exception(f"Cannot open {uri}")
            raise
        else:
            logger.info(f"File {uri} exists")
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
        """
        Check if target tile extent intersects with source extent.
        """

        if not hasattr(self, "src_bounds"):
            self.src_tile_exists()

        proj = Transformer.from_crs(
            self.grid.srs, self.src_profile["crs"], always_xy=True
        )
        inverse = Transformer.from_crs(
            self.src_profile["crs"], self.grid.srs, always_xy=True
        )

        # Get World Extent in Source Projection
        # Important: We have to get each top, left, right, bottom seperately.
        # We cannot get them using the corner coordinates.
        # For some projections such as Goode (epsg:54052) this would cause strange behavior
        world_top = proj.transform(0, 90)[1]
        world_left = proj.transform(-180, 0)[0]
        world_bottom = proj.transform(0, -90)[1]
        world_right = proj.transform(180, 0)[0]

        # Crop SRC Bounds to World Extent:
        left = max(world_left, self.src_bounds.left)
        top = min(world_top, self.src_bounds.top)
        right = min(world_right, self.src_bounds.right)
        bottom = max(world_bottom, self.src_bounds.bottom)

        # Convert back to Target Projection
        cropped_top = inverse.transform(0, top)[1]
        cropped_left = inverse.transform(left, 0)[0]
        cropped_bottom = inverse.transform(0, bottom)[1]
        cropped_right = inverse.transform(right, 0)[0]

        logger.debug(
            "World Extent: {}, {}, {}, {}".format(
                world_left, world_top, world_right, world_bottom
            )
        )
        logger.debug(
            "SRC Extent: {}, {}, {}, {}".format(
                self.src_bounds.left,
                self.src_bounds.top,
                self.src_bounds.right,
                self.src_bounds.bottom,
            )
        )
        logger.debug("Cropped Extent: {}, {}, {}, {}".format(left, top, right, bottom))
        logger.debug(
            "Inverted Copped Extent: {}, {}, {}, {}".format(
                cropped_left, cropped_top, cropped_right, cropped_bottom
            )
        )

        src_bbox = BoundingBox(
            left=cropped_left,
            top=cropped_top,
            right=cropped_right,
            bottom=cropped_bottom,
        )
        trg_bbox = BoundingBox(
            left=self.minx, top=self.maxy, right=self.maxx, bottom=self.miny
        )
        return not rasterio.coords.disjoint_bounds(src_bbox, trg_bbox)

    def calc_is_empty(self) -> bool:
        return self._is_empty(self.calc_uri)
