import logging
import math
from typing import Optional

from pyproj import CRS
from shapely.geometry import Point

logger = logging.getLogger(__name__)


class Grid(object):
    """
    Output tiles will be organized in a regular grid.
    Each tile within grid has same width and height and is subdivided into blocks.
    Blocks must fully fit into tile.
    By default tile width and height, block width and height and  pixel width and height
    are considered equal respectively.
    Grid identifier are the coordinates of the top left corner (ie 10N_010E)
    """

    def __init__(
        self,
        srs: str,
        width: int,
        cols: int,
        blockxsize: int,
        height: Optional[int] = None,
        rows: Optional[int] = None,
        blockysize: Optional[int] = None,
    ) -> None:
        self.srs: CRS = CRS.from_string(srs)
        self.width: int = width
        self.height: int = width if not height else height
        self.cols: int = cols
        self.rows: int = cols if not rows else rows
        self.blockxsize: int = blockxsize
        self.blockysize: int = blockxsize if not blockysize else blockysize
        self.xres: float = self.width / self.cols
        self.yres: float = self.height / self.rows
        self.name: str = "{}x{}".format(self.width, self.height)

    def pointGridId(self, point: Point) -> str:
        """
        Calculate the GRID ID based on a coordinate inside tile
        :param point: POINT(lng, lat)
        :return: Grid Id
        """
        col: int = math.floor(point.x / self.width) * self.width
        lng: str = str(col).zfill(3) + "E" if (col >= 0) else str(-col).zfill(3) + "W"

        row: int = math.ceil(point.y / self.height) * self.height
        lat: str = str(row).zfill(2) + "N" if (row >= 0) else str(-row).zfill(3) + "W"

        return "{}_{}".format(lat, lng)
