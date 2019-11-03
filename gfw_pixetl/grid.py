import math
from typing import Optional

from pyproj import CRS
from shapely.geometry import Point

from gfw_pixetl import get_module_logger


logger = get_module_logger(__name__)


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
        lat: str = str(row).zfill(2) + "N" if (row >= 0) else str(-row).zfill(2) + "S"

        return "{}_{}".format(lat, lng)


def grid_factory(grid_name) -> Grid:
    """
    Different Grid layout used for this project
    """

    # RAAD alerts
    if grid_name == "epsg_4326_3x3" or grid_name == "3x3":
        return Grid("epsg:4326", 3, 50000, 250)

    # GLAD alerts and UMD Forest Loss
    elif grid_name == "epsg_4326_10x10" or grid_name == "10x10":
        return Grid("epsg:4326", 10, 40000, 250)

    # VIIRS Fire alerts
    elif grid_name == "epsg_4326_30x30" or grid_name == "30x30":
        return Grid("epsg:4326", 30, 9000, 450)

    # MODIS Fire alerts
    elif grid_name == "epsg_4326_90x90" or grid_name == "90x90":
        return Grid("epsg:4326", 90, 10000, 500)

    else:
        message = "Unknown grid name: {}".format(grid_name)
        logger.exception(message)
        raise ValueError(message)
