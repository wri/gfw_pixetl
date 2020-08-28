import math

from pyproj import CRS
from shapely.geometry import Point

from gfw_pixetl import get_module_logger

LOGGER = get_module_logger(__name__)


class Grid(object):
    """Output tiles will be organized in a regular grid.

    Each tile within grid has same width and height and is subdivided
    into blocks. Blocks must fully fit into tile. By default tile width
    and height, block width and height and  pixel width and height are
    considered equal respectively. Grid identifier are the coordinates
    of the top left corner (ie 10N_010E)
    """

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"Grid(srs={self.srs.to_string()}, width={self.width}, height={self.height})"

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.name == other.name

    def __init__(self, srs: str, width: int, cols: int) -> None:
        """Generate tile grid.

        Grid must have equal width and height. Pixel row and column must
        be a multiple of 16, to be able to devide tile into blocks.
        Tiles must fully fit into 360 degree extent. If tile height does
        not fully fit into 180 degree extent, extent will be equally
        cropped at top and bottom.
        """

        assert not 360 % width, "Tiles must fully fit into 360 degree extent"
        assert not cols % 16, "Column number must be a multiple of 16"
        if width % 2:
            assert (
                not (360 / width) % 2
            ), "Uneven grid sizes cannot have a longitude offset"
            assert (
                not (180 / width) % 2
            ), "Uneven grid sizes cannot have a latitude offset"

        self.srs: CRS = CRS.from_string(srs)
        self.width: int = width
        self.height: int = width  # if not height else height
        self.cols: int = cols
        self.rows: int = cols  # if not rows else rows
        self.blockxsize: int = self._get_block_size()
        self.blockysize: int = self._get_block_size()
        self.xres: float = self.width / self.cols
        self.yres: float = self.height / self.rows
        self.name: str = f"{self.width}/{self.cols}"

    def xy_grid_origin(self, x: float, y: float) -> Point:
        return self.point_grid_origin(Point(x, y))

    def point_grid_origin(self, point: Point) -> Point:
        """Calculate top left corner of corresponding grid tile for any given
        point.

        In case tiles don't align with equator and central meridian we
        have to introduce an offset. We always assume that grids and
        offset are whole numbers
        """

        lng_offset: int = int(self.width / 2) if (360 / self.width) % 2 else 0
        lat_offset: int = int(self.height / 2) if (180 / self.height) % 2 else 0

        lng: int = math.floor(point.x / self.width) * self.width
        lng = self._apply_lng_offset(lng, point.x, lng_offset)

        lat: int = math.ceil(point.y / self.height) * self.height
        lat = self._apply_lat_offset(lat, point.y, lat_offset)

        # Make sure we are are still on earth
        assert 180 >= lng >= -180, "Origin's Longitude is out of bounds"
        assert 90 >= lat >= -90, "Origin's Latitude is out of bounds"

        return Point(lng, lat)

    def xy_grid_id(self, x: float, y: float) -> str:
        """Wrapper function, in case you want to pass points as x/y
        coordiantes."""
        return self.point_grid_id(Point(x, y))

    def point_grid_id(self, point: Point) -> str:
        """Calculate the GRID ID based on a coordinate inside tile."""
        point = self.point_grid_origin(point)
        col = int(point.x)
        row = int(point.y)
        # col: int = math.floor(point.x / self.width) * self.width
        lng: str = f"{str(col).zfill(3)}E" if (col >= 0) else f"{str(-col).zfill(3)}W"

        # row: int = math.ceil(point.y / self.height) * self.height
        lat: str = f"{str(row).zfill(2)}N" if (row >= 0) else f"{str(-row).zfill(2)}S"

        return f"{lat}_{lng}"

    def _apply_lng_offset(self, lng, x, offset):
        """apply longitudinal offset and shift grid cell in case point doesn't
        fall into it."""
        if lng != 0 and offset:
            offset = offset * int(lng / abs(lng))

        lng -= offset

        if offset and x < lng:
            lng -= self.width
        elif offset and x > lng + self.width:
            lng += self.width

        return lng

    def _apply_lat_offset(self, lat, y, offset):
        """apply latitudinal offset and shift grid cell in case point doesn't
        fall into it."""
        if lat != 0 and offset:
            offset = -(offset * int(lat / abs(lat)))

        lat += offset

        if offset and y > lat:
            lat += self.height
        elif offset and y < lat - self.height:
            lat -= self.height
        return lat

    def _get_block_size(self):
        """Try to divide tile into blocks between 128 and 512 pixels.

        Blocks must be a multiple of 16.
        """

        min_block_size = 128
        max_block_size = 512
        block_width = None
        b_width = 0
        x = 0

        while b_width <= max_block_size:
            x += 1
            n_blocks = self.cols / (16 * x)
            b_width = self.cols / n_blocks
            if (
                b_width >= min_block_size
                and b_width.is_integer()
                and (self.cols / b_width).is_integer()
            ):
                block_width = b_width

        if not block_width:
            raise ValueError("Cannot create blocks between 128 and 512 pixels")

        return int(block_width)


def grid_factory(grid_name) -> Grid:
    """Different Grid layout used for this project."""

    grids = {
        "1/4000": Grid("epsg:4326", 1, 4000),  # TEST grid
        "3/33600": Grid("epsg:4326", 3, 33600),  # RAAD alerts, ~10m pixel
        "10/40000": Grid("epsg:4326", 10, 40000),  # UMD alerts, ~30m pixel
        "8/32000": Grid(
            "epsg:4326", 8, 32000
        ),  # UMD alerts, ~30m pixel, data cube optimized Grid
        "90/27008": Grid("epsg:4326", 90, 27008),  # VIIRS Fire alerts, ~375m pixel
        "90/9984": Grid("epsg:4326", 90, 9984),  # MODIS Fire alerts, ~1000m pixel
    }

    try:
        grid = grids[grid_name]
    except KeyError:
        message = f"Unknown grid name: {grid_name}"
        LOGGER.exception(message)
        raise ValueError(message)

    return grid
