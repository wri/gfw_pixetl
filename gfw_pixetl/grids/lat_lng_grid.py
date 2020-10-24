import itertools
import math
from multiprocessing import Pool
from multiprocessing.pool import Pool as PoolType
from typing import Iterable, List, Set, Tuple

from rasterio.coords import BoundingBox
from shapely.geometry import Point

from gfw_pixetl import get_module_logger
from gfw_pixetl.grids import Grid
from gfw_pixetl.settings.globals import SETTINGS

LOGGER = get_module_logger(__name__)


class LatLngGrid(Grid):
    """Tile grid using lat/lng coordinates.

    Grid identifier are the coordinates of the top left corner (ie
    10N_010E)
    """

    def __repr__(self):
        return f"LatLngGrid(srs={self.crs.to_string()}, name={self.name})"

    def __init__(self, width: int, cols: int, crs: str = "epsg:4326") -> None:
        """Generate tile grid.

        Grid must have equal width and height. Pixel row and column must
        be a multiple of 16, to be able to divide tile into blocks.
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

        self.width: int = width
        self.height: int = width  # if not height else height
        self.lng_offset: int = int(self.width / 2) if (360 / self.width) % 2 else 0
        self.lat_offset: int = int(self.height / 2) if (180 / self.height) % 2 else 0

        self._cols: int = cols
        self._rows: int = cols  # if not rows else rows

        super().__init__(crs)

    def xy_to_tile_origin(self, x: float, y: float) -> Point:
        """Calculate top left corner of corresponding grid tile for any given
        point.

        In case tiles don't align with equator and central meridian we
        have to introduce an offset. We always assume that grids and
        offset are whole numbers
        """

        lng: int = math.floor(x / self.width) * self.width
        lng = self._apply_lng_offset(lng, x)

        lat: int = math.ceil(y / self.height) * self.height
        lat = self._apply_lat_offset(lat, y)

        print("LAT", lat)
        print("LNG", lng)

        # Make sure we are are still on earth
        assert 180 - self.width >= lng >= -180, "Origin's Longitude is out of bounds"
        assert 90 >= lat >= -90 + self.height, "Origin's Latitude is out of bounds"

        return Point(lng, lat)

    def xy_to_tile_id(self, x: float, y: float) -> str:
        """Wrapper function, in case you want to pass points as x/y
        coordiantes."""

        p = self.xy_to_tile_origin(x, y)
        x = p.x
        y = p.y
        col = int(x)
        row = int(y)
        # col: int = math.floor(x / self.width) * self.width
        lng: str = f"{str(col).zfill(3)}E" if (col >= 0) else f"{str(-col).zfill(3)}W"
        # row: int = math.ceil(y / self.height) * self.height
        lat: str = f"{str(row).zfill(2)}N" if (row >= 0) else f"{str(-row).zfill(2)}S"

        return f"{lat}_{lng}"

    @staticmethod
    def tile_id_to_point(grid_id: str) -> Point:
        """Compute top left coordinate based on grid id."""
        _lat, _lng = grid_id.split("_")
        lat = int(_lat[:2])
        lng = int(_lng[:3])

        if _lat[-1:] == "S":
            lat = lat * -1

        if _lng[-1:] == "W":
            lng = lng * -1

        return Point(lng, lat)

    def get_tile_bounds(self, grid_id) -> BoundingBox:

        origin = self.tile_id_to_point(grid_id)

        return BoundingBox(
            left=origin.x,
            bottom=origin.y - self.height,
            right=origin.x + self.width,
            top=origin.y,
        )

    def get_tile_ids(self) -> Set[str]:

        lat_offset = self.lat_offset if 180 % self.height else 0
        lng_offset = self.lng_offset if 360 % self.width else 0

        # get all top let corners within grid
        x: Iterable[int] = range(-180 + lng_offset, 180 - lng_offset, self.width)
        y: Iterable[int] = range(-89 + lat_offset, 91 - lat_offset, self.height)
        x_y: List[Tuple[int, int]] = list(itertools.product(x, y))

        # Get all grid ids using top left corners
        pool: PoolType = Pool(processes=SETTINGS.cores)
        tile_ids: Set[str] = set(pool.map(self._get_tile_ids, x_y))

        return tile_ids

    def _get_tile_ids(self, x_y: Tuple[int, int]) -> str:
        return self.xy_to_tile_id(x_y[0], x_y[1])

    def _apply_lng_offset(self, lng, x):
        """apply longitudinal offset and shift grid cell in case point doesn't
        fall into it."""

        offset = self.lng_offset
        if lng != 0 and offset:
            offset = offset * int(lng / abs(lng))

        lng -= offset

        if offset and x < lng:
            lng -= self.width
        elif offset and x > lng + self.width:
            lng += self.width

        return lng

    def _apply_lat_offset(self, lat, y):
        """apply latitudinal offset and shift grid cell in case point doesn't
        fall into it."""
        offset = self.lat_offset
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

    def _get_xres(self) -> float:
        """Horizontal resolution of tile."""
        return self.width / self.cols

    def _get_yres(self) -> float:
        """Vertical resolution of tile."""
        return self.height / self.rows

    def _get_name(self):
        """Name of Grid."""
        return f"{self.width}/{self.cols}"

    def _get_cols(self) -> int:
        """Get tile width."""
        return self._cols

    def _get_rows(self) -> int:
        """Get tile height."""
        return self._rows
