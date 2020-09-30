import itertools
import math
from abc import ABC, abstractmethod
from multiprocessing import Pool
from multiprocessing.pool import Pool as PoolType
from typing import Dict, Iterable, List, Set, Tuple

from pyproj import CRS, Transformer
from rasterio.coords import BoundingBox
from shapely.geometry import Point

from gfw_pixetl import get_module_logger
from gfw_pixetl.decorators import lazy_property
from gfw_pixetl.settings.globals import CORES

LOGGER = get_module_logger(__name__)


class Grid(ABC):
    """Output tiles will be organized in a regular grid.

    Each tile within grid has same width and height and is subdivided
    into blocks. Blocks must fully fit into tile. By default tile width
    and height, block width and height and  pixel width and height are
    considered equal respectively.
    """

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"Grid(srs={self.crs.to_string()}, name={self.name})"

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.name == other.name

    def __init__(self, crs: str) -> None:
        """Generate tile grid.

        Grid must have equal width and height. Pixel row and column must
        be a multiple of 16, to be able to divide tile into blocks.
        Tiles must fully fit into 360 degree extent. If tile height does
        not fully fit into 180 degree extent, extent will be equally
        cropped at top and bottom.
        """

        self.crs: CRS = CRS.from_string(crs)

        self.cols: int = self._get_cols()
        self.rows: int = self._get_rows()
        self.name: str = self._get_name()
        self.bounds: BoundingBox = self._get_bounds()
        self.xres: float = self._get_xres()
        self.yres: float = self._get_yres()
        self.blockxsize: int = self._get_block_size()
        self.blockysize: int = self._get_block_size()

    def to_wgs84(self, x: float, y: float) -> Tuple[float, float]:
        transformer = Transformer.from_crs(
            self.crs, CRS.from_epsg(4326), always_xy=True
        )
        return transformer.transform(x, y)

    def from_wgs84(self, x: float, y: float) -> Tuple[float, float]:
        transformer = Transformer.from_crs(
            CRS.from_epsg(4326), self.crs, always_xy=True
        )
        return transformer.transform(x, y)

    @abstractmethod
    def get_tile_ids(self) -> Set[str]:
        """Return all grid ids for given Grid."""
        ...

    @abstractmethod
    def get_tile_bounds(self, grid_id) -> BoundingBox:
        """Returns BBox for a given grid ID."""
        ...

    def _get_bounds(self) -> BoundingBox:
        area_of_use = self.crs.area_of_use
        left, top = self.from_wgs84(area_of_use.west, area_of_use.north)
        right, bottom = self.from_wgs84(area_of_use.east, area_of_use.south)
        return BoundingBox(left=left, right=right, top=top, bottom=bottom)

    @abstractmethod
    def _get_block_size(self) -> int:
        """Try to divide tile into blocks between 128 and 512 pixels.

        Blocks must be a multiple of 16.
        """
        ...

    @abstractmethod
    def _get_xres(self) -> float:
        """Horizontal resolution of tile."""
        ...

    @abstractmethod
    def _get_yres(self) -> float:
        """Vertical resolution of tile."""
        ...

    @abstractmethod
    def _get_cols(self) -> int:
        """Width of tile."""
        ...

    @abstractmethod
    def _get_rows(self) -> int:
        """Height of tile."""
        ...

    @abstractmethod
    def _get_name(self) -> str:
        """Name of Grid."""
        ...


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
        return self.point_to_tile_origin(Point(x, y))

    def point_to_tile_origin(self, point: Point) -> Point:
        """Calculate top left corner of corresponding grid tile for any given
        point.

        In case tiles don't align with equator and central meridian we
        have to introduce an offset. We always assume that grids and
        offset are whole numbers
        """

        lng: int = math.floor(point.x / self.width) * self.width
        lng = self._apply_lng_offset(lng, point.x)

        lat: int = math.ceil(point.y / self.height) * self.height
        lat = self._apply_lat_offset(lat, point.y)

        # Make sure we are are still on earth
        assert 180 >= lng >= -180, "Origin's Longitude is out of bounds"
        assert 90 >= lat >= -90, "Origin's Latitude is out of bounds"

        return Point(lng, lat)

    def xy_to_tile_id(self, x: float, y: float) -> str:
        """Wrapper function, in case you want to pass points as x/y
        coordiantes."""
        #     return self.point_to_tile_id(Point(x, y))
        #
        # def point_to_tile_id(self, point: Point) -> str:
        #     """Calculate the GRID ID based on a coordinate inside tile."""
        #     point = self.point_to_tile_origin(point)
        #     col = int(point.x)
        #     row = int(point.y)

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
        pool: PoolType = Pool(processes=CORES)
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


class WebMercatorGrid(Grid):
    """Tile grid using webmercator coordindates.

    Each Grid instance defines tile for a given zoom level

    Output tiles within grid always have a block size of 256 px (equal to raster tile cache tile).
    Max size of a tile within grid can not be larger than 65536x65526 pixel (256x256).
    """

    def __repr__(self):
        return f"WebMercatorGrid(srs={self.crs.to_string()}, name={self.name})"

    def __init__(self, zoom: int, crs: str = "epsg:3857") -> None:
        """Initialize Webmercator tile grid of a given Zoom level."""

        self.zoom: int = zoom
        self.nb_tiles = max(1, int(2 ** self.zoom / 256)) ** 2
        super().__init__(crs)

    def get_tile_ids(self) -> Set[str]:
        """List of all tile ids within grid."""
        _nb_tiles: int = int(math.sqrt(self.nb_tiles))
        rows: Iterable[int] = range(0, _nb_tiles)
        cols: Iterable[int] = range(0, _nb_tiles)

        rows_cols: List[Tuple[int, int]] = list(itertools.product(rows, cols))

        # Get all grid ids using top left corners
        pool: PoolType = Pool(processes=CORES)
        tile_ids: Set[str] = set(pool.map(self._get_tile_ids, rows_cols))

        return tile_ids

    def _get_tile_ids(self, row_col: Tuple[int, int]):
        row = row_col[0]
        col = row_col[1]
        return f"{str(row).zfill(3)}R_{str(col).zfill(3)}C"

    def get_tile_bounds(self, grid_id) -> BoundingBox:
        """BBox for a given tile."""
        nb_tiles = int(math.sqrt(self.nb_tiles))

        _row, _col = grid_id.split("_")
        row = int(_row[:-1])
        col = int(_col[:-1])

        # Top left corner is (0,0)
        false_easting = self.bounds.left * -1
        false_southing = self.bounds.top * -1

        grid_height = self.bounds.top + self.bounds.bottom + (2 * false_southing)
        grid_width = self.bounds.east + self.bounds.west + (2 * false_easting)

        tile_height = grid_height / nb_tiles
        tile_width = grid_width / nb_tiles

        tile_left = col * tile_width - false_easting
        tile_right = (col + 1) * tile_width - false_easting
        tile_top = row * tile_height - false_southing
        tile_bottom = (row + 1) * tile_height - false_southing

        return BoundingBox(
            left=tile_left, top=tile_top, right=tile_right, bottom=tile_bottom
        )

    def _get_block_size(self) -> int:
        """Block size for WebMercator Tiles are is always 256x256."""
        return 256

    def _get_xres(self) -> float:
        """Pixel width."""
        grid_width = self.bounds.left + self.bounds.right + (-2 * self.bounds.left)
        pixels_per_row = 256 * 2 ** self.zoom
        return grid_width / pixels_per_row

    def _get_yres(self) -> float:
        """Pixel height."""
        grid_height = self.bounds.top + self.bounds.bottom + (-2 * self.bounds.bottom)
        pixels_per_col = 256 * 2 ** self.zoom
        return grid_height / pixels_per_col

    def _get_cols(self) -> int:
        """Number of columns per grid."""
        return int(2 ** self.zoom * 256 / math.sqrt(self.nb_tiles))

    def _get_rows(self) -> int:
        """Number of rows per grid."""
        return self._get_cols()

    def _get_name(self) -> str:
        return f"zoom_{self.zoom}"


def grid_factory(grid_name) -> Grid:
    """Different Grid layout used for this project."""

    grids: Dict[str, Grid] = {
        "1/4000": LatLngGrid(1, 4000),  # TEST grid
        "3/33600": LatLngGrid(3, 33600),  # RAAD alerts, ~10m pixel
        "10/40000": LatLngGrid(10, 40000),  # UMD alerts, ~30m pixel
        "8/32000": LatLngGrid(
            8, 32000
        ),  # UMD alerts, ~30m pixel, data cube optimized Grid
        "90/27008": LatLngGrid(90, 27008),  # VIIRS Fire alerts, ~375m pixel
        "90/9984": LatLngGrid(90, 9984),  # MODIS Fire alerts, ~1000m pixel
    }

    for zoom in range(0, 23):
        grids[f"zoom_{zoom}"] = WebMercatorGrid(zoom)

    try:
        grid = grids[grid_name]
    except KeyError:

        message = f"Unknown grid name: {grid_name}"
        LOGGER.exception(message)
        raise ValueError(message)

    return grid
