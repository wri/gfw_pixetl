import itertools
import math
from typing import Iterable, Set, Tuple

from rasterio.coords import BoundingBox

from gfw_pixetl import get_module_logger
from gfw_pixetl.grids.grid import Grid
from gfw_pixetl.models.named_tuples import AreaOfUse

LOGGER = get_module_logger(__name__)


class WebMercatorGrid(Grid):
    """Tile grid using webmercator coordindates.

    Each Grid instance defines tile for a given zoom level

    Output tiles within grid always have a block size of 256 px (equal to raster tile cache tile).
    Max size of a tile within grid can not be larger than 65536x65526 pixel (256x256).
    """

    is_snapped_grid = False

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

        tile_ids: Set[str] = set()
        for rows_cols in itertools.product(rows, cols):
            tile_ids.add(self._get_tile_ids(rows_cols))

        # rows_cols: List[Tuple[int, int]] = list(itertools.product(rows, cols))
        #
        # # Get all grid ids using top left corners
        # with get_context("spawn").Pool(processes=GLOBALS.num_processes) as pool:
        #     tile_ids: Set[str] = set(pool.map(self._get_tile_ids, rows_cols))

        return tile_ids

    def _get_area_of_use(self) -> AreaOfUse:
        """Use more precise North/South coordinates than what is returned by
        PyProj."""
        return AreaOfUse(
            west=-180, south=-85.05112878, east=180, north=85.05112878, name="World"
        )

    @staticmethod
    def _get_tile_ids(row_col: Tuple[int, int]):
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
        grid_width = self.bounds.left + self.bounds.right + (2 * false_easting)

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
        """Block size for WebMercator Tiles is always 256x256."""
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
