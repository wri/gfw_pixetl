from abc import ABC, abstractmethod
from typing import Set, Tuple

from pyproj import CRS, Transformer
from rasterio.coords import BoundingBox

from gfw_pixetl import get_module_logger
from gfw_pixetl.utils.utils import AreaOfUse

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
        self.area_of_use: AreaOfUse = self._get_area_of_use()

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

    def _get_area_of_use(self) -> AreaOfUse:
        """Area of use for given projection.

        Use AOU as defined by projection, but allow to optionally
        override with custom values
        """
        aou = self.crs.area_of_use
        return AreaOfUse(
            west=aou.west,
            north=aou.north,
            east=aou.east,
            south=aou.south,
            name=aou.name,
        )

    def _get_bounds(self) -> BoundingBox:
        left, top = self.from_wgs84(self.area_of_use.west, self.area_of_use.north)
        right, bottom = self.from_wgs84(self.area_of_use.east, self.area_of_use.south)
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
