import math
from math import floor, sqrt
from typing import Iterator, List, Tuple

import numpy as np
import rasterio
from numpy.ma import MaskedArray
from rasterio.io import DatasetReader, DatasetWriter
from rasterio.vrt import WarpedVRT
from rasterio.warp import transform_bounds
from rasterio.windows import Window, bounds, from_bounds, union
from retrying import retry

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.decorators import lazy_property, processify
from gfw_pixetl.errors import retry_if_rasterio_io_error
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.settings.globals import GDAL_ENV, SETTINGS
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.gdal import create_vrt

LOGGER = get_module_logger(__name__)

Windows = Tuple[Window, Window]
Bounds = Tuple[float, float, float, float]


class RasterSrcTile(Tile):
    def __init__(self, tile_id: str, grid: Grid, layer: RasterSrcLayer) -> None:
        super().__init__(tile_id, grid, layer)
        self.layer: RasterSrcLayer = layer
        # self.src: RasterSource = RasterSource(uri=self._vrt())

    @lazy_property
    def src(self) -> RasterSource:
        LOGGER.debug(f"Find input files for {self.tile_id}")
        files = list()
        for f in self.layer.input_files:
            if self.dst[self.default_format].geom.intersects(f[0]) and not self.dst[
                self.default_format
            ].geom.touches(f[0]):
                LOGGER.debug(f"Add file {f[1]} to input files for {self.tile_id}.vrt")
                files.append(f[1])

        if not len(files):
            raise Exception(
                f"Did not find any intersecting files for tile {self.tile_id}"
            )
        return RasterSource(
            create_vrt(files, self.tile_id + ".vrt", self.tile_id + ".txt")
        )

    @lazy_property
    def intersecting_window(self) -> Window:
        dst_left, dst_bottom, dst_right, dst_top = self.dst[self.default_format].bounds
        src_left, src_bottom, src_right, src_top = self.src.reproject_bounds(
            self.grid.crs
        )

        left = max(dst_left, src_left)
        bottom = max(dst_bottom, src_bottom)
        right = min(dst_right, src_right)
        top = min(dst_top, src_top)

        window: Window = rasterio.windows.from_bounds(
            left, bottom, right, top, transform=self.dst[self.default_format].transform
        )
        return utils.snapped_window(window)

    def within(self) -> bool:
        """Check if target tile extent intersects with source extent."""
        return (
            # must intersect, but we don't want geometries that only share an exterior point
            self.dst[self.default_format].geom.intersects(self.layer.geom)
            and not self.dst[self.default_format].geom.touches(self.layer.geom)
        )

    def transform(self) -> bool:
        """Write input data to output tile."""
        LOGGER.info(f"Transform tile {self.tile_id}")

        window: Window
        has_data = False

        try:
            with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True, **GDAL_ENV):
                src: DatasetReader = rasterio.open(self.src.uri, "r", sharing=False)

                transform, width, height = self._vrt_transform(
                    *self.src.reproject_bounds(self.grid.crs)
                )
                vrt: WarpedVRT = WarpedVRT(
                    src,
                    crs=self.dst[self.default_format].crs,
                    transform=transform,
                    width=width,
                    height=height,
                    warp_mem_limit=int(
                        utils.available_memory_per_process() / 1000
                    ),  # Memory in MB
                    resampling=self.layer.resampling,
                )

                for window in self.windows():
                    has_data = self._transform(vrt, window, has_data)
                vrt.close()
                src.close()

        except Exception as e:
            LOGGER.exception(e)
            self.status = "failed"
            has_data = True

        else:
            # invoking gdal-geotiff here instead of in a separate stage to assure we don't run out of memory
            # the transform stage uses all available memory for concurrent processes.
            # Having another stage which needs a lot of memory might cause the process to crash
            self.create_gdal_geotiff()

        return has_data

    @processify
    def _transform(self, vrt: WarpedVRT, window: Window, has_data: bool):
        """Reading windows from input VRT, reproject, resample, transform and
        write to destination.

        We run this in a separate process, to make sure that memory get
        completely cleared once block is processed. With out this, we
        might experience memory leakage, in particular for float data
        types.
        """
        masked_array: MaskedArray = self._read_window(vrt, window)
        if self._block_has_data(masked_array):
            LOGGER.debug(f"{window} of tile {self.tile_id} has data - continue")
            masked_array = self._calc(masked_array, window)
            array: np.ndarray = self._set_dtype(masked_array, window)
            del masked_array
            self._write_window(array, window)
            del array
            has_data = True
        else:
            LOGGER.debug(f"{window} of tile {self.tile_id} has no data - skip")
            del masked_array
        return has_data

    def windows(self) -> List[Window]:
        """Creates local output file and returns list of size optimized windows
        to process."""
        LOGGER.debug(f"Create local output file for tile {self.tile_id}")
        with rasterio.Env(**GDAL_ENV):
            with rasterio.open(
                self.get_local_dst_uri(self.default_format),
                "w",
                **self.dst[self.default_format].profile,
            ) as dst:
                windows = [window for window in self._windows(dst)]
        self.set_local_dst(self.default_format)

        return windows

    def _windows(self, dst: DatasetWriter) -> Iterator[Window]:
        """Divides raster source into larger windows which will still fit into
        memory."""

        block_count: int = int(sqrt(self._max_blocks(dst)))
        x_blocks: int = int(dst.width / dst.block_shapes[0][0])
        y_blocks: int = int(dst.height / dst.block_shapes[0][1])

        for i in range(0, x_blocks, block_count):
            for j in range(0, y_blocks, block_count):
                max_i = min(i + block_count, x_blocks)
                max_j = min(j + block_count, y_blocks)
                window = self._union_blocks(dst, i, j, max_i, max_j)
                try:
                    yield utils.snapped_window(
                        window.intersection(self.intersecting_window)
                    )
                except rasterio.errors.WindowError as e:
                    if not (str(e) == "windows do not intersect"):
                        raise

    @staticmethod
    def _block_has_data(array: MaskedArray) -> bool:
        """Check if current block has any data."""
        msk = np.invert(array.mask.astype(bool))
        size = msk[msk].size
        LOGGER.debug(f"Block has {size} data pixels")
        return array.shape[0] > 0 and array.shape[1] > 0 and size != 0

    def _calc(self, array: MaskedArray, dst_window: Window) -> MaskedArray:
        """Apply user defined calculation on array."""
        if self.layer.calc:
            LOGGER.debug(f"Update {dst_window} of tile {self.tile_id}")
            funcstr = (
                f"def f(A: MaskedArray) -> MaskedArray:\n    return {self.layer.calc}"
            )
            exec(funcstr, globals())
            array = f(array)  # type: ignore # noqa: F821
        else:
            LOGGER.debug(
                f"No user defined formula provided. Skip calculating values for {dst_window} of tile {self.tile_id}"
            )
        return array

    def _max_blocks(self, dst: DatasetWriter) -> int:
        """Calculate the maximum amount of blocks we can fit into memory,
        making sure that blocks can always fill a squared extent.

        We can only use a fraction of the available memory per process
        per block b/c we might have multiple copies of the array at the
        same time. Using a divisor of 8 leads to max memory usage of
        about 75%.
        """

        divisor = SETTINGS.divisor

        if self.layer.calc is not None:

            divisor = (
                divisor ** 2
            )  # further reduce block size in case we need to perform additional computations
        LOGGER.debug(f"Divisor set to {divisor} for tile {self.tile_id}")

        # max_bytes_per_block: float = self._max_block_size(dst) * self._max_itemsize()
        block_size: int = (
            self.dst[self.default_format].blockxsize
            * self.dst[self.default_format].blockysize
        )
        item_size: int = np.zeros(1, dtype=self.dst[self.default_format].dtype).itemsize

        max_bytes_per_block: int = block_size * item_size
        memory_per_process: float = utils.available_memory_per_process() / divisor

        # make sure we get an number we whose sqrt is a whole number
        max_blocks: int = floor(sqrt(memory_per_process / max_bytes_per_block)) ** 2

        LOGGER.debug(f"Maximum number of blocks to read at once: {max_blocks}")
        return max_blocks

    def _max_block_size(self, dst: DatasetWriter) -> float:
        """Depending on projections, # of input pixels for output block can
        vary.

        Here we take the corner blocks of the output tile and compare
        the pixel count with the pixel count in input tile, covered by
        each block extent We return the largest amount of pixels which
        are possibly covered by one block.
        """
        width: float = dst.width
        height: float = dst.height
        blockxsize: int = self.dst[self.default_format].blockxsize
        blockysize: int = self.dst[self.default_format].blockysize

        ul: Window = dst.block_window(1, 0, 0)
        ur: Window = dst.block_window(1, 0, width / blockxsize - 1)
        ll: Window = dst.block_window(1, height / blockysize - 1, 0)
        lr: Window = dst.block_window(
            1, height / blockysize - 1, width / blockxsize - 1
        )

        dst_windows: List[Window] = [ul, ur, ll, lr]
        src_windows: List[Window] = [self._reproject_dst_window(w) for w in dst_windows]

        max_block_size = max([w.width * w.height for w in dst_windows + src_windows])
        LOGGER.debug(f"Max block size for tile {self.tile_id} set to {max_block_size}")

        return max_block_size

    def _max_itemsize(self) -> int:
        """Check how many bytes one pixel of the largest datatype used will
        require."""
        src_itemsize: int = np.zeros(1, dtype=self.src.dtype).itemsize
        dst_itemsize: int = np.zeros(
            1, dtype=self.dst[self.default_format].dtype
        ).itemsize

        return max(src_itemsize, dst_itemsize)

    @retry(
        retry_on_exception=retry_if_rasterio_io_error,
        stop_max_attempt_number=7,
        wait_exponential_multiplier=1000,
        wait_exponential_max=300000,
    )  # Wait 2^x * 1000 ms between retries by to 300 sec, then 300 sec afterwards.
    def _read_window(self, vrt: WarpedVRT, dst_window: Window) -> MaskedArray:
        """Read window of input raster."""
        dst_bounds: Bounds = bounds(dst_window, self.dst[self.default_format].transform)
        window = vrt.window(*dst_bounds)

        src_bounds = transform_bounds(
            self.dst[self.default_format].crs, self.src.crs, *dst_bounds
        )

        LOGGER.debug(
            f"Read {dst_window} for Tile {self.tile_id} - this corresponds to bounds {src_bounds} in source"
        )
        try:
            return vrt.read(
                window=window,
                out_shape=(int(round(dst_window.height)), int(round(dst_window.width))),
                masked=True,
            )
        except rasterio.RasterioIOError:
            LOGGER.warning(
                f"RasterioIO error while reading {dst_window} for Tile {self.tile_id}. "
                "Will make attempt to retry."
            )
            raise

    def _reproject_dst_window(self, dst_window: Window) -> Window:
        """Reproject window into same projection as source raster."""

        dst_bounds: Bounds = bounds(
            window=dst_window,
            transform=self.dst[self.default_format].transform,
            height=self.grid.blockysize,
            width=self.grid.blockxsize,
        )
        src_bounds: Bounds = transform_bounds(
            self.dst[self.default_format].crs, self.src.crs, *dst_bounds
        )

        src_window: Window = from_bounds(*src_bounds, transform=self.src.transform)
        LOGGER.debug(
            f"Source window for {dst_window} of tile {self.tile_id} is {src_window}"
        )
        return src_window

    def _set_dtype(self, array: MaskedArray, dst_window) -> np.ndarray:
        """Update data type to desired output datatype Update nodata value to
        desired nodata value (current no data values will be updated and any
        values which already has new no data value will stay as is)"""
        if self.dst[self.default_format].has_no_data():
            LOGGER.debug(
                f"Set datatype and no data value for {dst_window} of tile {self.tile_id}"
            )
            array = np.ma.filled(array, self.dst[self.default_format].nodata).astype(
                self.dst[self.default_format].dtype
            )

        else:
            LOGGER.debug(f"Set datatype for {dst_window} of tile {self.tile_id}")
            array = array.data.astype(self.dst[self.default_format].dtype)
        return array

    def _snap_coordinates(self, lat: float, lng: float) -> Tuple[float, float]:
        """Snap a given coordinate to tile grid coordinates.

        Always returns the closes coordinates to the top left of the
        input coordinates
        """

        LOGGER.debug(f"Snap coordinates {lat}, {lng}")

        # Get top left corner for 1x1 degree grid
        top: float = math.ceil(lat)
        left: float = math.floor(lng)

        # get closes coordinate pair
        while top - lat > self.grid.yres:
            top -= self.grid.yres

        while lng - left > self.grid.xres:
            left += self.grid.xres

        LOGGER.debug(f"Snapped coordinates {top}, {left}")

        return top, left

    def _vrt_transform(
        self, west: float, south: float, east: float, north: float
    ) -> Tuple[rasterio.Affine, float, float]:
        """Compute Affine transformation, width and height for WarpedVRT using
        output CRS and pixel size."""

        LOGGER.debug(f"Output Bounds {west, south, east, north}")
        north, west = self._snap_coordinates(north, west)
        south, east = self._snap_coordinates(south, east)

        transform: rasterio.Affine = rasterio.transform.from_origin(
            west, north, self.grid.xres, self.grid.yres
        )
        width = round((east - west) / self.grid.xres)
        height = round((north - south) / self.grid.yres)

        LOGGER.debug(f"Output Affine and dimensions {transform}, {width}, {height}")
        return transform, width, height

    @staticmethod
    def _union_blocks(
        dst: DatasetWriter, min_i: int, min_j: int, max_i: int, max_j: int
    ) -> Window:
        """Loops over selected blocks of data source and merges their windows
        into one."""
        windows: List[Window] = list()

        for i in range(min_i, max_i):
            for j in range(min_j, max_j):
                windows.append(dst.block_window(1, i, j))
        return union(*windows)

    def _write_window(self, array: np.ndarray, dst_window: Window) -> None:
        """Write blocks into output raster."""
        with rasterio.Env(**GDAL_ENV):
            with rasterio.open(
                self.local_dst[self.default_format].uri,
                "r+",
                **self.dst[self.default_format].profile,
            ) as dst:
                LOGGER.debug(f"Write {dst_window} of tile {self.tile_id}")
                dst.write(array, window=dst_window)
                del array
