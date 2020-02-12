import math
from math import floor, sqrt
from typing import Iterator, List, Tuple

import numpy as np
import rasterio
from numpy.ma import MaskedArray
from pyproj import Transformer
from rasterio.coords import BoundingBox
from rasterio.io import DatasetWriter, DatasetReader
from rasterio.vrt import WarpedVRT
from rasterio.warp import transform_bounds
from rasterio.windows import Window, from_bounds, bounds, union
from retrying import retry
from shapely.geometry import Point

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.decorators import lazy_property
from gfw_pixetl.errors import retry_if_rasterio_io_error
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.tiles import Tile

LOGGER = get_module_logger(__name__)

Windows = Tuple[Window, Window]
Bounds = Tuple[float, float, float, float]


class RasterSrcTile(Tile):
    def __init__(self, origin: Point, grid: Grid, layer: RasterSrcLayer) -> None:
        super().__init__(origin, grid, layer)
        self.src: RasterSource = layer.src

    def src_tile_intersects(self) -> bool:
        """
        Check if target tile extent intersects with source extent.
        """
        src_bounds: Bounds = self.reprojected_src_bounds
        src_bbox = BoundingBox(*src_bounds)
        return not rasterio.coords.disjoint_bounds(src_bbox, self.bounds)

    def transform(self) -> bool:
        """
        Write input data to output tile
        """

        src_window: Window
        dst_window: Window

        stage = "transform"
        dst_uri = self.get_stage_uri(stage)

        LOGGER.info(f"Transform tile {self.tile_id}")
        with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):

            src: DatasetReader = rasterio.open(self.src.uri)
            dst: DatasetWriter = rasterio.open(dst_uri, "w+", **self.dst.profile)

            transform, width, height = self._vrt_transform(
                src.crs, self.dst.profile["crs"], *self.reprojected_src_bounds
            )
            vrt: WarpedVRT = WarpedVRT(
                src,
                crs=self.dst.profile["crs"],
                transform=transform,
                width=width,
                height=height,
                resampling=self.layer.resampling,
            )

            has_data = False
            for dst_window in self.windows(dst):
                masked_array: MaskedArray = self._read_window(vrt, dst_window)
                if self._block_has_data(masked_array):
                    LOGGER.debug(
                        f"{dst_window} of tile {self.tile_id} has data - continue"
                    )
                    masked_array = self._calc(masked_array, dst_window)
                    array: np.ndarray = self._set_dtype(masked_array, dst_window)
                    del masked_array
                    self._write_window(dst, array, dst_window)
                    del array
                    has_data = True
                else:
                    LOGGER.debug(
                        f"{dst_window} of tile {self.tile_id} has no data - skip"
                    )
                    del masked_array
            vrt.close()
            src.close()
            dst.close()
            self.set_local_src(stage)
            return has_data

    @lazy_property
    def reprojected_src_bounds(self) -> Bounds:
        """
        Reproject src bounds to dst CRT.
        Make sure that coordinates fall within real world coordinates system
        """

        LOGGER.debug(
            "SRC Extent: {}, {}, {}, {}".format(
                self.src.bounds.left,
                self.src.bounds.top,
                self.src.bounds.right,
                self.src.bounds.bottom,
            )
        )

        world_bounds = self._world_bounds()

        clamped_src_bounds = self._clamp_src_bounds(*world_bounds)

        return self._reproject_src_bounds(*clamped_src_bounds)

    def windows(self, dst: DatasetWriter) -> Iterator[Window]:
        """
        Divides raster source into larger windows which will still fit into memory
        """
        max_blocks: int = int(sqrt(self._max_blocks(dst)))
        x_blocks: int = int(dst.width / dst.block_shapes[0][0])
        y_blocks: int = int(dst.height / dst.block_shapes[0][1])

        for i in range(0, x_blocks, max_blocks):
            for j in range(0, y_blocks, max_blocks):
                max_i = min(i + max_blocks, x_blocks)
                max_j = min(j + max_blocks, y_blocks)
                yield self._windows(dst, i, j, max_i, max_j)

    def _world_bounds(self) -> Bounds:
        """
        Get world bounds in src CRT
        """
        proj = Transformer.from_crs(
            self.grid.srs, self.src.profile["crs"], always_xy=True
        )

        # Get World Extent in Source Projection
        # Important: We have to get each top, left, right, bottom seperately.
        # We cannot get them using the corner coordinates.
        # For some projections such as Goode (epsg:54052) this would cause strange behavior
        top = proj.transform(0, 90)[1]
        left = proj.transform(-180, 0)[0]
        bottom = proj.transform(0, -90)[1]
        right = proj.transform(180, 0)[0]

        LOGGER.debug("World Extent: {}, {}, {}, {}".format(left, bottom, right, top))

        return left, bottom, right, top

    def _clamp_src_bounds(self, left, bottom, right, top) -> Bounds:
        """
        Make sure src bounds are within world extent
        """

        # Crop SRC Bounds to World Extent:
        clamp_left = max(left, self.src.bounds.left)
        clamp_top = min(top, self.src.bounds.top)
        clamp_right = min(right, self.src.bounds.right)
        clamp_bottom = max(bottom, self.src.bounds.bottom)

        LOGGER.debug(
            "Cropped Extent: {}, {}, {}, {}".format(
                clamp_left, clamp_bottom, clamp_right, clamp_top
            )
        )

        return clamp_left, clamp_bottom, clamp_right, clamp_top

    def _reproject_src_bounds(
        self, left: float, bottom: float, right: float, top: float
    ) -> Bounds:
        """
        Reproject bounds to dst CRT
        """

        proj = Transformer.from_crs(
            self.src.profile["crs"], self.grid.srs, always_xy=True
        )

        reproject_top = proj.transform(0, top)[1]
        reproject_left = proj.transform(left, 0)[0]
        reproject_bottom = proj.transform(0, bottom)[1]
        reproject_right = proj.transform(right, 0)[0]

        LOGGER.debug(
            "Inverted Copped Extent: {}, {}, {}, {}".format(
                reproject_left, reproject_bottom, reproject_right, reproject_top
            )
        )

        return reproject_left, reproject_bottom, reproject_right, reproject_top

    @staticmethod
    def _block_has_data(array: MaskedArray) -> bool:
        """
        Check if current block has any data
        """
        msk = np.invert(array.mask.astype(bool))
        size = msk[msk].size
        LOGGER.debug(f"Block has {size} data pixels")
        return array.shape[0] > 0 and array.shape[1] > 0 and size != 0

    def _calc(self, array: MaskedArray, dst_window: Window) -> MaskedArray:
        """
        Apply user defined calculation on array
        """
        if self.layer.calc:
            LOGGER.debug(f"Update {dst_window} of tile {self.tile_id}")
            funcstr = (
                f"def f(A: MaskedArray) -> MaskedArray:\n    return {self.layer.calc}"
            )
            exec(funcstr, globals())
            array = f(array)  # type: ignore # noqa: F821
        else:
            LOGGER.debug(f"Nothing to update. Skip {dst_window} of tile {self.tile_id}")
        return array

    def _max_blocks(self, dst: DatasetWriter) -> int:
        """
        Calculate the maximum amount of blocks we can fit into memory,
        making sure that blocks can always fill a squared extent.
        We can only use half the available memory per process per block
        b/c we might have two copies of the array at the same time
        """

        max_bytes_per_block: float = self._max_block_size(dst) * self._max_itemsize()
        memory_per_block = utils.available_memory_per_process() / 4
        return floor(sqrt(memory_per_block / max_bytes_per_block)) ** 2

    def _max_block_size(self, dst: DatasetWriter) -> float:
        """
        Depending on projections, # of input pixels for output block can vary
        Here we take the corner blocks of the output tile and compare the pixel count with
        the pixel count in input tile, covered by each block extent
        We return the largest amount of pixels which are possibly covered by one block
        """
        width: float = dst.profile["width"]
        height: float = dst.profile["height"]
        blockxsize: int = dst.profile["blockxsize"]
        blockysize: int = dst.profile["blockysize"]

        ul: Window = dst.block_window(1, 0, 0)
        ur: Window = dst.block_window(1, 0, width / blockxsize - 1)
        ll: Window = dst.block_window(1, height / blockysize - 1, 0)
        lr: Window = dst.block_window(
            1, height / blockysize - 1, width / blockxsize - 1
        )

        dst_windows: List[Window] = [ul, ur, ll, lr]
        src_windows: List[Window] = [self._reproject_dst_window(w) for w in dst_windows]

        return max([w.width * w.height for w in dst_windows + src_windows])

    def _max_itemsize(self) -> int:
        """
        Check how many bytes one pixel of the largest datatype used will require
        """
        src_itemsize: int = np.zeros(1, dtype=self.src.profile["dtype"]).itemsize
        dst_itemsize: int = np.zeros(1, dtype=self.dst.profile["dtype"]).itemsize

        return max(src_itemsize, dst_itemsize)

    @retry(
        retry_on_exception=retry_if_rasterio_io_error,
        stop_max_attempt_number=7,
        wait_fixed=1000,
    )
    def _read_window(self, vrt: WarpedVRT, dst_window: Window) -> MaskedArray:
        """
            Read window of input raster
            """
        LOGGER.debug(f"Read {dst_window} for Tile {self.tile_id}")

        window = vrt.window(*bounds(dst_window, self.dst.profile["transform"]))
        return vrt.read(
            window=window, out_shape=(dst_window.width, dst_window.height), masked=True
        )

    def _vrt_transform(
        self, src_crs, dst_crs, west: float, south: float, east: float, north: float
    ) -> Tuple[rasterio.Affine, float, float]:
        """
        Compute Affine transformation, width and height for WarpedVRT using output CRS and pixel size
        """

        # LOGGER.debug(f"Input Bounds {west, south, east, north}")
        # west, south, east, north = rasterio.warp.transform_bounds(src_crs, dst_crs, west, south, east, north)
        LOGGER.debug(f"Output Bounds {west, south, east, north}")
        north, west = self._snap_coordinates(north, west)
        south, east = self._snap_coordinates(south, east)

        transform: rasterio.Affine = rasterio.transform.from_origin(
            west, north, self.grid.xres, self.grid.yres
        )
        width = (east - west) / self.grid.xres
        height = (north - south) / self.grid.yres

        return transform, width, height

    def _snap_coordinates(self, lat: float, lng: float) -> Tuple[float, float]:
        """
        Snap a given coordinate to tile grid coordinates.
        Always returns the closes coordinates to the top left of the input coordinates
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

    def _reproject_dst_window(self, dst_window: Window) -> Window:
        """
        Reproject window into same projection as source raster
        """

        dst_bounds: Bounds = bounds(
            window=dst_window,
            transform=self.dst.profile["transform"],
            height=self.grid.blockysize,
            width=self.grid.blockxsize,
        )
        src_bounds: Bounds = transform_bounds(
            self.dst.profile["crs"], self.src.profile["crs"], *dst_bounds
        )

        src_window: Window = from_bounds(
            *src_bounds,
            transform=self.src.profile["transform"],
            # width=self.src.profile["blockxsize"],
            # height=self.src.profile["blockysize"],
        )
        LOGGER.debug(
            f"Source window for {dst_window} of tile {self.tile_id} is {src_window}"
        )
        return src_window

    def _set_dtype(self, array: MaskedArray, dst_window) -> np.ndarray:
        """
        Update data type to desired output datatype
        Update nodata value to desired nodata value
        (current no data values will be updated and
        any values which already has new no data value will stay as is)
        """
        if self.dst.profile["nodata"] == 0 or self.dst.profile["nodata"]:
            LOGGER.debug(
                f"Set datatype and no data value for {dst_window} of tile {self.tile_id}"
            )
            array = np.ma.filled(array, self.dst.profile["nodata"]).astype(
                # TODO: Check if we still need np.asarray wrapper
                self.dst.profile["dtype"]
            )

        else:
            LOGGER.debug(f"Set datatype for {dst_window} of tile {self.tile_id}")
            array = array.data.astype(self.dst.profile["dtype"])
        return array

    @staticmethod
    def _windows(
        dst: DatasetWriter, min_i: int, min_j: int, max_i: int, max_j: int
    ) -> Window:
        """
        Loops over selected blocks of data source and merges their windows into one
        """
        windows: List[Window] = list()

        for i in range(min_i, max_i):
            for j in range(min_j, max_j):
                windows.append(dst.block_window(1, i, j))
        return union(*windows)

    def _write_window(
        self, dst: DatasetWriter, array: np.ndarray, dst_window: Window
    ) -> None:
        """
        Write blocks into output raster
        """
        LOGGER.debug(f"Write {dst_window} of tile {self.tile_id}")
        dst.write(array, window=dst_window)  # , indexes=1)
