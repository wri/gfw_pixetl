import math
from math import floor, sqrt
from typing import Iterator, List, Tuple

import numpy as np
import rasterio
from numpy.ma import MaskedArray
from pyproj import Transformer
from rasterio.coords import BoundingBox
from rasterio.io import DatasetWriter, DatasetReader
from rasterio.warp import transform_bounds, reproject
from rasterio.windows import Window, from_bounds, bounds, union
from retrying import retry
from shapely.geometry import Point

from gfw_pixetl import get_module_logger, utils
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

        proj = Transformer.from_crs(
            self.grid.srs, self.src.profile["crs"], always_xy=True
        )
        inverse = Transformer.from_crs(
            self.src.profile["crs"], self.grid.srs, always_xy=True
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
        left = max(world_left, self.src.bounds.left)
        top = min(world_top, self.src.bounds.top)
        right = min(world_right, self.src.bounds.right)
        bottom = max(world_bottom, self.src.bounds.bottom)

        # Convert back to Target Projection
        cropped_top = inverse.transform(0, top)[1]
        cropped_left = inverse.transform(left, 0)[0]
        cropped_bottom = inverse.transform(0, bottom)[1]
        cropped_right = inverse.transform(right, 0)[0]

        LOGGER.debug(
            "World Extent: {}, {}, {}, {}".format(
                world_left, world_top, world_right, world_bottom
            )
        )
        LOGGER.debug(
            "SRC Extent: {}, {}, {}, {}".format(
                self.src.bounds.left,
                self.src.bounds.top,
                self.src.bounds.right,
                self.src.bounds.bottom,
            )
        )
        LOGGER.debug("Cropped Extent: {}, {}, {}, {}".format(left, top, right, bottom))
        LOGGER.debug(
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

            has_data = False
            for dst_window in self.windows(dst):
                src_window, dst_window = self._reproject_window(dst_window)
                masked_array: MaskedArray = self._read_window(src, src_window)
                if self._block_has_data(masked_array):
                    masked_array = self._warp(masked_array, src_window, dst_window)
                    masked_array = self._calc(masked_array, dst_window)
                    array: np.ndarray = self._set_dtype(masked_array)
                    del masked_array
                    self._write_window(dst, array, dst_window)
                    del array
                    has_data = True
                else:
                    LOGGER.debug(f"{dst_window} has no data - skip")
                    del masked_array
            src.close()
            dst.close()
            self.set_local_src(stage)
            return has_data

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

    def _block_has_data(self, array: MaskedArray) -> bool:
        """
        Check if current block has any data
        """
        msk = array.mask.astype(bool)
        return array.shape[0] > 0 and array.shape[1] > 0 and msk[msk].size == 0

    def _calc(self, array: MaskedArray, dst_window: Window) -> MaskedArray:
        """
        Apply user defined calculation on array
        """
        if self.layer.calc:
            LOGGER.debug(f"Update {dst_window}")
            funcstr = (
                f"def f(A: MaskedArray) -> MaskedArray:\n    return {self.layer.calc}"
            )
            exec(funcstr, globals())
            array = f(array)  # type: ignore # noqa: F821
        else:
            LOGGER.debug(f"Nothing to update. Skip {dst_window}")
        return array

    def _max_blocks(self, dst: DatasetWriter) -> int:
        """
        Calculate the maximum amount of blocks we can fit into memory
        """

        max_bytes_per_block: float = self._max_block_size(dst) * self._max_itemsize()
        return (
            floor(sqrt(utils.available_memory_per_process() / max_bytes_per_block)) ** 2
        )

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
        src_windows: List[Window] = [self._reproject_window(w)[0] for w in dst_windows]

        return max([w.width * w.height for w in dst_windows + src_windows])

    def _max_itemsize(self) -> int:
        """
        Check how many bytes one pixel of the largest datatype used will require
        """
        src_itemsize: int = np.zeros(1, dtype=self.src.profile["dtype"]).itemsize
        dst_itemsize: int = np.zeros(1, dtype=self.dst.profile["dtype"]).itemsize

        return max(src_itemsize, dst_itemsize)

    @staticmethod
    @retry(
        retry_on_exception=retry_if_rasterio_io_error,
        stop_max_attempt_number=7,
        wait_fixed=1000,
    )
    def _read_window(src: DatasetReader, src_window: Window) -> MaskedArray:
        """
        Read window of input raster
        """
        return src.read(1, window=src_window, masked=True)

    def _reproject_window(self, dst_window: Window) -> Windows:
        """
        Reproject window into same projectionas source raster
        """
        LOGGER.debug(f"Reproject {dst_window}")
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

        return src_window, dst_window

    def _set_dtype(self, array: MaskedArray) -> np.ndarray:
        """
        Update data type to desired output datatype
        Update nodata value to desired nodata value
        (current no data values will be updated and
        any values which already has new no data value will stay as is)
        """
        if self.dst.profile["nodata"] == 0 or self.dst.profile["nodata"]:
            array = np.asarray(np.ma.filled(array, self.dst.profile["nodata"])).astype(
                # TODO: Check if we still need np.asarray wrapper
                self.dst.profile["dtype"]
            )

        else:
            array = np.asarray(array.data).astype(self.dst.profile["dtype"])
        return array

    def _warp(self, array: MaskedArray, src_window: Window, dst_window: Window):
        """
        Reproject and resample input array to output projection and output resolution
        """
        src_transform = rasterio.windows.transform(
            src_window, self.src.profile["transform"]
        )
        dst_transform = rasterio.windows.transform(
            dst_window, self.dst.profile["transform"]
        )

        if (
            not math.isclose(src_transform[0], dst_transform[0])
            or not math.isclose(src_transform[4], dst_transform[4])
            or self.src.profile["crs"] != self.dst.profile["crs"]
        ):
            LOGGER.debug(f"Warp {dst_window}")
            warped_array = np.ma.empty(
                (dst_window.width, dst_window.height), self.src.profile["dtype"],
            )

            reproject(
                source=array,
                destination=warped_array,
                src_transform=src_transform,
                src_crs=self.src.profile["crs"],
                # src_nodata=self.src.profile["nodata"],
                dst_transform=dst_transform,
                dst_crs=self.dst.profile["crs"],
                # dst_nodata=self.dst.profile["nodata"],
                # dst_resolution=(self.grid.xres, self.grid.yres),
                resampling=self.layer.resampling,
                warp_mem_limit=max(warped_array.nbytes / 1000, array.nbytes / 1000),
            )
            return warped_array
        else:
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
        LOGGER.debug(f"Write {dst_window}")
        dst.write(array, window=dst_window, indexes=1)

    # def transform(self) -> bool:
    #     """
    #     Write input data to output tile
    #     """
    #
    #     stage = "transform"
    #     dst_uri = self.get_stage_uri(stage)
    #
    #     LOGGER.info(f"Transform tile {self.tile_id}")
    #     with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
    #
    #         src: DatasetReader = rasterio.open(self.src.uri)
    #         dst: DatasetWriter = rasterio.open(dst_uri, "w+", **self.dst.profile)
    #
    #     pipe = (
    #             self.windows(dst)
    #             # dst.block_windows(1)
    #             | Stage(self._reproject_window_stage).setup(
    #         workers=self.workers, qsize=self.workers
    #     )
    #             | Stage(  # get destination blocks, then read from source
    #         self._read_window_stage, src
    #     ).setup(workers=self.workers, qsize=self.workers)
    #             | Stage(self._drop_empty_blocks_stage).setup(
    #         workers=self.workers, qsize=self.workers
    #     )
    #             | Stage(self._warp_stage).setup(
    #         workers=self.workers, qsize=self.workers
    #     )
    #             | Stage(self._calc_stage).setup(
    #         workers=self.workers, qsize=self.workers
    #     )
    #             | Stage(self._set_dtype_stage).setup(
    #         workers=self.workers, qsize=self.workers
    #     )
    #     )
    #
    #     has_blocks = False
    #     for array, windows in pipe.results():
    #         LOGGER.debug(f"Write {windows[1]}")
    #         dst.write(array, window=windows[1], indexes=1)
    #         has_blocks = True
    #
    #     src.close()
    #     dst.close()
    #     self.set_local_src(stage)
    #     return has_blocks
    #
    # def _reproject_window_stage(self, windows: Iterator[Window]) -> Iterator[Windows]:
    #     """
    #     Stage to reproject each dst_window into src project, so that we can read the required input data
    #     """
    #     for dst_window in windows:
    #         yield self._reproject_window(dst_window)
    #
    #
    # def _read_window_stage(
    #         self, windows: Iterator[Windows], src: rasterio.DatasetReader
    # ) -> Iterator[Tuple[np.ndarray, Windows]]:
    #     """
    #     Stage to read input data
    #     """
    #     for src_window, dst_window in windows:
    #         yield self._read_window(src, src_window), (src_window, dst_window)
    #
    #
    # def _drop_empty_blocks_stage(self,
    #                              arrays: Iterator[Tuple[MaskedArray, Windows]]
    #                              ) -> Iterator[Tuple[MaskedArray, Windows]]:
    #     """
    #     Stage to drop windows in case they are empty
    #     """
    #     for array, windows in arrays:
    #         if self._block_has_data(array):
    #             LOGGER.debug(f"{windows[1]} has data")
    #             yield array, windows
    #         else:
    #             LOGGER.debug(f"{windows[1]} has no data - DROP")
    #
    #
    # def _warp_stage(
    #         self, arrays: Iterator[Tuple[MaskedArray, Windows]]
    # ) -> Iterator[Tuple[MaskedArray, Windows]]:
    #     """
    #     Stage to reproject and resample input data for that they match output projection and resolution
    #     """
    #
    #     for array, windows in arrays:
    #         array = self._warp(array, *windows)
    #         yield array, windows
    #
    #
    # def _calc_stage(
    #         self, arrays: Iterator[Tuple[MaskedArray, Windows]]
    # ) -> Iterator[Tuple[MaskedArray, Windows]]:
    #     """
    #     Update pixel values using user defined formular
    #     """
    #     for array, windows in arrays:
    #         array = self._calc(array, windows[1])
    #         yield array, windows
    #
    #
    # def _set_dtype_stage(
    #         self, arrays: Iterator[Tuple[MaskedArray, Windows]]
    # ) -> Iterator[Tuple[Array, Windows]]:
    #     """
    #     Set final datatype and update no data values
    #     """
    #     for array, windows in arrays:
    #         LOGGER.debug(f"Set datatype for {windows[1]}")
    #         yield self._set_dtype(array), windows
