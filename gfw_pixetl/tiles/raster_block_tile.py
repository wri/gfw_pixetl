import math
import multiprocessing
from math import floor, sqrt
from typing import Iterator, List, Tuple

import numpy as np
import psutil
import rasterio
from parallelpipe import Stage
from pyproj import Transformer
from rasterio.warp import Resampling, transform_bounds
from rasterio.windows import Window, from_bounds, bounds, union
from rasterio.coords import BoundingBox
from rasterio.warp import reproject
from retrying import retry
from shapely.geometry import Point

from gfw_pixetl import get_module_logger
from gfw_pixetl.errors import retry_if_rasterio_io_error
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.tiles import Tile

LOGGER = get_module_logger(__name__)

Windows = Tuple[Window, Window]
Bounds = Tuple[float, float, float, float]


class RasterBlockTile(Tile):
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

    def transform(self) -> None:
        """
        Write input data to output tile
        """

        stage = "transform"
        dst_uri = self.get_stage_uri(stage)

        with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
            src = rasterio.open(self.src.uri)
            dst = rasterio.open(dst_uri, "w+", **self.dst.profile)

            pipe = (
                self.windows(dst)
                # dst.block_windows(1)
                | Stage(self._reproject_window_stage).setup(
                    workers=self.workers, qsize=self.workers
                )
                | Stage(  # get destination blocks, then read from source
                    self._read_window_stage, src
                ).setup(workers=self.workers, qsize=self.workers)
                | Stage(self._drop_empty_blocks_stage).setup(
                    workers=self.workers, qsize=self.workers
                )
                | Stage(self._warp_stage).setup(
                    workers=self.workers, qsize=self.workers
                )
                | Stage(self._calc_stage).setup(
                    workers=self.workers, qsize=self.workers
                )
                | Stage(self._set_dtype_stage).setup(
                    workers=self.workers, qsize=self.workers
                )
            )

            for array, windows in pipe.results():
                LOGGER.debug(f"Write {windows[1]}")
                dst.write(array, window=windows[1], indexes=1)

            src.close()
            dst.close()
            self.set_local_src(stage)

    def windows(self, dst: rasterio.DatasetReader) -> Iterator[Window]:
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

    def _reproject_window_stage(self, windows: Iterator[Window]) -> Iterator[Windows]:
        """
        Stage to reproject each dst_window into src project, so that we can read the required input data
        """
        for dst_window in windows:
            yield self._reproject_window(dst_window)

    def _read_window_stage(
        self, windows: Iterator[Windows], src: rasterio.DatasetReader
    ) -> Iterator[Tuple[np.ndarray, Windows]]:
        """
        Stage to read input data
        """
        for src_window, dst_window in windows:
            yield self._read_window(src, src_window), (src_window, dst_window)

    @staticmethod
    def _drop_empty_blocks_stage(
        arrays: Iterator[Tuple[np.ndarray, Windows]]
    ) -> Iterator[Tuple[np.ndarray, Windows]]:
        """
        Stage to drop windows in case they are empty
        """
        for array, windows in arrays:
            msk = array.mask.astype(bool)
            if array.shape[0] > 0 and array.shape[1] > 0 and msk[msk].size == 0:
                LOGGER.debug(f"{windows[1]} has data")
                yield array, windows
            else:
                LOGGER.debug(f"{windows[1]} has no data - DROP")

    def _warp_stage(
        self, arrays: Iterator[Tuple[np.ndarray, Windows]]
    ) -> Iterator[Tuple[np.ndarray, Windows]]:
        """
        Stage to reproject and resample input data for that they match output projection and resolution
        """

        for array, windows in arrays:

            src_transform = rasterio.windows.transform(
                windows[0], self.src.profile["transform"]
            )
            dst_transform = rasterio.windows.transform(
                windows[1], self.dst.profile["transform"]
            )

            if (
                # not
                math.isclose(src_transform[0], dst_transform[0])
                or not math.isclose(src_transform[4], dst_transform[4])
                or self.src.profile["crs"] != self.dst.profile["crs"]
            ):
                LOGGER.debug(f"Reproject {windows[1]}")
                data = np.empty(
                    (windows[1].width, windows[1].height), self.src.profile["dtype"],
                )

                reproject(
                    source=array,
                    destination=data,
                    src_transform=src_transform,
                    src_crs=self.src.profile["crs"],
                    # src_nodata=self.src.profile["nodata"],
                    dst_transform=dst_transform,
                    dst_crs=self.dst.profile["crs"],
                    # dst_nodata=self.dst.profile["nodata"],
                    # dst_resolution=(self.grid.xres, self.grid.yres),
                    resampling=Resampling.nearest,
                    # self.layer.resampling # TODO: change self.resampling to rasterio class
                    warp_mem_limit=max(data.nbytes / 1000, array.nbytes / 1000),
                )
                yield data, windows

            else:
                LOGGER.debug(f"Input CRS and resolution are identical for {windows[1]}")
                yield array, windows

    def _calc_stage(
        self, arrays: Iterator[Tuple[np.ndarray, Windows]]
    ) -> Iterator[Tuple[np.ndarray, Windows]]:
        """
        Update pixel values using user defined formular
        """
        for array, windows in arrays:
            if self.layer.calc:
                LOGGER.debug(f"Update {windows[1]}")
                funcstr = (
                    f"def f(A: np.ndarray) -> np.ndarray:\n    return {self.layer.calc}"
                )
                exec(funcstr, globals())
                yield f(array), windows  # type: ignore # noqa: F821
            else:
                LOGGER.debug(f"Nothing to update. Skip {windows[1]}")
                yield array, windows

    def _set_dtype_stage(
        self, arrays: Iterator[Tuple[np.ndarray, Windows]]
    ) -> Iterator[Tuple[np.ndarray, Windows]]:
        """
        Set final datatype and update no data values
        """
        dst_dtype = self.dst.profile["dtype"]
        dst_nodata = self.dst.profile["nodata"]
        for array, windows in arrays:
            LOGGER.debug(f"Set datatype for {windows[1]}")
            # update no data value if wanted
            if dst_nodata == 0 or dst_nodata:
                array = np.asarray(np.ma.filled(array, dst_nodata)).astype(dst_dtype)

            else:
                array = np.asarray(array.data).astype(dst_dtype)

            yield array, windows

    @retry(
        retry_on_exception=retry_if_rasterio_io_error,
        stop_max_attempt_number=7,
        wait_fixed=1000,
    )
    def _read_window(self, src, src_window):
        return src.read(1, window=src_window, masked=True)

    def _reproject_window(self, dst_window: Window) -> Windows:

        LOGGER.debug(f"Reproject {dst_window}")
        dst_bounds: Bounds = bounds(
            window=dst_window,
            # transform=rasterio.windows.transform(
            #     dst_window, self.dst.profile["transform"]
            # ),
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

    def _max_block_size(self, dst: rasterio.DatasetReader) -> float:
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
    def _available_memory_per_process(divisor=1) -> float:
        """
        Snapshot of currently available memory per core
        """
        available_memory: int = psutil.virtual_memory()[1]
        processes: int = max(floor(multiprocessing.cpu_count() / divisor), 1)

        return available_memory / processes

    def _max_blocks(self, dst: rasterio.DatasetReader) -> int:
        """
        Calculate the maximum amount of blocks we can fit into memory
        """

        max_bytes_per_block: float = self._max_block_size(dst) * self._max_itemsize()
        return (
            floor(sqrt(self._available_memory_per_process() / max_bytes_per_block)) ** 2
        )

    def _windows(self, dst, min_i, min_j, max_i, max_j) -> Window:
        """
        Loops over selected blocks of data source and merges their windows into one
        """
        windows: List[Window] = list()

        for i in range(min_i, max_i):
            for j in range(min_j, max_j):
                windows.append(dst.block_window(1, i, j))
        return union(*windows)
