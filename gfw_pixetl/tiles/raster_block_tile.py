from typing import Iterator, Tuple

import numpy as np
import rasterio
from parallelpipe import Stage
from pyproj import Transformer
from rasterio.windows import Window
from rasterio.coords import BoundingBox
from shapely.geometry import Point

from gfw_pixetl import get_module_logger
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.tiles import Tile

LOGGER = get_module_logger(__name__)


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

    def transform(self) -> None:
        stage = "update_values"
        dst_uri = self.get_stage_uri(stage)

        with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
            src = rasterio.open(self.src.uri)
            dst = rasterio.open(dst_uri, "w+", **self.dst.profile)

            pipe = (
                dst.block_windows(1)
                | Stage(  # get destination blocks, then read from source
                    self._read_window, src
                ).setup(workers=self.workers)
                | Stage(self._drop_empty_blocks).setup(workers=self.workers)
                | Stage(self._warp).setup(workers=self.workers)
                | Stage(self._calc).setup(workers=self.workers)
                | Stage(self._set_dtype).setup(workers=self.workers)
            )

            for array, window in pipe.results():
                dst.write(array, window=window)

            src.close()
            dst.close()

    @staticmethod
    def _read_window(
        windows: Iterator[Tuple[np.ndarray, Window]], src: rasterio.DatasetReader
    ) -> Iterator[Tuple[np.ndarray, Window]]:
        for block_index, window in windows:
            yield src.read(window=window, masked=True), window

    @staticmethod
    def _drop_empty_blocks(
        arrays: Iterator[Tuple[np.ndarray, Window]]
    ) -> Iterator[Tuple[np.ndarray, Window]]:
        for array, window in arrays:
            msk = array.mask.astype(bool)
            if msk[msk].size == 0:
                yield array, window

    def _warp(
        self, arrays: Iterator[Tuple[np.ndarray, Window]]
    ) -> Iterator[Tuple[np.ndarray, Window]]:

        src_transform = self.src.profile["transform"]
        src_crs = self.src.profile["crs"]
        src_nodata = self.src.profile["nodata"]

        dst_transform = self.dst.profile["transform"]
        dst_crs = self.dst.profile["crs"]
        dst_nodata = self.dst.profile["nodata"]

        resolution = (self.grid.xres, self.grid.yres)
        resampling = self.layer.resampling

        for array, window in arrays:
            data, dst_transform = rasterio.warp.reproject(
                source=array,
                src_transform=src_transform,
                src_crs=src_crs,
                src_nodata=src_nodata,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                dst_nodata=dst_nodata,
                dst_resolution=resolution,
                src_alpha=0,
                dst_alpha=0,
                resampling=resampling,
                num_threads=1,
                init_dest_nodata=True,
                warp_mem_limit=0,
            )
            yield data, window

    def _calc(
        self, arrays: Iterator[Tuple[np.ndarray, Window]]
    ) -> Iterator[Tuple[np.ndarray, Window]]:

        for array, window in arrays:
            if self.layer.calc:
                funcstr = (
                    f"def f(A: np.ndarray) -> np.ndarray:\n    return {self.layer.calc}"
                )
                exec(funcstr, globals())
                yield f(array), window  # type: ignore # noqa: F821
            else:
                yield array, window

    def _set_dtype(
        self, arrays: Iterator[Tuple[np.ndarray, Window]]
    ) -> Iterator[Tuple[np.ndarray, Window]]:

        dst_dtype = self.dst.profile["dtype"]
        dst_nodata = self.dst.profile["nodata"]
        for array, window in arrays:

            # update no data value if wanted
            if dst_nodata == 0 or dst_nodata:
                array = np.ma.filled(array, dst_nodata).astype(dst_dtype)

            else:
                array = array.data.astype(dst_dtype)

            yield array, window
