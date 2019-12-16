from typing import List

import numpy as np
import rasterio
from rasterio.coords import BoundingBox
from pyproj import Transformer
from shapely.geometry import Point

from gfw_pixetl import get_module_logger
from gfw_pixetl.data_type import to_gdal_dt
from gfw_pixetl.errors import GDALError
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.tiles import Tile


logger = get_module_logger(__name__)


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

        logger.debug(
            "World Extent: {}, {}, {}, {}".format(
                world_left, world_top, world_right, world_bottom
            )
        )
        logger.debug(
            "SRC Extent: {}, {}, {}, {}".format(
                self.src.bounds.left,
                self.src.bounds.top,
                self.src.bounds.right,
                self.src.bounds.bottom,
            )
        )
        logger.debug("Cropped Extent: {}, {}, {}, {}".format(left, top, right, bottom))
        logger.debug(
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

    def transform(self, is_final=True) -> None:
        stage = "transform"
        dst = self.get_stage_uri(stage)

        cmd: List[str] = ["gdalwarp"]

        if is_final:
            cmd += self._is_final_cmd()

        cmd += [
            "-s_srs",
            self.src.profile["crs"].to_proj4(),
            "-t_srs",
            self.grid.srs.srs,
            "-tr",
            str(self.grid.xres),
            str(self.grid.yres),
            "-te",
            str(self.bounds.left),
            str(self.bounds.bottom),
            str(self.bounds.right),
            str(self.bounds.top),
            "-te_srs",
            self.grid.srs.srs,
            "-ovr",
            "NONE",
            "-co",
            f"COMPRESS=NONE",  # {self.data_type.compression}",
            "-co",
            "TILED=YES",
            "-co",
            f"BLOCKXSIZE={self.grid.blockxsize}",
            "-co",
            f"BLOCKYSIZE={self.grid.blockysize}",
            # "-co", "SPARSE_OK=TRUE",
            "-r",
            self.layer.resampling,
            "-q",
            "-overwrite",
            self.src.uri,
            dst,
        ]

        logger.info(f"Transform tile {self.tile_id}")

        try:
            self._run_gdal_subcommand(cmd)
        except GDALError as e:
            logger.error(f"Could not transform file {dst}")
            logger.exception(e)
            raise
        else:
            self.set_local_src(dst)

    def compress(self):
        stage = "compress"
        dst = self.get_stage_uri(stage)

        cmd = [
            "gdal_translate",
            "-co",
            f"COMPRESS={self.dst.profile['compression']}",
            self.local_src.uri,
            dst,
        ]

        logger.info(f"Compress tile {self.tile_id}")

        try:
            self._run_gdal_subcommand(cmd)
        except GDALError as e:
            logger.error(f"Could not compress file {dst}")
            logger.exception(e)
            raise
        else:
            self.set_local_src(stage)

    def update_values(self):
        stage = "update_values"
        dst = self.get_stage_uri(stage)

        with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
            src = rasterio.open(self.local_src.uri)

            dst = rasterio.open(dst, "w", **self.dst.profile)

            for block_index, window in src.block_windows(1):
                data = src.read(window=window, masked=True)
                data = self._apply_calc(data)
                data = self._set_no_data_calc(data)
                dst.write(data, window=window)
            src.close()
            dst.close()

    def _is_final_cmd(self):

        cmd = ["-ot", to_gdal_dt(self.dst.profile["dtype"])]

        if "pixeltype" in self.dst.profile:
            cmd += ["-co", f"PIXELTYPE={self.dst.profile['pixeltype']}"]

        if "nbits" in self.dst.profile:
            cmd += ["-co", f"NBITS={self.dst.profile['nbits']}"]

        if self._dst_has_no_data():
            cmd += ["-dstnodata", str(self.dst.profile["nodata"])]

        return cmd

    def _apply_calc(
        self, data
    ):  # can use type hints here b/c of the way we create function f from string. Mypy would thow an error
        # apply user submitted calculation

        funcstr = f"def f(A: np.ndarray) -> np.ndarray:\n    return {self.layer.calc}"
        exec(funcstr, globals())
        return f(data)  # noqa: F821

    def _set_no_data_calc(self, data):
        # update no data value if wanted
        if self._dst_has_no_data():
            data = np.ma.filled(data, self.dst.profile["nodata"]).astype(
                self.dst.profile["data_type"]
            )

        else:
            data = data.data.astype(self.dst.profile["data_type"])
        return data
