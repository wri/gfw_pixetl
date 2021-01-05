import copy
import errno
import os
from abc import ABC
from typing import Dict, Union

import rasterio
from rasterio.coords import BoundingBox
from rasterio.crs import CRS
from rasterio.shutil import copy as raster_copy

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.errors import GDALError
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import Layer
from gfw_pixetl.models import RGBA, ColorMapType, DstFormat, OrderedColorMap
from gfw_pixetl.settings import GLOBALS
from gfw_pixetl.sources import Destination, RasterSource
from gfw_pixetl.utils.aws import get_s3_client
from gfw_pixetl.utils.gdal import run_gdal_subcommand

LOGGER = get_module_logger(__name__)
S3 = get_s3_client()


class Tile(ABC):
    """A tile object which represents a single tile within a given grid."""

    def __str__(self):
        return self.tile_id

    def __repr__(self):
        return f"Tile(tile_id={self.tile_id}, grid={self.grid.name})"

    def __hash__(self):
        return hash((self.tile_id, self.grid))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.tile_id == other.tile_id and self.grid == other.grid

    def __init__(self, tile_id: str, grid: Grid, layer: Layer) -> None:

        self.grid: Grid = grid
        self.layer: Layer = layer

        self.local_dst: Dict[str, RasterSource] = dict()

        self.tile_id: str = tile_id
        self.bounds: BoundingBox = grid.get_tile_bounds(tile_id)

        gdal_profile = {
            "driver": "GTiff",
            "width": grid.cols,
            "height": grid.rows,
            "count": 1,
            "transform": rasterio.transform.from_origin(
                self.bounds.left, self.bounds.top, grid.xres, grid.yres
            ),
            "crs": CRS.from_string(
                grid.crs.to_string()
            ),  # Need to convert from ProjPy CRS to RasterIO CRS
            "sparse_ok": "TRUE",
            "interleave": "BAND",
        }
        gdal_profile.update(self.layer.dst_profile)

        # Drop GDAL specific optimizations which might not be readable by other applications
        geotiff_profile = copy.deepcopy(gdal_profile)
        geotiff_profile.pop("nbits", None)
        geotiff_profile.pop("sparse_ok", None)
        geotiff_profile.pop("interleave", None)
        geotiff_profile["compress"] = "DEFLATE"

        self.dst: Dict[str, Destination] = {
            DstFormat.gdal_geotiff: Destination(
                uri=os.path.join(
                    layer.prefix, DstFormat.gdal_geotiff, f"{self.tile_id}.tif"
                ),
                profile=gdal_profile,
                bounds=self.bounds,
            ),
            DstFormat.geotiff: Destination(
                uri=os.path.join(
                    layer.prefix, DstFormat.geotiff, f"{self.tile_id}.tif"
                ),
                profile=geotiff_profile,
                bounds=self.bounds,
            ),
        }

        self.tmp_dir = os.path.join(layer.prefix, "tmp")
        try:
            os.makedirs(self.tmp_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        self.default_format = GLOBALS.default_dst_format
        self.status = "pending"
        self.metadata: Dict[str, Dict] = dict()

    def set_local_dst(self, dst_format) -> None:
        if hasattr(self, "local_src"):
            self.rm_local_src(dst_format)

        uri = self.get_local_dst_uri(dst_format)
        LOGGER.debug(f"Set Local Source URI: {uri}")
        self.local_dst[dst_format] = RasterSource(uri)

    def get_local_dst_uri(self, dst_format) -> str:

        prefix = f"{self.layer.prefix}/{dst_format}"
        LOGGER.debug(f"Attempt to create local folder {prefix} if not already exists")
        os.makedirs(f"{prefix}", exist_ok=True)

        uri = os.path.join(prefix, f"{self.tile_id}.tif")

        LOGGER.debug(f"Local Source URI: {uri}")
        return uri

    def create_gdal_geotiff(self) -> None:
        dst_format = DstFormat.gdal_geotiff
        if self.default_format != dst_format:
            LOGGER.info(
                f"Create copy of local file as Gdal Geotiff for tile {self.tile_id}"
            )

            raster_copy(
                self.local_dst[self.default_format].uri,
                self.get_local_dst_uri(dst_format),
                strict=False,
                **self.dst[dst_format].profile,
            )
            self.set_local_dst(dst_format)
        else:
            LOGGER.warning(
                f"Local file already Gdal Geotiff. Skip copying as Gdal Geotiff for tile {self.tile_id}"
            )

    def upload(self) -> None:

        try:
            for dst_format in self.local_dst.keys():
                LOGGER.info(f"Upload {dst_format} tile {self.tile_id} to s3")
                S3.upload_file(
                    self.local_dst[dst_format].uri,
                    utils.get_bucket(),
                    self.dst[dst_format].uri,
                )
        except Exception as e:
            LOGGER.error(f"Could not upload file {self.tile_id}")
            LOGGER.exception(str(e))
            self.status = "failed"

    def rm_local_src(self, dst_format) -> None:
        if dst_format in self.local_dst.keys() and os.path.isfile(
            self.local_dst[dst_format].uri
        ):
            LOGGER.info(f"Delete local file {self.local_dst[dst_format].uri}")
            os.remove(self.local_dst[dst_format].uri)

    def postprocessing(self):
        """Once we have the final geotiff, all postprocessing steps should be
        the same no matter the source format and grid type."""

        if self.layer.symbology:
            self.add_symbology()

        # Add superior compression, which only works with GDAL drivers
        self.create_gdal_geotiff()

        # Compute stats and histogram
        for dst_format in self.local_dst.keys():
            self.metadata[dst_format] = self.local_dst[dst_format].metadata(
                self.layer.compute_stats, self.layer.compute_histogram
            )

    def add_symbology(self):
        """Add symbology to output raster.

        Gradient colormap: Use linear interpolation based on provided
        colormap to compute RGBA quadruplet for any given pixel value.
        Discrete colormap: Use strict matching when searching in the
        color configuration file. If none matching color entry is found,
        the “0,0,0,0” RGBA quadruplet will be used.
        """

        LOGGER.info(f"Create RGBA raster for tile {self.tile_id}")

        ordered_colormap: OrderedColorMap = self._sort_colormap()
        colormap_file = os.path.join(self.tmp_dir, "colormap.txt")

        # write to file
        with open(colormap_file, "w") as f:
            for pixel_value in ordered_colormap:
                values = [str(pixel_value)] + [
                    str(i) for i in ordered_colormap[pixel_value]
                ]
                row = " ".join(values)
                f.write(row)
                f.write("\n")

        src = self.local_dst[self.default_format].uri
        dst = os.path.join(self.tmp_dir, f"{self.tile_id}_colored.tif")
        cmd = [
            "gdaldem",
            "color-relief",
            "-alpha",
            "-co",
            f"COMPRESS={self.dst[self.default_format].compress}",
            "-co",
            "TILED=YES",
            "-co",
            f"BLOCKXSIZE={self.grid.blockxsize}",
            "-co",
            f"BLOCKYSIZE={self.grid.blockxsize}",
            "-co",
            "SPARSE_OK=TRUE",
            "-co",
            "INTERLEAVE=BAND",
        ]

        if self.layer.symbology.type == ColorMapType.discrete:
            cmd += ["-exact_color_entry"]

        cmd += [src, colormap_file, dst]

        try:
            run_gdal_subcommand(cmd)
        except GDALError:
            LOGGER.error("Could not create Color Relief")
            raise
        # switch uri with new output file
        self.local_dst[self.default_format].uri = dst

    def _sort_colormap(self) -> OrderedColorMap:
        """
        Create value - quadruplet colormap (GDAL format) including no data value.

        """
        assert self.layer.symbology, "No colormap specified."
        colormap: Dict[Union[int, float], RGBA] = copy.deepcopy(
            self.layer.symbology.colormap
        )

        ordered_gdal_colormap: OrderedColorMap = dict()

        # add no data value to colormap, if exists
        # (not sure why mypy throws an error here, hence type: ignore)
        if self.dst[self.default_format].nodata is not None:
            colormap[self.dst[self.default_format].nodata] = RGBA(red=0, green=0, blue=0, alpha=0)  # type: ignore

        # make sure values are correctly sorted and convert to value-quadruplet string
        for pixel_value in sorted(colormap.keys()):

            ordered_gdal_colormap[pixel_value] = colormap[pixel_value].tuple()

        return ordered_gdal_colormap
