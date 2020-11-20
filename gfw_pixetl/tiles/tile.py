import copy
import errno
import json
import os
import subprocess as sp
from abc import ABC
from typing import Any, Dict, List, Tuple

import rasterio
from rasterio.coords import BoundingBox
from rasterio.crs import CRS
from rasterio.shutil import copy as raster_copy

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.errors import GDALError
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import Layer
from gfw_pixetl.models import ColorMapType
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
            "gdal-geotiff": Destination(
                uri=os.path.join(layer.prefix, "gdal-geotiff", f"{self.tile_id}.tif"),
                profile=gdal_profile,
                bounds=self.bounds,
            ),
            "geotiff": Destination(
                uri=os.path.join(layer.prefix, "geotiff", f"{self.tile_id}.tif"),
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

        self.default_format = "geotiff"
        self.status = "pending"
        self.stats: Dict[str, Dict] = dict()

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
        dst_format = "gdal-geotiff"
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
            self.stats[dst_format] = self.local_dst[dst_format].stats()

    def add_symbology(self):
        symbology_constructor = {
            ColorMapType.discrete: self._add_discrete_symbology,
            ColorMapType.gradient: self._add_gradient_symbology,
        }

        symbology_constructor[self.layer.symbology.type]()

    def _add_discrete_symbology(self):
        _colormap = self.layer.symbology.colormap
        colormap = dict()
        for pixel_value in _colormap:
            colormap[pixel_value] = tuple(_colormap[pixel_value].dict().values())

        with rasterio.open(
            self.local_dst[self.default_format].uri,
            "r+",
            **self.local_dst[self.default_format].profile,
        ) as dst:
            dst.write_colormap(colormap)

    def _add_gradient_symbology(self):
        colormap = os.path.join(self.tmp_dir, "colormap.txt")
        with open(colormap, "w") as f:
            for k, v in self.layer.symbology.colormap.items():
                values = [k, *v.dict().values()]
                f.write(" ".join(str(values)))
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
            src,
            colormap,
            dst,
        ]
        try:
            run_gdal_subcommand(cmd)
        except GDALError:
            LOGGER.error("Could not create Color Relief")
            raise
        # switch uri with new output file
        self.local_dst[self.default_format].uri = dst
