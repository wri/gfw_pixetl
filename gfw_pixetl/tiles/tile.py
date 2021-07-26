import copy
import os
import shutil
from abc import ABC
from typing import Dict

import rasterio
from rasterio.coords import BoundingBox
from rasterio.crs import CRS

from gfw_pixetl import get_module_logger
from gfw_pixetl.decorators import SubprocessKilledError
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import Layer
from gfw_pixetl.models.enums import DstFormat
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.sources import Destination, RasterSource
from gfw_pixetl.utils import get_bucket
from gfw_pixetl.utils.aws import upload_s3
from gfw_pixetl.utils.gdal import just_copy_to_gdal_geotiff
from gfw_pixetl.utils.path import create_dir

LOGGER = get_module_logger(__name__)

stats_ext = ".aux.xml"  # Extension of stats sidecar gdalinfo -stats creates


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
            "count": self.layer.band_count,
            "transform": rasterio.transform.from_origin(
                self.bounds.left, self.bounds.top, grid.xres, grid.yres
            ),
            "crs": CRS.from_string(
                grid.crs.to_string()
            ),  # Need to convert from ProjPy CRS to RasterIO CRS
            "sparse_ok": "TRUE",
            "interleave": "BAND",
        }
        if layer.photometric:
            gdal_profile[
                "photometric"
            ] = layer.photometric.value  # need value, not just Enum!

        gdal_profile.update(self.layer.dst_profile)

        LOGGER.debug(f"GDAL Profile for tile {self.tile_id}: {gdal_profile}")

        # Drop GDAL specific optimizations which might not be readable by other applications
        geotiff_profile = copy.deepcopy(gdal_profile)
        geotiff_profile.pop("nbits", None)
        geotiff_profile.pop("sparse_ok", None)
        geotiff_profile.pop("interleave", None)
        geotiff_profile["compress"] = "DEFLATE"

        LOGGER.debug(f"GEOTIFF Profile for tile {self.tile_id}: {geotiff_profile}")

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

        self.work_dir = create_dir(os.path.join(os.getcwd(), tile_id))
        self.tmp_dir = create_dir(os.path.join(self.work_dir, "tmp"))

        self.default_format = GLOBALS.default_dst_format
        self.status = "pending"
        self.metadata: Dict[str, Dict] = dict()

    def remove_work_dir(self):
        LOGGER.debug(f"Delete working directory for tile {self.tile_id}")
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def set_local_dst(self, dst_format) -> None:
        if hasattr(self, "local_src"):
            self.rm_local_src(dst_format)

        uri = self.get_local_dst_uri(dst_format)
        LOGGER.debug(f"Set Local Source URI: {uri}")
        self.local_dst[dst_format] = RasterSource(uri)

    def get_local_dst_uri(self, dst_format) -> str:

        prefix = f"{self.work_dir}/{dst_format}"
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

            just_copy_to_gdal_geotiff(
                self.local_dst[self.default_format].uri,
                self.get_local_dst_uri(dst_format),
                self.dst[dst_format].profile,
            )
            self.set_local_dst(dst_format)
        else:
            LOGGER.warning(
                f"Local file already Gdal Geotiff. Skip copying as Gdal Geotiff for tile {self.tile_id}"
            )

    def upload(self) -> None:
        try:
            bucket = get_bucket()
            for dst_format in self.local_dst.keys():
                local_tiff_path = self.local_dst[dst_format].uri
                LOGGER.info(f"Upload {local_tiff_path} to s3")
                _ = upload_s3(
                    local_tiff_path,
                    bucket,
                    self.dst[dst_format].uri,
                )
                # Also upload the stats sidecar file that gdalinfo creates
                # Use the default format for path because we only create 1 sidecar
                local_stats_path = self.local_dst[self.default_format].uri + stats_ext
                if os.path.isfile(local_stats_path):
                    LOGGER.info(f"Upload {local_stats_path} to s3")
                    _ = upload_s3(
                        local_stats_path,
                        bucket,
                        self.dst[dst_format].uri + stats_ext,
                    )

        except SubprocessKilledError as e:
            LOGGER.error(f"Could not upload file {self.tile_id}")
            LOGGER.exception(str(e))
            self.status = "failed - subprocess was killed"
        except Exception as e:
            LOGGER.error(f"Could not upload file {self.tile_id}")
            LOGGER.exception(str(e))
            self.status = "failed"

    def rm_local_src(self, dst_format) -> None:
        if dst_format in self.local_dst.keys():
            tiff_uri = self.local_dst[dst_format].uri
            stats_uri = tiff_uri + stats_ext
            for local_file in (tiff_uri, stats_uri):
                if os.path.isfile(local_file):
                    LOGGER.info(f"Delete local file {local_file}")
                    os.remove(local_file)

    def postprocessing(self):
        """Once we have the final geotiff, all postprocessing steps should be
        the same no matter the source format and grid type."""

        # Add superior compression, which only works with GDAL drivers
        self.create_gdal_geotiff()

        # Compute stats and histogram
        for dst_format in self.local_dst.keys():
            self.metadata[dst_format] = self.local_dst[dst_format].metadata(
                self.layer.compute_stats, self.layer.compute_histogram
            )
