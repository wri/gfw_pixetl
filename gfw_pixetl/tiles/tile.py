import copy
import multiprocessing as mp
import os
import shutil
import traceback
from abc import ABC
from time import perf_counter
from typing import Dict

import rasterio
from rasterio.coords import BoundingBox
from rasterio.crs import CRS

from gfw_pixetl import get_module_logger
from gfw_pixetl.decorators import SubprocessKilledError
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import Layer
from gfw_pixetl.memory_admission import MEMORY_ADMISSION
from gfw_pixetl.models.enums import DstFormat
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.sources import Destination, RasterSource
from gfw_pixetl.utils import get_bucket
from gfw_pixetl.utils.aws import upload_s3
from gfw_pixetl.utils.gdal import just_copy_geotiff
from gfw_pixetl.utils.path import create_dir

LOGGER = get_module_logger(__name__)

stats_ext = ".aux.xml"  # Extension of stats sidecar gdalinfo -stats creates
_COPY_CONTEXT = mp.get_context("spawn")


def _copy_geotiff_target(conn, src_uri, dst_uri, profile) -> None:
    """Run the memory-heavy GDAL copy in a disposable spawned process."""
    try:
        just_copy_geotiff(src_uri, dst_uri, profile)
        conn.send((True, None))
    except BaseException:
        conn.send((False, traceback.format_exc()))
    finally:
        conn.close()


def _copy_geotiff_spawned(src_uri, dst_uri, profile) -> None:
    """Copy a GeoTIFF in a child whose exit deterministically reclaims
    memory."""
    recv_conn, send_conn = _COPY_CONTEXT.Pipe(duplex=False)
    process = _COPY_CONTEXT.Process(
        target=_copy_geotiff_target,
        args=(send_conn, src_uri, dst_uri, profile),
        name="pixetl-geotiff-copy",
    )
    process.start()
    send_conn.close()
    try:
        process.join()
        if recv_conn.poll():
            ok, error = recv_conn.recv()
            if not ok:
                raise RuntimeError(f"GeoTIFF copy subprocess failed:\n{error}")
        elif process.exitcode != 0:
            raise RuntimeError(
                f"GeoTIFF copy subprocess exited with code {process.exitcode}"
            )
        else:
            raise RuntimeError("GeoTIFF copy subprocess returned no result")
    finally:
        recv_conn.close()
        if process.is_alive():
            process.terminate()
            process.join()


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
            gdal_profile["photometric"] = (
                layer.photometric.value
            )  # need value, not just Enum!

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

        self.work_dir = create_dir(os.path.join(os.getcwd(), tile_id))
        self.tmp_dir = create_dir(os.path.join(self.work_dir, "tmp"))

        self.default_format = GLOBALS.default_dst_format
        self.status = "pending"
        self.metadata: Dict[str, Dict] = dict()

    def remove_work_dir(self):
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def set_local_dst(self, dst_format) -> None:
        if hasattr(self, "local_src"):
            self.rm_local_src(dst_format)

        uri = self.get_local_dst_uri(dst_format)
        LOGGER.debug(f"Set Local Source URI: {uri}")
        self.local_dst[dst_format] = RasterSource(uri)

    def get_local_dst_uri(self, dst_format) -> str:
        prefix = f"{self.work_dir}/{dst_format}"
        os.makedirs(f"{prefix}", exist_ok=True)

        uri = os.path.join(prefix, f"{self.tile_id}.tif")

        return uri

    def create_gdal_geotiff(self) -> None:
        dst_format = DstFormat.gdal_geotiff
        if dst_format in self.local_dst:
            LOGGER.info(
                f"Local Gdal Geotiff already exists for tile {self.tile_id}; skip copying"
            )
            return
        if self.default_format != dst_format:
            LOGGER.info(
                f"Create copy of local file as Gdal Geotiff for tile {self.tile_id}"
            )

            with MEMORY_ADMISSION.copy_slot(self.tile_id):
                _copy_geotiff_spawned(
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
        """Finalize a tile and report coarse copy/statistics timings."""
        total_started = perf_counter()

        # Add superior compression, which only works with GDAL drivers.
        phase_started = perf_counter()
        self.create_gdal_geotiff()
        copy_seconds = perf_counter() - phase_started

        # Structural metadata is cheap. Stats/histograms require full raster
        # scans, so only those opt-in paths are memory- and concurrency-gated.
        phase_started = perf_counter()
        needs_stats_gate = self.layer.compute_stats or self.layer.compute_histogram
        if needs_stats_gate:
            with MEMORY_ADMISSION.stats_slot(self.tile_id):
                for dst_format in self.local_dst.keys():
                    self.metadata[dst_format] = self.local_dst[dst_format].metadata(
                        self.layer.compute_stats, self.layer.compute_histogram
                    )
        else:
            for dst_format in self.local_dst.keys():
                self.metadata[dst_format] = self.local_dst[dst_format].metadata(
                    False, False
                )
        metadata_seconds = perf_counter() - phase_started

        LOGGER.info(
            "PERF postprocess "
            f"tile={self.tile_id} copy_s={copy_seconds:.3f} "
            f"metadata_s={metadata_seconds:.3f} "
            f"total_s={perf_counter() - total_started:.3f}"
        )
