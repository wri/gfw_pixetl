import multiprocessing as mp
import os
import queue
import sys
import traceback
from math import floor, sqrt
from pathlib import Path
from typing import Iterator, List, Optional, Tuple, Union

import numpy as np
import rasterio
from rasterio.io import DatasetReader, DatasetWriter
from rasterio.vrt import WarpedVRT
from rasterio.warp import transform_bounds
from rasterio.windows import Window, bounds, from_bounds, union

from gfw_pixetl import get_module_logger
from gfw_pixetl.decorators import SubprocessKilledError, lazy_property
from gfw_pixetl.grids import Grid
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.memory_admission import MEMORY_ADMISSION
from gfw_pixetl.models.named_tuples import InputBandElement
from gfw_pixetl.models.types import Bounds
from gfw_pixetl.settings.gdal import GDAL_ENV
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.tiles import Tile
from gfw_pixetl.tiles.utils.named_tuples import Destination, Layer, Source
from gfw_pixetl.tiles.utils.transform import transform
from gfw_pixetl.utils import (
    available_memory_per_process_bytes,
    available_memory_per_process_mb,
    snapped_window,
)
from gfw_pixetl.utils.gdal import create_multiband_vrt
from gfw_pixetl.utils.utils import create_empty_file, fetch_metadata

LOGGER = get_module_logger(__name__)

Windows = Tuple[Window, Window]


def _gdal_cache_size(block_byte_size: int, max_blocks: int) -> int:
    """Return a GDAL cache size as a plain Python integer."""
    return int(block_byte_size * max_blocks)


def _persistent_window_worker(
    result_queue, command_queue, tile_bytes: bytes, windows: List[Window]
) -> None:
    """Process every window for one tile in a single spawned interpreter."""
    import dill

    from gfw_pixetl.logs import configure_worker_logging

    configure_worker_logging("INFO")
    tile = dill.loads(tile_bytes)

    for window_index, window in enumerate(windows):
        try:
            result = tile._transform_window_in_current_process(window)
        except Exception:
            ex_type, ex_value, tb = sys.exc_info()
            result_queue.put(
                (
                    "error",
                    window_index,
                    (ex_type, ex_value, "".join(traceback.format_tb(tb))),
                )
            )
            return
        result_queue.put(("result", window_index, result))

        command = command_queue.get()
        if command == "stop":
            return
        if command != "continue":
            raise RuntimeError(f"Unknown persistent-worker command: {command!r}")


class RasterSrcTile(Tile):
    def __init__(self, tile_id: str, grid: Grid, layer: RasterSrcLayer) -> None:
        super().__init__(tile_id, grid, layer)
        self.layer: RasterSrcLayer = layer

    @lazy_property
    def src(self) -> RasterSource:
        LOGGER.debug(f"Finding input files for tile {self.tile_id}")

        input_bands: List[List[InputBandElement]] = list()

        for i, band in enumerate(self.layer.input_bands):
            input_elements: List[InputBandElement] = list()
            for f in band:
                # if self.dst[self.default_format].geom.intersects(
                #     f.geometry
                # ) and not self.dst[self.default_format].geom.touches(f.geometry):
                LOGGER.debug(
                    f"In src() Adding {f.uri} to input files for tile {self.tile_id}"
                )
                assert os.path.exists(f.uri), f"In src, {f.uri} does not exist!"

                uri = self.make_local_copy(f.uri)
                input_file = InputBandElement(uri=uri, geometry=f.geometry, band=f.band)

                input_elements.append(input_file)
            if band and not input_elements:
                LOGGER.debug(
                    f"No input files found for tile {self.tile_id} "
                    f"in band {i}, padding VRT with empty file"
                )
                # But we need to know the profile of the tile's siblings in this band.
                _, profile = fetch_metadata(band[0].uri)
                empty_file_uri = create_empty_file(self.work_dir, profile)
                empty_file_element = InputBandElement(
                    geometry=None, band=band[0].band, uri=empty_file_uri
                )
                input_elements.append(empty_file_element)
            input_bands.append(input_elements)

        if all([item.geometry is None for sublist in input_bands for item in sublist]):
            raise Exception(
                f"Did not find any intersecting files for tile {self.tile_id}"
            )
        return RasterSource(
            create_multiband_vrt(input_bands, vrt=self.tile_id + ".vrt")
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

        LOGGER.debug(
            f"Final bounds for window for tile {self.tile_id}: "
            f"Left: {left} Bottom: {bottom} Right: {right} Top: {top}"
        )

        try:
            window: Window = rasterio.windows.from_bounds(
                left,
                bottom,
                right,
                top,
                transform=self.dst[self.default_format].transform,
            )
        except rasterio.errors.WindowError:
            LOGGER.error(
                f"WindowError encountered for tile {self.tile_id} with "
                f"transform {self.dst[self.default_format].transform} "
                f"SRC bounds {src_left, src_bottom, src_right, src_top} "
                f"and DST bounds {dst_left, dst_bottom, dst_right, dst_top}"
            )
            raise

        return snapped_window(window)

    def reset_for_retry(self) -> None:
        """Clear all cached properties that reference files in work_dir.

        ``src`` creates hardlinks into ``work_dir`` and a VRT file on disk.
        ``intersecting_window`` is derived from ``src``.  Both are stored by
        ``cached_property`` (via ``lazy_property``) in the instance
        ``__dict__``, so deleting the key is all that is needed to force
        recomputation on next access.

        We must clear these *before* calling super(), which recreates
        ``work_dir``, because the old cached ``src`` holds a ``RasterSource``
        whose ``uri`` points to a VRT file that was deleted along with the
        previous ``work_dir``.
        """
        for attr in ("src", "intersecting_window"):
            self.__dict__.pop(attr, None)

        super().reset_for_retry()

    def within(self) -> bool:
        """Check if target tile extent intersects with source extent."""
        return (
            # must intersect, but we don't want geometries that only share an exterior point
            self.dst[self.default_format].geom.intersects(self.layer.geom)
            and not self.dst[self.default_format].geom.touches(self.layer.geom)
        )

    def transform(self) -> bool:
        """Write input data to output tile."""
        LOGGER.debug(f"Transform tile {self.tile_id}")

        try:
            has_data: bool = self._process_windows()

            # creating gdal-geotiff and computing stats here
            # instead of in a separate stage to assure we don't run out of memory
            # the transform stage uses all available memory for concurrent processes.
            # Having another stage which needs a lot of memory might cause the process to crash
            if has_data:
                self.postprocessing()

        except SubprocessKilledError as e:
            LOGGER.exception(e)
            self.status = "failed - subprocess was killed"
            has_data = True
        except Exception as e:
            LOGGER.exception(e)
            self.status = "failed"
            has_data = True

        return has_data

    def _src_to_vrt(self) -> Tuple[DatasetReader, WarpedVRT]:
        chunk_size = _gdal_cache_size(self._block_byte_size(), self._max_blocks())
        with rasterio.Env(
            **GDAL_ENV,
            VSI_CACHE_SIZE=chunk_size,  # Cache size for current file.
            CPL_VSIL_CURL_CHUNK_SIZE=chunk_size,  # Chunk size for partial downloads
        ):
            src: DatasetReader = rasterio.open(self.src.uri)

            transform, width, height = self._vrt_transform(
                *self.src.reproject_bounds(self.grid.crs)
            )
            vrt = WarpedVRT(
                src,
                crs=self.dst[self.default_format].crs,
                transform=transform,
                width=width,
                height=height,
                warp_mem_limit=available_memory_per_process_mb(),
                resampling=self.layer.resampling,
            )

        return src, vrt

    def _process_windows(self) -> bool:
        """Process windows sequentially in one spawned child per tile."""
        return self._process_windows_sequential()

    def _process_windows_sequential(self) -> bool:
        """Process every window for this tile in one persistent spawned
        worker."""
        import dill

        windows = self.windows()
        if not windows:
            MEMORY_ADMISSION.commit_transform_reservation()
            return False

        LOGGER.info(
            f"Processing tile {self.tile_id} with one persistent window worker "
            f"({len(windows)} windows)"
        )

        ctx = mp.get_context("spawn")
        result_queue = ctx.Queue()
        command_queue = ctx.Queue()
        worker = ctx.Process(
            target=_persistent_window_worker,
            args=(result_queue, command_queue, dill.dumps(self), windows),
            name=f"window-worker-{self.tile_id}",
        )
        out_files = []
        first_window = True
        window_reservation_held = False

        try:
            MEMORY_ADMISSION.acquire_window(self.tile_id, 0)
            window_reservation_held = True
            worker.start()

            for received in range(len(windows)):
                while True:
                    try:
                        kind, window_index, payload = result_queue.get(timeout=1)
                        break
                    except queue.Empty:
                        if worker.is_alive():
                            continue
                        raise SubprocessKilledError(
                            f"Persistent window worker exited with code {worker.exitcode}"
                        )

                if window_reservation_held:
                    MEMORY_ADMISSION.release_window()
                    window_reservation_held = False

                if kind == "error":
                    command_queue.put("stop")
                    _, ex_value, child_traceback = payload
                    LOGGER.error(
                        "Persistent window worker failed for tile %s window %s:\n%s",
                        self.tile_id,
                        window_index,
                        child_traceback,
                    )
                    raise ex_value

                out_files.append(payload)
                if first_window:
                    MEMORY_ADMISSION.commit_transform_reservation()
                    first_window = False

                if received + 1 == len(windows):
                    command_queue.put("stop")
                    continue

                MEMORY_ADMISSION.acquire_window(self.tile_id, window_index + 1)
                window_reservation_held = True
                command_queue.put("continue")

            worker.join(timeout=60)
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=10)
                raise SubprocessKilledError("Persistent window worker did not exit")
            if worker.exitcode != 0:
                raise SubprocessKilledError(
                    f"Persistent window worker exited with code {worker.exitcode}"
                )
        finally:
            MEMORY_ADMISSION.commit_transform_reservation()
            if window_reservation_held:
                MEMORY_ADMISSION.release_window()
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=10)
            result_queue.close()
            result_queue.join_thread()
            command_queue.close()
            command_queue.join_thread()

        return any(value is not None for value in out_files)

    def _transform_window_in_current_process(
        self, window: Window, write_to_seperate_files=False
    ) -> Optional[str]:
        """Transform one window in the current, already-spawned process."""
        src, vrt = self._src_to_vrt()
        try:
            layer = Layer(
                input_bands=self.layer.input_bands, calc_string=self.layer.calc
            )
            source = Source(vrt=vrt, crs=self.src.crs)
            destination = Destination(
                transform=self.dst[self.default_format].transform,
                crs=self.dst[self.default_format].crs,
                count=self.dst[self.default_format].profile["count"],
                no_data=self.dst[self.default_format].nodata,
                datatype=self.dst[self.default_format].dtype,
                profile=self.dst[self.default_format].profile,
                tmp_dir=self.tmp_dir,
                uri=self.local_dst[self.default_format].uri,
                write_to_separate_files=write_to_seperate_files,
            )
            return transform(self.tile_id, window, layer, source, destination)
        finally:
            vrt.close()
            src.close()

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

        block_count: int = int(sqrt(self._max_blocks()))
        x_blocks: int = int(dst.width / dst.block_shapes[0][0])
        y_blocks: int = int(dst.height / dst.block_shapes[0][1])

        for i in range(0, x_blocks, block_count):
            for j in range(0, y_blocks, block_count):
                max_i = min(i + block_count, x_blocks)
                max_j = min(j + block_count, y_blocks)
                window = self._union_blocks(dst, i, j, max_i, max_j)
                try:
                    yield snapped_window(window.intersection(self.intersecting_window))
                except rasterio.errors.WindowError as e:
                    e_str = str(e)
                    if "Bounds and transform are inconsistent" in e_str:
                        # FIXME: This check was introduced recently in rasterio
                        # Figure out what it means to fail, and fix the window
                        # generating code in this function
                        LOGGER.warning(
                            f"Bogus window generated for tile {self.tile_id}! "
                            f"i: {i} j: {j} max_i: {max_i} max_j: {max_j} window: {window}"
                        )
                    elif "Intersection is empty Window" in e_str:
                        # Seems harmless to skip empty windows we generate
                        continue
                    elif "windows do not intersect" in e_str:
                        # Hmm, should this happen? Log for further investigation
                        LOGGER.warning(
                            f"Non-intersecting windows generated for tile {self.tile_id}! "
                            f"i: {i} j: {j} max_i: {max_i} max_j: {max_j} window: {window}"
                        )
                    else:
                        raise

    def _max_blocks(self) -> int:
        """Calculate the maximum amount of blocks we can fit into memory,
        making sure that blocks can always fill a squared extent.

        We can only use a fraction of the available memory per process
        per block b/c we might have multiple copies of the array at the
        same time. Using a divisor of 8 leads to max memory usage of
        about 75%.
        """

        # Adjust divisor to band count
        divisor = GLOBALS.divisor

        # Float data types seem to need more memory.
        if np.issubdtype(
            self.dst[self.default_format].dtype, np.floating
        ) or np.issubdtype(self.src.dtype, np.floating):
            divisor *= 2
            LOGGER.debug("Divisor doubled for float data")

            # Float64s require even more?
            if (
                self.dst[self.default_format].dtype == np.dtype("float64")
            ) or self.src.dtype == np.dtype("float64"):
                divisor *= 2
                LOGGER.debug("Divisor doubled again for float64 data")

        # Multiple layers need more memory
        divisor *= self.layer.band_count

        # further reduce block size in case we need to perform additional computations
        if self.layer.calc is not None:
            divisor **= 2
            LOGGER.debug("Divisor squared for calc operations")

        LOGGER.debug(f"Divisor set to {divisor} for tile {self.tile_id}")

        block_byte_size: int = self._block_byte_size()
        memory_per_process: float = available_memory_per_process_bytes() / divisor

        # make sure we get a number whose sqrt is a whole number
        max_blocks: int = max(1, floor(sqrt(memory_per_process / block_byte_size)) ** 2)

        LOGGER.debug(
            f"Maximum number of blocks for tile {self.tile_id} to read at once: {max_blocks}. "
            f"Expected max chunk size: {max_blocks * block_byte_size} B."
        )

        return max_blocks

    def _block_byte_size(self):
        shape = (
            len(self.layer.input_bands),
            self.dst[self.default_format].blockxsize,
            self.dst[self.default_format].blockysize,
        )

        dst_block_byte_size = np.dtype(
            self.dst[self.default_format].dtype
        ).itemsize * np.prod(shape)
        src_block_byte_size = np.dtype(self.src.dtype).itemsize * np.prod(shape)

        max_block_byte_size = max(dst_block_byte_size, src_block_byte_size)
        LOGGER.debug(f"Block byte size is {max_block_byte_size / 1000000} MB")

        return max_block_byte_size

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

    def _vrt_transform(
        self, west: float, south: float, east: float, north: float
    ) -> Tuple[rasterio.Affine, float, float]:
        """Compute Affine transformation, width and height for WarpedVRT using
        output CRS and pixel size."""

        LOGGER.debug(f"Output Bounds {west, south, east, north}")
        north, west = self.grid.snap_coordinates(north, west)
        south, east = self.grid.snap_coordinates(south, east)

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

    def make_local_copy(self, path: Union[Path, str]) -> str:
        """Make a hardlink to a source file in this tile's work directory."""
        LOGGER.debug(f"In make_local_copy. Original file path: {path}")

        assert os.path.exists(path), f"In make_local_copy. {path} does not exist!"

        # Convert to Path object for cleaner manipulation
        path_obj = Path(path)

        # Source downloads live alongside tile work directories. Preserve the
        # source path relative to that workspace rather than assuming the
        # workspace itself is /tmp.
        if path_obj.is_absolute():
            workspace = Path(self.work_dir).parent
            try:
                relative_path = path_obj.relative_to(workspace)
            except ValueError:
                # Preserve the existing fallback for absolute paths outside the
                # PixETL workspace.
                relative_path = Path(*path_obj.parts[1:])  # Skip the root /
        else:
            relative_path = path_obj

        new_path = Path(self.work_dir) / relative_path
        LOGGER.debug(f"In make_local_copy. New path: {new_path}")

        # Create parent directory
        new_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if link already exists (from a previous process/attempt)
        if new_path.exists():
            if new_path.samefile(path):
                LOGGER.debug(f"Link already exists: {new_path}")
                return str(new_path)
            else:
                LOGGER.warning(f"File exists but is not the same: {new_path}")
                new_path.unlink()  # Remove and recreate

        LOGGER.debug(f"Linking file {path} to {new_path}")
        os.link(path, new_path)

        return str(new_path)
