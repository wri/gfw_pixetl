from typing import Dict, Iterator, List, Optional, Set, Tuple, Union

import numpy as np

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import VectorSrcLayer
from gfw_pixetl.memory_admission import GIB, MEMORY_ADMISSION, AdmissionSharedState
from gfw_pixetl.parallelpipe import Pipeline, Stage, stage
from gfw_pixetl.pipes import Pipe
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.tiles import Tile, VectorSrcTile
from gfw_pixetl.tiles.vector_src_tile import (
    RASTER_PART_COUNT,
    VectorRasterAssemblyItem,
    VectorRasterWorkItem,
)

LOGGER = get_module_logger(__name__)

ASSEMBLE_RASTER_WORKERS = 64


class VectorPipe(Pipe):
    def create_tiles(
        self, overwrite
    ) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
        """Vector Pipe."""
        LOGGER.debug("Start Vector Pipe")
        tiles = self.collect_tiles(overwrite=overwrite)

        workers = self._processing_workers()
        result = self._process_pipe(self._build_pipe(tiles, workers))

        LOGGER.debug("Finished Vector Pipe")
        return result

    def _rasterize_reservation_bytes(self) -> int:
        """Estimate the rasterize stage's per-tile memory reservation.

        Every tile in a single run shares the same grid, so this only needs
        computing once per pipe, not per tile.

        Real memory does NOT scale anywhere close to proportionally with
        output pixel count once GDAL_CACHEMAX is capped (see gdal.py):
        going from a 10/40000 WDPA grid (~30m/pixel, 40000x40000 pixels)
        to 10/100000 (~10m/pixel, 100000x100000 pixels) is a 6.25x larger
        raw output array, but real observed memory only grew ~1.5x (~1.92
        -> ~3.0GiB). gdal_rasterize appears to stream output in bounded
        blocks rather than materializing the whole array, so real cost is
        mostly a fixed per-tile floor (GDAL's small cache, the in-memory
        vector GeoDataFrame, process overhead) with only a modest
        resolution-dependent term on top -- an affine model, not a
        multiplier on the raw array size. See
        vector_rasterize_reservation_base_gib's description for the fit
        this is drawn from and its caveats.
        """
        if GLOBALS.vector_rasterize_reservation_gib is not None:
            return int(GLOBALS.vector_rasterize_reservation_gib * GIB)

        pixel_count = self.grid.cols * self.grid.rows
        bytes_per_pixel = np.dtype(self.layer.dst_profile["dtype"]).itemsize
        band_count = max(self.layer.band_count, 1)
        raw_bytes = pixel_count * bytes_per_pixel * band_count

        base_bytes = int(GLOBALS.vector_rasterize_reservation_base_gib * GIB)
        reservation_bytes = base_bytes + int(
            raw_bytes * GLOBALS.vector_rasterize_reservation_scale_factor
        )
        floor_bytes = int(GLOBALS.vector_rasterize_reservation_floor_gib * GIB)
        return max(reservation_bytes, floor_bytes)

    def _build_pipe(self, tiles: List[Tile], workers: int) -> Pipeline:
        """Construct the vector pipeline for a given tile list and worker
        count.

        ``tiles`` have already been classified by ``collect_tiles()``. Filtering
        must not be repeated here because source filtering performs database I/O
        and tile status has already been decided.

        ``workers`` controls the parallelism of the memory-intensive
        ``rasterize`` stage.
        """
        # Configure admission before workers spawn. The reservation reflects
        # this layer's grid and dtype so startup bursts are memory-bounded.
        MEMORY_ADMISSION.configure(
            enabled=GLOBALS.memory_admission_enabled,
            high_watermark=GLOBALS.memory_admission_high_watermark,
            resume_watermark=GLOBALS.memory_admission_resume_watermark,
            reservation_bytes=self._rasterize_reservation_bytes(),
            window_reservation_bytes=int(
                GLOBALS.memory_admission_window_reservation_gib * GIB
            ),
            poll_seconds=GLOBALS.memory_admission_poll_seconds,
        )

        # Spawned workers do not inherit the configured module singleton, so
        # pass its shared state explicitly and bind it in each rasterize
        # worker.
        admission_state = MEMORY_ADMISSION.snapshot_shared_state()

        return (
            tiles
            | self.fetch_tile_data
            | self.split_raster_work
            | Stage(self.rasterize_work, admission_state).setup(workers=workers)
            | Stage(self.group_raster_work).setup(workers=1)
            | Stage(self.assemble_raster_work, admission_state).setup(
                workers=ASSEMBLE_RASTER_WORKERS
            )
            | self.upload_file
            | self.delete_work_dir
        )

    def get_grid_tiles(self) -> Set[VectorSrcTile]:  # type: ignore
        """Seed all available tiles within given grid.

        Use 1x1 degree tiles covering all land area as starting point.
        Then see in which target grid cell it would fall. Remove
        duplicated grid cells.
        """
        tiles: Set[VectorSrcTile] = set()
        for tile_id in self.grid.get_tile_ids():
            tiles.add(self._get_grid_tile(tile_id))

        tile_count: int = len(tiles)
        LOGGER.info(f"Found {tile_count} tiles inside grid")

        return tiles

    def _get_grid_tile(self, tile_id: str) -> VectorSrcTile:
        assert isinstance(self.layer, VectorSrcLayer)
        return VectorSrcTile(tile_id=tile_id, grid=self.grid, layer=self.layer)

    @staticmethod
    @stage(
        workers=GLOBALS.db_fetch_workers
    )  # Budget for the source DB, see GLOBALS.db_fetch_workers
    def filter_src_tiles(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """Only include tiles which intersect input vector extent."""
        for tile in tiles:
            if tile.status == "pending" and not tile.src_vector_intersects():
                tile.status = "skipped (does not intersect)"
            yield tile

    @staticmethod
    @stage(
        workers=GLOBALS.db_fetch_workers
    )  # Budget for the source DB, see GLOBALS.db_fetch_workers
    def fetch_tile_data(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        """Download vector data from the database."""
        for tile in tiles:
            if tile.status == "pending":
                tile.fetch_data()
            yield tile

    @staticmethod
    @stage(workers=1)
    def split_raster_work(
        tiles: Iterator[VectorSrcTile],
    ) -> Iterator[Union[VectorSrcTile, VectorRasterWorkItem]]:
        """Fan pending parent tiles out into fixed raster work items."""
        for tile in tiles:
            if tile.status == "pending":
                yield from tile.raster_work_items()
            else:
                yield tile

    @staticmethod
    def rasterize_work(
        items: Iterator[Union[VectorSrcTile, VectorRasterWorkItem]],
        admission_state: Optional[AdmissionSharedState] = None,
    ) -> Iterator[Union[VectorSrcTile, VectorRasterWorkItem]]:
        """Rasterize independent work items under the existing admission
        gate."""
        if admission_state is not None:
            MEMORY_ADMISSION.bind_shared_state(admission_state)

        for item in items:
            if isinstance(item, VectorRasterWorkItem):
                with MEMORY_ADMISSION.tile_slot(item.work_id):
                    item.parent.rasterize_work_item(item)
            yield item

    @staticmethod
    def group_raster_work(
        items: Iterator[Union[VectorSrcTile, VectorRasterWorkItem]],
    ) -> Iterator[Union[VectorSrcTile, VectorRasterAssemblyItem]]:
        """Group completed raster work items without doing heavy work."""
        pending: Dict[str, List[VectorRasterWorkItem]] = {}

        for item in items:
            if not isinstance(item, VectorRasterWorkItem):
                yield item
                continue

            tile_id = item.parent.tile_id
            group = pending.setdefault(tile_id, [])
            group.append(item)
            if len(group) == RASTER_PART_COUNT:
                yield VectorRasterAssemblyItem(group[0].parent, group)
                del pending[tile_id]

        if pending:
            incomplete = ", ".join(
                f"{tile_id} ({len(parts)}/{RASTER_PART_COUNT})"
                for tile_id, parts in sorted(pending.items())
            )
            raise RuntimeError(f"Incomplete vector raster work groups: {incomplete}")

    @staticmethod
    def assemble_raster_work(
        items: Iterator[Union[VectorSrcTile, VectorRasterAssemblyItem]],
        admission_state: Optional[AdmissionSharedState] = None,
    ) -> Iterator[VectorSrcTile]:
        """Assemble complete groups in parallel and restore parent tiles."""
        if admission_state is not None:
            MEMORY_ADMISSION.bind_shared_state(admission_state)

        for item in items:
            if isinstance(item, VectorRasterAssemblyItem):
                parent = item.parent
                with MEMORY_ADMISSION.tile_slot(f"{parent.tile_id}-assemble"):
                    parent.assemble_raster_work_items(item.parts)
                yield parent
            else:
                yield item
