from abc import ABC, abstractmethod
from typing import Iterator, List, Optional, Set, Tuple

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import Layer
from gfw_pixetl.parallelpipe import OomKillException, Pipeline, stage
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.tiles.tile import Tile
from gfw_pixetl.utils import upload_geometries
from gfw_pixetl.utils.gdal import get_metadata

LOGGER = get_module_logger(__name__)

# Maximum number of times we will retry a pipeline run after OOM kills.
MAX_OOM_RETRIES = 5


class Pipe(ABC):
    """Base Pipe including all the basic stages to seed, filter, delete and
    upload tiles.

    Create a subclass and override create_tiles() method to create your
    own pipe.
    """

    def __init__(self, layer: Layer, subset: Optional[List[str]] = None) -> None:
        self.grid = layer.grid
        self.layer = layer
        self.subset = subset
        self.tiles_to_process = 0

    def collect_tiles(self, overwrite: bool) -> List[Tile]:
        pipe = (
            self.get_grid_tiles()
            | self.filter_subset_tiles(self.subset)
            | self.filter_src_tiles
            | self.filter_target_tiles(overwrite=overwrite)
        )
        tiles = list()

        for tile in pipe.results():
            if tile.status == "pending":
                self.tiles_to_process += 1
            tiles.append(tile)

        LOGGER.info(f"{self.tiles_to_process} tiles to process")

        return tiles

    @abstractmethod
    def create_tiles(
        self, overwrite
    ) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
        """Override this method when implementing pipes."""
        ...

    @abstractmethod
    def get_grid_tiles(self) -> Set[Tile]:
        """Seed all available tiles within given grid.

        Use 1x1 degree tiles covering all land area as starting point.
        Then see in which target grid cell it would fall. Remove
        duplicated grid cells.
        """
        ...

    @abstractmethod
    def _get_grid_tile(self, tile_id: str) -> Tile:
        """Override this method when implementing pipes."""
        ...

    @staticmethod
    @stage(workers=GLOBALS.num_processes)
    @abstractmethod
    def filter_src_tiles():
        """Override this method when implementing pipes."""
        ...

    @staticmethod
    @stage(workers=GLOBALS.num_processes)
    def filter_subset_tiles(tiles: Iterator[Tile], subset) -> Iterator[Tile]:
        """Apply filter in case user only wants to process a subset.

        Useful for testing.
        """
        for tile in tiles:
            if subset and tile.status == "pending" and tile.tile_id not in subset:
                LOGGER.debug(f"Tile {tile} not in subset. Skip.")
                tile.status = "skipped (not in subset)"
            yield tile

    @staticmethod
    @stage(workers=GLOBALS.num_processes)
    def filter_target_tiles(tiles: Iterator[Tile], overwrite: bool) -> Iterator[Tile]:
        """Don't process tiles if they already exist in target location, unless
        overwrite is set to True."""
        for tile in tiles:
            if (
                not overwrite
                and tile.status == "pending"
                and all([tile.dst[fmt].exists() for fmt in tile.dst.keys()])
            ):
                for dst_format in tile.dst.keys():
                    tile.metadata[dst_format] = get_metadata(
                        tile.dst[tile.default_format].url,
                        tile.layer.compute_stats,
                        tile.layer.compute_histogram,
                    ).dict()
                tile.status = "existing"
                LOGGER.debug(f"Tile {tile} already in destination. Skip processing.")
            yield tile

    @staticmethod
    @stage(workers=GLOBALS.num_processes)
    def create_gdal_geotiff(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """Copy local file to geotiff format."""
        for tile in tiles:
            if tile.status == "pending":
                tile.create_gdal_geotiff()
            yield tile

    @staticmethod
    @stage(workers=GLOBALS.num_processes)
    def upload_file(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """Upload tile to target location."""
        for tile in tiles:
            if tile.status == "pending":
                tile.upload()
            yield tile

    @staticmethod
    @stage(workers=GLOBALS.num_processes)
    def delete_work_dir(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """Delete local files."""
        for tile in tiles:
            tile.remove_work_dir()
            yield tile

    def _build_pipe(self, tiles: List[Tile], workers: int) -> Pipeline:
        """Build the pipeline for a given list of tiles and worker count.

        Subclasses must override this to support OOM retry.  ``workers``
        controls the parallelism of the most memory-intensive stage;
        the retry loop halves it on each OOM kill.
        """
        raise NotImplementedError(
            "Subclasses must implement _build_pipe() to support OOM retry, "
            "or override _process_pipe() directly."
        )

    # ------------------------------------------------------------------
    # Core result-collection helpers
    # ------------------------------------------------------------------

    def _collect_pipe_results(
        self, pipe
    ) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
        """Drain *pipe* and bucket tiles by status.  Does NOT upload
        geometries.

        Returns (processed, skipped, failed, existing).
        """
        processed_tiles: List[Tile] = []
        skipped_tiles: List[Tile] = []
        failed_tiles: List[Tile] = []
        existing_tiles: List[Tile] = []

        for tile in pipe.results():
            if tile.status == "pending":
                tile.status = "processed"
                processed_tiles.append(tile)
            elif tile.status.startswith("failed"):
                failed_tiles.append(tile)
            elif tile.status == "existing":
                existing_tiles.append(tile)
            else:
                skipped_tiles.append(tile)

        return processed_tiles, skipped_tiles, failed_tiles, existing_tiles

    def _process_pipe(
        self, pipe
    ) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
        """Fetch all tiles from *pipe*, sort by status, upload geometries.

        This is the original single-run entry point.  Subclasses that do
        NOT need OOM retry can continue to call this directly.
        """
        processed_tiles, skipped_tiles, failed_tiles, existing_tiles = (
            self._collect_pipe_results(pipe)
        )

        if not failed_tiles:
            upload_geometries.upload_geojsons(
                processed_tiles, existing_tiles, self.layer.prefix
            )

        return processed_tiles, skipped_tiles, failed_tiles, existing_tiles

    def _process_pipe_with_oom_retry(
        self,
        tiles: List[Tile],
        workers: int,
        build_pipe,
    ) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
        """Run the pipeline with automatic OOM-kill recovery.

        Parameters
        ----------
        tiles:
            The full list of Tile objects to feed into the pipeline.
        workers:
            Initial worker count for the most memory-intensive stage.
        build_pipe:
            A callable ``build_pipe(tiles, workers) -> Pipeline`` that
            constructs the pipeline given a tile list and a worker count.
            Subclasses supply this so the retry loop can rebuild the pipe
            without knowing its internal structure.

        Returns
        -------
        The usual (processed, skipped, failed, existing) four-tuple.
        """
        all_processed: List[Tile] = []
        all_skipped: List[Tile] = []
        all_failed: List[Tile] = []
        all_existing: List[Tile] = []

        # We track "pending" tiles as those still needing a run.  Tiles that
        # complete (any status) are removed from pending_tiles so we never
        # feed them into a retry run.
        pending_tiles = list(tiles)
        current_workers = workers

        for attempt in range(1, MAX_OOM_RETRIES + 2):  # +2: attempts + final failure
            if not pending_tiles:
                break

            if attempt > 1:
                LOGGER.warning(
                    f"OOM retry attempt {attempt - 1}/{MAX_OOM_RETRIES}: "
                    f"{len(pending_tiles)} tile(s) remaining, "
                    f"retrying with {current_workers} worker(s)."
                )

            # Record which tile IDs are going into this attempt so we can
            # detect tiles that were dropped (in-flight when a worker was killed).
            tile_ids_in = {t.tile_id for t in pending_tiles}

            try:
                pipe = build_pipe(pending_tiles, current_workers)
                processed, skipped, failed, existing = self._collect_pipe_results(pipe)

                # Accumulate results
                all_processed.extend(processed)
                all_skipped.extend(skipped)
                all_failed.extend(failed)
                all_existing.extend(existing)

                # Remove completed tiles from pending
                completed_ids = {
                    t.tile_id for t in processed + skipped + failed + existing
                }
                pending_tiles = [
                    t for t in pending_tiles if t.tile_id not in completed_ids
                ]

                # Detect silently dropped tiles (were in-flight when worker died).
                # The OomKillException would have been raised before we got here,
                # but if somehow tiles just didn't come out, keep them pending.
                # In the normal (no-OOM) path pending_tiles should now be empty.
                if not pending_tiles:
                    LOGGER.info("All tiles completed successfully.")
                    break

                # If we still have pending tiles but no exception was raised,
                # something unexpected happened — treat remaining as needing retry.
                LOGGER.warning(
                    f"{len(pending_tiles)} tile(s) did not appear in pipeline output; "
                    "will retry."
                )
                # Don't reduce workers in this case — it's not an OOM
                if attempt > MAX_OOM_RETRIES:
                    LOGGER.error(
                        "Exceeded maximum retries. Marking remaining tiles as failed."
                    )
                    for t in pending_tiles:
                        t.status = "failed (dropped after max retries)"
                    all_failed.extend(pending_tiles)
                    pending_tiles = []
                else:
                    for t in pending_tiles:
                        t.reset_for_retry()

            except OomKillException as oom:
                LOGGER.error(
                    f"OOM kill detected: {oom}. "
                    f"Stages affected: "
                    f"{', '.join(e.stage_name for e in oom.events)}"
                )

                # Reduce workers so the next attempt uses less memory.
                # We halve, but never go below 1.
                current_workers = max(1, current_workers // 2)

                # Accumulate whatever did come out before the kill
                # (the pipeline yields items before raising OomKillException)
                # — those are already in all_* from _collect_pipe_results which
                # runs inside the try block before the exception propagates.
                # We need to remove completed tiles from pending.
                completed_ids = {
                    t.tile_id
                    for t in all_processed + all_skipped + all_failed + all_existing
                }
                pending_tiles = [
                    t for t in pending_tiles if t.tile_id not in completed_ids
                ]

                # Reset status to "pending" for tiles that were not completed,
                # so they go through the full pipeline again on the next attempt.
                for t in pending_tiles:
                    t.reset_for_retry()

                if attempt > MAX_OOM_RETRIES:
                    LOGGER.error(
                        f"Exceeded maximum OOM retries ({MAX_OOM_RETRIES}). "
                        f"Marking {len(pending_tiles)} tile(s) as failed."
                    )
                    for t in pending_tiles:
                        t.status = "failed (OOM after max retries)"
                    all_failed.extend(pending_tiles)
                    pending_tiles = []

        if not all_failed:
            upload_geometries.upload_geojsons(
                all_processed, all_existing, self.layer.prefix
            )

        return all_processed, all_skipped, all_failed, all_existing
