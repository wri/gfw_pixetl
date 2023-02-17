import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from typing import DefaultDict, Iterator, List, Set, Tuple, cast
from urllib.parse import urlparse

from parallelpipe import Stage, stage

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import RasterSrcLayer
from gfw_pixetl.pipes import Pipe
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.tiles import RasterSrcTile, Tile
from gfw_pixetl.utils.aws import download_s3
from gfw_pixetl.utils.google import download_gcs
from gfw_pixetl.utils.path import create_dir, from_vsi

LOGGER = get_module_logger(__name__)


def populate_local_sources(args: Tuple[str, List[str]]):
    uri, local_dsts = args
    download_constructor = {"gs": download_gcs, "s3": download_s3}
    path = from_vsi(uri)
    parts = urlparse(path)

    create_dir(os.path.dirname(local_dsts[0]))

    LOGGER.debug(f"Downloading remote file {uri} to {local_dsts}")
    download_constructor[parts.scheme](
        bucket=parts.netloc, key=parts.path[1:], dst=local_dsts[0]
    )
    for dest in local_dsts[1:]:
        create_dir(os.path.dirname(dest))

        # LOGGER.info(f"Copying {local_dsts[0]} to {dest}")
        # shutil.copyfile(local_dsts[0], dest)
        # if not os.path.isfile(dest):
        #     LOGGER.error(f"Copying to {dest} seems to have failed!")
        # elif os.stat(dest).st_size == 0:
        #     LOGGER.error(
        #         f"Copying {local_dsts[0]} to {dest} threw no errors, "
        #         "but result is an empty file!"
        #     )

        # LOGGER.info(f"Making {dest} a hardlink to {local_dsts[0]}")
        # os.link(local_dsts[0], dest)
        # if not os.path.isfile(dest):
        #     LOGGER.error(f"Making hardlink {dest} seems to have failed!")
        # elif os.stat(dest).st_size == 0:
        #     LOGGER.error(f"Hardlinking {dest} to {local_dsts[0]} succeeded, but result is an empty file!")

        LOGGER.info(f"Making {dest} a symlink to {local_dsts[0]}")
        os.symlink(local_dsts[0], dest)
        if not os.path.islink(dest):
            LOGGER.error(f"Making symlink {dest} seems to have failed!")
        # elif os.stat(dest).st_size == 0:
        #     LOGGER.error(f"Hardlinking {dest} to {local_dsts[0]} succeeded, but result is an empty file!")


class RasterPipe(Pipe):
    def get_grid_tiles(self) -> Set[RasterSrcTile]:  # type: ignore
        """Seed all available tiles within given grid.

        Use 1x1 degree tiles covering all land area as starting point.
        Then see in which target grid cell it would fall. Remove
        duplicated grid cells.
        """

        tiles: Set[RasterSrcTile] = set()
        for tile_id in self.grid.get_tile_ids():
            tiles.add(self._get_grid_tile(tile_id))

        # tile_ids = self.grid.get_tile_ids()
        #
        # with get_context("spawn").Pool(processes=GLOBALS.num_processes) as pool:
        #     tiles: Set[RasterSrcTile] = set(pool.map(self._get_grid_tile, tile_ids))

        tile_count: int = len(tiles)
        LOGGER.info(f"Found {tile_count} tile(s) inside grid")

        return tiles

    def _get_grid_tile(self, tile_id: str) -> RasterSrcTile:
        assert isinstance(self.layer, RasterSrcLayer)
        return RasterSrcTile(tile_id=tile_id, grid=self.grid, layer=self.layer)

    def create_tiles(
        self, overwrite: bool
    ) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
        """Raster Pipe."""

        LOGGER.info("Starting Raster Pipe create_tiles")

        tiles: List[Tile] = self.collect_tiles(overwrite=overwrite)

        LOGGER.info(
            f"There are {len(tiles)} total tiles, {self.tiles_to_process} "
            "of which are to be processed"
        )
        LOGGER.info(
            f"Right now, GLOBALS.workers is {GLOBALS.workers} and "
            f"GLOBALS.num_processes is {GLOBALS.num_processes}"
        )

        GLOBALS.workers = max(self.tiles_to_process, 1)
        LOGGER.info(f"And now, GLOBALS.workers is set to {GLOBALS.workers}")

        # Different tiles may reference the same source files. To prevent
        # multiple workers trying to download the same source and
        # stepping on each others' toes, download the source files first with
        # one process per SOURCE, not per TILE.
        src_uri_to_local_paths: DefaultDict[str, Set[str]] = defaultdict(lambda: set())

        for tile in tiles:
            if tile.status == "pending":
                raster_tile = cast(RasterSrcTile, tile)
                for uri in raster_tile.src_uris():
                    LOGGER.info(
                        f"Adding {uri} to be downloaded to {raster_tile._get_local_source_uri(uri)}"
                    )
                    src_uri_to_local_paths[uri].add(
                        raster_tile._get_local_source_uri(uri)
                    )

        LOGGER.info(f"Pre-fetching {len(src_uri_to_local_paths)} source files")

        with ProcessPoolExecutor(max_workers=GLOBALS.num_processes) as executor:
            for uri, local_dests in src_uri_to_local_paths.items():
                executor.submit(populate_local_sources, (uri, list(local_dests)))

        LOGGER.info("Pre-fetching done")

        LOGGER.info("Starting Raster Pipe")
        pipe = (
            tiles
            | Stage(self.transform).setup(workers=GLOBALS.workers)
            | self.upload_file
            | self.delete_work_dir
        )

        tiles, skipped_tiles, failed_tiles, existing_tiles = self._process_pipe(pipe)

        LOGGER.info("Finished Raster Pipe")
        return tiles, skipped_tiles, failed_tiles, existing_tiles

    @staticmethod
    @stage(workers=GLOBALS.num_processes)
    def filter_src_tiles(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """Only process tiles which intersect with source raster."""
        for tile in tiles:
            if tile.status == "pending" and not tile.within():
                LOGGER.info(
                    f"Tile {tile.tile_id} does not intersect with source raster - skip"
                )
                tile.status = "skipped (does not intersect)"
            yield tile

    # We cannot use the @stage decorator here but need to create a Stage
    # instance directly in the pipe. When using the decorator, the number
    # of workers gets set during RasterPipe class instantiation
    # and cannot be changed afterwards. The Stage class gives us more
    # flexibility.
    @staticmethod
    def transform(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """Transform input raster to match new tile grid and projection."""
        for tile in tiles:
            if tile.status == "pending" and not tile.transform():
                tile.status = "skipped (has no data)"
                LOGGER.info(f"Tile {tile.tile_id} has no data - skip")
            yield tile
