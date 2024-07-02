from abc import ABC, abstractmethod
from typing import Iterator, List, Optional, Set, Tuple

from parallelpipe import stage

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import Layer
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.tiles.tile import Tile
from gfw_pixetl.utils import upload_geometries
from gfw_pixetl.utils.gdal import get_metadata

LOGGER = get_module_logger(__name__)


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
        self,
        overwrite: bool,
        remove_work: bool = True,
        upload: bool = True,
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
        """Don't process tiles if they already exist in target location,
        unless overwrite is set to True."""
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

    def _process_pipe(
        self, pipe, upload: bool = True
    ) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
        """Fetching all tiles which ran through the pipe.

        Check and sort by status.
        """

        processed_tiles: List[Tile] = list()
        skipped_tiles: List[Tile] = list()
        failed_tiles: List[Tile] = list()
        existing_tiles: List[Tile] = list()

        for tile in pipe.results():

            # Sort tiles based on their final status
            if tile.status == "pending":
                tile.status = "processed"
                processed_tiles.append(tile)
            elif tile.status.startswith("failed"):
                failed_tiles.append(tile)
            elif tile.status == "existing":
                existing_tiles.append(tile)
            else:
                skipped_tiles.append(tile)

        if upload and not failed_tiles:
            upload_geometries.upload_geojsons(
                processed_tiles, existing_tiles, self.layer.prefix
            )

        return processed_tiles, skipped_tiles, failed_tiles, existing_tiles
