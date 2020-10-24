from abc import ABC, abstractmethod
from typing import Iterator, List, Optional, Set, Tuple

from parallelpipe import stage

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.layers import Layer
from gfw_pixetl.settings.globals import SETTINGS
from gfw_pixetl.tiles.tile import Tile
from gfw_pixetl.utils import upload_geometries

LOGGER = get_module_logger(__name__)
WORKERS = utils.get_workers()


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
        """Raster Pipe."""

        LOGGER.info("Start Raster Pipe")

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
    def create_tiles(self, overwrite) -> Tuple[List[Tile], List[Tile], List[Tile]]:
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
    @stage(workers=SETTINGS.cores)
    @abstractmethod
    def filter_src_tiles():
        """Override this method when implementing pipes."""
        ...

    @staticmethod
    @stage(workers=SETTINGS.cores)
    def filter_subset_tiles(tiles: Iterator[Tile], subset) -> Iterator[Tile]:
        """Apply filter in case user only want to process only a subset.

        Useful for testing.
        """
        for tile in tiles:
            if subset and tile.status == "pending" and tile.tile_id not in subset:
                LOGGER.debug(f"Tile {tile} not in subset. Skip.")
                tile.status = "skipped (not in subset)"
            yield tile

    @staticmethod
    @stage(workers=SETTINGS.cores)
    def filter_target_tiles(tiles: Iterator[Tile], overwrite: bool) -> Iterator[Tile]:
        """Don't process tiles if they already exists in target location,
        unless overwrite is set to True."""
        for tile in tiles:
            if (
                not overwrite
                and tile.status == "pending"
                and tile.dst[tile.default_format].exists()
            ):
                tile.status = "skipped (tile exists)"
                LOGGER.debug(f"Tile {tile} already in destination. Skip.")
            yield tile

    @staticmethod
    @stage(workers=SETTINGS.cores)
    def create_gdal_geotiff(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """Copy local file to geotiff format."""
        for tile in tiles:
            if tile.status == "pending":
                tile.create_gdal_geotiff()
            yield tile

    @staticmethod
    @stage(workers=SETTINGS.cores)
    def upload_file(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """Upload tile to target location."""
        for tile in tiles:
            if tile.status == "pending":
                tile.upload()
            yield tile

    @staticmethod
    @stage(workers=SETTINGS.cores)
    def delete_file(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """Delete local file."""
        for tile in tiles:
            if tile.status == "pending":
                for dst_format in tile.local_dst.keys():
                    tile.rm_local_src(dst_format)
            yield tile

    def _process_pipe(self, pipe) -> Tuple[List[Tile], List[Tile], List[Tile]]:
        """Fetching all tiles, which ran through the pipe.

        Check and sort by status.
        """

        tiles: List[Tile] = list()
        skipped_tiles: List[Tile] = list()
        failed_tiles: List[Tile] = list()
        existing_tiles: List[Tile] = list()

        for tile in pipe.results():

            # Checking again which tiles are already in the final output folder.
            # We need this to build the final geojson file which includes all the tiles.
            # There might be already files which have been processed in a previous run
            # So we cannot rely on the tile status alone.
            # S3 is eventually consistent and it might take up to 15min for a file to become available after upload
            # We hence don't check if remote files exists for processed files,
            # just for those which were skipped or failed during the current run

            if tile.status == "pending" or tile.dst[tile.default_format].exists():
                existing_tiles.append(tile)

            # Sorting tiles based on their status final reporting
            if tile.status == "pending":
                tile.status = "processed"
                tiles.append(tile)
            elif tile.status == "failed":
                failed_tiles.append(tile)
            else:
                skipped_tiles.append(tile)

        self._upload_geometries(existing_tiles)

        return tiles, skipped_tiles, failed_tiles

    def _upload_geometries(self, tiles) -> None:
        """Computing VRT, extent GeoJSON and Tile GeoJSON and upload to S3."""
        if len(tiles):
            # upload_geometries.upload_vrt(tiles, self.layer.prefix)
            upload_geometries.upload_geom(tiles, self.layer.prefix)
            upload_geometries.upload_tile_geoms(tiles, self.layer.prefix)
