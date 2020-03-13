import multiprocessing
from math import ceil
from typing import Iterator, List, Optional, Set, Tuple

import boto3
from parallelpipe import stage


from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.utils import upload_geometries
from gfw_pixetl.layers import Layer
from gfw_pixetl.tiles.tile import Tile

LOGGER = get_module_logger(__name__)
S3 = boto3.client("s3")
WORKERS = utils.get_workers()
CORES = multiprocessing.cpu_count()


class Pipe(object):
    """
    Base Pipe including all the basic stages to seed, filter, delete and upload tiles.
    Create a subclass and override create_tiles() method to create your own pipe.
    """

    def __init__(self, layer: Layer, subset: Optional[List[str]] = None) -> None:
        self.grid = layer.grid
        self.layer = layer
        self.subset = subset
        self.tiles_to_process = 0

    def collect_tiles(self, overwrite: bool) -> List[Tile]:
        """
        Raster Pipe
        """

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

    def create_tiles(self, overwrite) -> Tuple[List[Tile], List[Tile], List[Tile]]:
        """
        Override this method when implementing pipes
        """
        raise NotImplementedError()

    def get_grid_tiles(self) -> Set[Tile]:
        """
        Seed all available tiles within given grid.
        Use 1x1 degree tiles covering all land area as starting point.
        Then see in which target grid cell it would fall.
        Remove duplicated grid cells.
        """

        LOGGER.debug("Get grid Tiles")
        tiles = set()

        for i in range(-89, 91):
            for j in range(-180, 180):
                origin = self.grid.xy_grid_origin(j, i)
                tiles.add(Tile(origin=origin, grid=self.grid, layer=self.layer))

        tile_count = len(tiles)
        LOGGER.info(f"Found {tile_count} tile inside grid")
        utils.set_workers(tile_count)

        return tiles

    @staticmethod
    @stage(workers=ceil(CORES / 4), qsize=ceil(CORES / 4))
    def filter_src_tiles():
        """
        Override this method when implementing pipes
        """
        raise NotImplementedError()

    @staticmethod
    @stage(workers=ceil(CORES / 4), qsize=ceil(CORES / 4))
    def filter_subset_tiles(tiles: Iterator[Tile], subset) -> Iterator[Tile]:
        """
        Apply filter in case user only want to process only a subset.
        Useful for testing.
        """
        for tile in tiles:
            if subset and tile.status == "pending" and tile.tile_id not in subset:
                LOGGER.debug(f"Tile {tile} not in subset. Skip.")
                tile.status = "skipped (not in subset)"
            yield tile

    @staticmethod
    @stage(workers=ceil(CORES / 4), qsize=ceil(CORES / 4))
    def filter_target_tiles(tiles: Iterator[Tile], overwrite: bool) -> Iterator[Tile]:
        """
        Don't process tiles if they already exists in target location,
        unless overwrite is set to True
        """
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
    @stage(workers=ceil(CORES / 4), qsize=ceil(CORES / 4))
    def create_gdal_geotiff(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """
        Copy local file to geotiff format
        """
        for tile in tiles:
            if tile.status == "pending":
                tile.create_gdal_geotiff()
            yield tile

    @staticmethod
    @stage(workers=ceil(CORES / 4), qsize=ceil(CORES / 4))
    def upload_file(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """
        Upload tile to target location
        """
        for tile in tiles:
            if tile.status == "pending":
                tile.upload()
            yield tile

    @staticmethod
    @stage(workers=ceil(CORES / 4), qsize=ceil(CORES / 4))
    def delete_file(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """
        Delete local file
        """
        for tile in tiles:
            if tile.status == "pending":
                for dst_format in tile.local_dst.keys():
                    tile.rm_local_src(dst_format)
            yield tile

    def _process_pipe(self, pipe) -> Tuple[List[Tile], List[Tile], List[Tile]]:

        tiles: List[Tile] = list()
        skipped_tiles: List[Tile] = list()
        failed_tiles: List[Tile] = list()
        existing_tiles: List[Tile] = list()

        for tile in pipe.results():

            if tile.dst[tile.default_format].exists():
                existing_tiles.append(tile)

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
        if len(tiles):
            upload_geometries.upload_vrt(tiles, self.layer.prefix)
            upload_geometries.upload_geom(tiles, self.layer.prefix)
            upload_geometries.upload_tile_geoms(tiles, self.layer.prefix)
