import multiprocessing
import os
from math import ceil
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union, Sequence

import boto3
from geojson import FeatureCollection, Feature, dumps
from parallelpipe import stage
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union

from gfw_pixetl import get_module_logger, utils
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

    def collect_tiles(self, overwrite=True) -> List[Tile]:
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
            tiles.append(tile)

        tile_count = len(tiles)
        LOGGER.info(f"{tile_count} tiles to process")

        return tiles

    def create_tiles(self, overwrite=True) -> List[Tile]:
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
            if not subset:
                yield tile
            elif tile.tile_id in subset:
                yield tile
            else:
                LOGGER.debug(f"Tile {tile} not in subset. Skip.")

    @staticmethod
    @stage(workers=ceil(CORES / 4), qsize=ceil(CORES / 4))
    def filter_target_tiles(
        tiles: Iterator[Tile], overwrite: bool = True
    ) -> Iterator[Tile]:
        """
        Don't process tiles if they already exists in target location,
        unless overwrite is set to True
        """
        for tile in tiles:
            if overwrite or not tile.dst.exists():
                LOGGER.debug(f"Processing tile {tile}")
                yield tile
            else:
                LOGGER.debug(f"Tile {tile} already in destination. Skip.")

    # @staticmethod
    # @stage(workers=WORKERS)
    # def delete_if_empty(tiles: Iterator[Tile]) -> Iterator[Tile]:
    #     """
    #     Exclude empty intermediate tiles and delete local copy
    #     """
    #     for tile in tiles:
    #         if tile.local_src_is_empty():
    #             tile.rm_local_src()
    #         else:
    #             yield tile

    @staticmethod
    @stage(workers=ceil(CORES / 4), qsize=ceil(CORES / 4))
    def upload_file(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """
        Upload tile to target location
        """
        for tile in tiles:
            tile.upload()
            yield tile

    @staticmethod
    @stage(workers=ceil(CORES / 4), qsize=ceil(CORES / 4))
    def delete_file(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """
        Delete local file
        """
        for tile in tiles:
            tile.rm_local_src()
            yield tile

    def _process_pipe(self, pipe):
        tile_uris: List[str] = list()
        tiles: List[Tile] = list()
        for tile in pipe.results():
            tiles.append(tile)
            tile_uris.append(f"/vsis3/{utils.get_bucket()}/{tile.dst.uri}")

        if len(tiles):
            self.upload_vrt(tile_uris)
            self.upload_geom(tiles)

        return tiles

    def upload_vrt(self, uris: List[str]) -> Dict[str, Any]:
        vrt = utils.create_vrt(uris)
        return self._upload_vrt(vrt)

    def _upload_vrt(self, vrt):
        LOGGER.info("Upload vrt")
        return S3.upload_file(
            vrt, utils.get_bucket(), os.path.join(self.layer.prefix, vrt)
        )

    def upload_geom(
        self, tiles: List[Tile], bucket: str = utils.get_bucket(), key: str = None
    ) -> Dict[str, Any]:

        if key is None:
            key = os.path.join(self.layer.prefix, "extent.geojson")
        extent: Union[Polygon, MultiPolygon] = self._union_tile_geoms(tiles)
        fc: FeatureCollection = self._to_feature_collection([(extent, None)])
        return self._upload_geom(fc, bucket, key)

    def upload_tile_geoms(
        self, tiles: List[Tile], bucket: str = utils.get_bucket(), key: str = None
    ) -> Dict[str, Any]:

        if key is None:
            key = os.path.join(self.layer.prefix, "tiles.geojson")
        geoms: Sequence[Tuple[Polygon, Dict[str, Any]]] = [
            (tile.dst.geom, {"name": tile.dst.uri}) for tile in tiles
        ]
        fc: FeatureCollection = self._to_feature_collection(geoms)
        return self._upload_geom(fc, bucket, key)

    @staticmethod
    def _union_tile_geoms(tiles: List[Tile]) -> Union[Polygon, MultiPolygon]:
        LOGGER.debug("Create Polygon from tile bounds")
        geoms: List[Polygon] = [tile.dst.geom for tile in tiles]
        return unary_union(geoms)

    @staticmethod
    def _to_feature_collection(
        geoms: Sequence[Tuple[Union[Polygon, MultiPolygon], Optional[Dict[str, Any]]]]
    ) -> FeatureCollection:

        features: List[Feature] = [
            Feature(geometry=item[0], properties=item[1]) for item in geoms
        ]
        return FeatureCollection(features)

    @staticmethod
    def _upload_geom(fc: FeatureCollection, bucket: str, key: str) -> Dict[str, Any]:
        LOGGER.info(f"Upload geometry to {bucket} {key}")
        return S3.put_object(Body=str.encode(dumps(fc)), Bucket=bucket, Key=key,)
