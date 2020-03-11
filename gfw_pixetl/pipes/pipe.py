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

        for tile in pipe.results():
            if tile.status == "pending":
                tile.status = "processed"
                tiles.append(tile)
            elif tile.status == "failed":
                failed_tiles.append(tile)
            else:
                skipped_tiles.append(tile)

        if len(tiles):
            self.upload_vrt(tiles)
            self.upload_geom(tiles)
            self.upload_tile_geoms(tiles)

        if len(skipped_tiles):
            LOGGER.warning(f"The following tiles were skipped: {skipped_tiles}")
        if len(failed_tiles):
            LOGGER.warning(f"The following tiles failed to process: {failed_tiles}")

        return tiles, skipped_tiles, failed_tiles

    def upload_vrt(self, tiles: List[Tile]) -> List[Dict[str, Any]]:
        response = list()
        uris: Dict[str, List[str]] = dict()
        bucket: str = utils.get_bucket()

        for tile in tiles:
            for key in tile.dst.keys():
                if key not in uris.keys():
                    uris[key] = list()
                uris[key].append(f"/vsis3/{bucket}/{tile.dst[key].uri}")

        for key in uris.keys():
            vrt = utils.create_vrt(uris[key])
            response.append(self._upload_vrt(key, vrt))

        return response

    def _upload_vrt(self, key, vrt):
        LOGGER.info("Upload vrt")
        return S3.upload_file(
            vrt, utils.get_bucket(), os.path.join(self.layer.prefix, key, vrt)
        )

    def upload_geom(
        self,
        tiles: List[Tile],
        bucket: str = utils.get_bucket(),
        forced_key: str = None,
    ) -> List[Dict[str, Any]]:

        fc: FeatureCollection
        response: List[Dict[str, Any]] = list()

        extent: Dict[str, Union[Polygon, MultiPolygon]] = self._union_tile_geoms(tiles)
        for dst_format in extent.keys():
            fc = self._to_feature_collection([(extent[dst_format], None)])
            if (
                forced_key is None
            ):  # hack used in source_prep, TODO: find a more elegant way
                key = os.path.join(self.layer.prefix, dst_format, "extent.geojson")
            else:
                key = forced_key
            response.append(self._upload_geom(fc, bucket, key))
        return response

    def upload_tile_geoms(
        self,
        tiles: List[Tile],
        bucket: str = utils.get_bucket(),
        forced_key: str = None,
    ) -> List[Dict[str, Any]]:

        fc: FeatureCollection
        response: List[Dict[str, Any]] = list()

        geoms: Dict[
            str, List[Tuple[Polygon, Dict[str, Any]]]
        ] = self._collect_tile_geoms(tiles)
        for dst_format in geoms.keys():
            fc = self._to_feature_collection(geoms[dst_format])
            if (
                forced_key is None
            ):  # hack used in source_prep, TODO: find a more elegant way
                key = os.path.join(self.layer.prefix, dst_format, "tiles.geojson")
            else:
                key = forced_key
            response.append(self._upload_geom(fc, bucket, key))
        return response

    @staticmethod
    def _collect_tile_geoms(
        tiles: List[Tile],
    ) -> Dict[str, List[Tuple[Polygon, Dict[str, Any]]]]:
        LOGGER.debug("Collect Polygon from tile bounds")

        geoms: Dict[str, List[Tuple[Polygon, Dict[str, Any]]]] = dict()

        for tile in tiles:
            for dst_format in tile.dst.keys():
                if dst_format not in geoms.keys():
                    geoms[dst_format] = list()
                geoms[dst_format].append(
                    (tile.dst[dst_format].geom, {"name": tile.dst[dst_format].uri})
                )

        return geoms

    @staticmethod
    def _union_tile_geoms(tiles: List[Tile]) -> Dict[str, Union[Polygon, MultiPolygon]]:
        LOGGER.debug("Create Polygon from tile bounds")

        geoms: Dict[str, Union[Polygon, MultiPolygon]] = dict()
        polygons: Dict[str, List[Polygon]] = dict()
        for tile in tiles:
            for dst_format in tile.dst.keys():
                if dst_format not in polygons.keys():
                    polygons[dst_format] = list()
                polygons[dst_format].append(tile.dst[dst_format].geom)

        for dst_format in polygons.keys():
            geoms[dst_format] = unary_union(polygons[dst_format])

        return geoms

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
