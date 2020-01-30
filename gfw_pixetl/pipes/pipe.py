import multiprocessing
import os
import subprocess as sp
from typing import Any, Dict, Iterator, List, Optional, Set, Union

import boto3
from geojson import FeatureCollection, Feature, dumps
from parallelpipe import stage
from shapely.geometry import box, Polygon, MultiPolygon

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.errors import GDALError
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
        utils.set_workers(tile_count)
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
        # logger.debug(tiles)

        return tiles

    @staticmethod
    @stage(workers=CORES)
    def filter_src_tiles():
        """
        Override this method when implementing pipes
        """
        raise NotImplementedError()

    @staticmethod
    @stage(workers=CORES)
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
    @stage(workers=CORES)
    def filter_target_tiles(
        tiles: Iterator[Tile], overwrite: bool = True
    ) -> Iterator[Tile]:
        """
        Don't process tiles if they already exists in target location,
        unless overwrite is set to True
        """
        for tile in tiles:
            if overwrite or not tile.dst_exists():
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
    @stage(workers=CORES)
    def upload_file(tiles: Iterator[Tile]) -> Iterator[Tile]:
        """
        Upload tile to target location
        """
        for tile in tiles:
            tile.upload()
            yield tile

    @staticmethod
    @stage(workers=CORES)
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
            tile_uris.append(tile.dst.uri)

        if len(tiles):
            self.upload_vrt(tile_uris)
            self.upload_extent(tiles)

        return tiles

    def upload_vrt(self, uris: List[str]) -> Dict[str, Any]:
        vrt = self._create_vrt(uris)
        return self._upload_vrt(vrt)

    def _create_vrt(self, uris: List[str]) -> str:
        """
        ! Important this is not a parallelpipe Stage and must be run with only one worker
        Create VRT file from input URI.
        """

        vrt = "all.vrt"
        tile_list = "tiles.txt"

        self._write_tile_list(tile_list, uris)

        cmd = ["gdalbuildvrt", "-input_file_list", tile_list, vrt]
        env = utils.set_aws_credentials()

        LOGGER.info("Create VRT file")
        p: sp.Popen = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, env=env)

        o: Any
        e: Any
        o, e = p.communicate()

        os.remove(tile_list)

        if p.returncode != 0:
            LOGGER.error("Could not create VRT file")
            LOGGER.exception(e)
            raise GDALError(e)
        else:
            return vrt

    def _upload_vrt(self, vrt):
        LOGGER.info("Upload vrt")
        return S3.upload_file(
            vrt, utils.get_bucket(), os.path.join(self.layer.prefix, vrt)
        )

    def upload_extent(self, tiles: List[Tile]) -> Dict[str, Any]:
        extent: Optional[Union[Polygon, MultiPolygon]] = self._to_polygon(tiles)
        fc: FeatureCollection = self._to_feature_collection(extent)
        return self._upload_extent(fc)

    def _to_polygon(self, tiles: List[Tile]) -> Optional[Union[Polygon, MultiPolygon]]:
        LOGGER.debug("Create Polygon from tile bounds")
        extent: Optional[Union[Polygon, MultiPolygon]] = None
        for tile in tiles:
            geom: Polygon = self._bounds_to_polygon(tile.bounds)
            if not extent:
                extent = geom
            else:
                extent = extent.union(geom)
        return extent

    @staticmethod
    def _to_feature_collection(geom: Polygon) -> FeatureCollection:
        feature: Feature = Feature(geometry=geom)
        return FeatureCollection([feature])

    def _upload_extent(self, fc: FeatureCollection) -> Dict[str, Any]:
        LOGGER.info("Upload extent")
        return S3.put_object(
            Body=str.encode(dumps(fc)),
            Bucket=utils.get_bucket(),
            Key=os.path.join(self.layer.prefix, "extent.geojson"),
        )

    @staticmethod
    def _write_tile_list(tile_list: str, uris: List[str]) -> None:
        with open(tile_list, "w") as input_tiles:
            for uri in uris:
                tile_uri = f"/vsis3/{utils.get_bucket()}/{uri}\n"
                input_tiles.write(tile_uri)

    @staticmethod
    def _bounds_to_polygon(bounds: box) -> Polygon:
        return Polygon(
            [
                (bounds[0], bounds[1]),
                (bounds[2], bounds[1]),
                (bounds[2], bounds[3]),
                (bounds[0], bounds[3]),
                (bounds[0], bounds[1]),
            ]
        )
