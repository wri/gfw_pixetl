import csv
import logging
import os
import subprocess as sp
from typing import Iterator, List, Set

from parallelpipe import stage

from gfw_tile_prep.data_type import DataType
from gfw_tile_prep.data_type_factory import data_type_factory
from gfw_tile_prep.grid import Grid
from gfw_tile_prep.tile import Tile

logger = logging.getLogger(__name__)


class Layer(object):
    def __init__(
        self,
        name,
        version,
        value,
        src_type,
        src_path,
        grid: Grid,
        data_type,
        no_data,
        nbits,
    ):
        base_name = "gfw-data-lake/{name}/{version}/raster/{srs_authority}-{srs_code}/{width}x{height}/{resolution}/{version}".format(
            name=name,
            version=version,
            srs_authority=grid.srs.to_authority()[0].lower(),
            srs_code=grid.srs.to_authority()[1],
            width=grid.width,
            height=grid.height,
            resolution=grid.xres,
            value=value,
        )
        self.name = name
        self.version = version
        self.data_type: DataType = data_type_factory(
            data_type, no_data=no_data, nbits=nbits
        )
        self.grid = grid
        self.src_type = src_type
        self.src_path = src_path
        self.s3_path = base_name + "/{tile_id}.tif"

    def get_grid_tiles(self) -> Set[Tile]:
        tiles = set()
        with open(os.path.join(os.path.dirname(__file__), "csv/tiles.csv")) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            for row in csv_reader:
                tiles.add(Tile(row[2], row[5], self))
        return tiles

    @staticmethod
    @stage
    def filter_target_tiles(tiles, overwrite=True) -> Iterator[Tile]:
        for tile in tiles:
            if tile.target_exists() and overwrite:
                yield tile

    @stage
    def delete_if_empty(self, tiles) -> Iterator[Tile]:
        for tile in tiles:
            if tile.is_empty():
                os.remove(tile.uri)
            else:
                yield tile

    @stage
    def upload_file(self, tiles) -> Iterator[Tile]:

        for tile in tiles:
            s3_path = "s3://" + tile.uri
            cmd: List[str] = ["aws", "s3", "cp", tile.uri, s3_path]
            try:
                logger.info("Upload to " + s3_path)
                sp.check_call(cmd)
            except sp.CalledProcessError as e:
                logger.warning("Could not upload file " + tile)
                logger.warning(e)
            else:
                yield tile

    @staticmethod
    @stage
    def delete_file(tiles) -> Iterator[Tile]:
        for tile in tiles:
            try:
                logger.info("Delete file " + tile.uri)
                os.remove(tile.uri)
            except Exception as e:
                logger.error("Could not delete file " + tile.uri)
                logger.error(e)
                yield tile
