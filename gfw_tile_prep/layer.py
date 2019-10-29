import csv
import logging
import os
import subprocess as sp
from typing import Iterator, List, Set


from parallelpipe import stage

from gfw_tile_prep.data_type import DataType
from gfw_tile_prep.grid import Grid
from gfw_tile_prep.tile import Tile
from gfw_tile_prep.source import Source

logger = logging.getLogger(__name__)


class Layer(object):
    def __init__(
        self,
        name: str,
        version: str,
        field: str,
        grid: Grid,
        data_type: DataType,
        src: Source,
    ):
        base_name = "gfw-data-lake/{name}/{version}/raster/{srs_authority}-{srs_code}/{width}x{height}/{resolution}/{field}".format(
            name=name,
            version=version,
            srs_authority=grid.srs.to_authority()[0].lower(),
            srs_code=grid.srs.to_authority()[1],
            width=grid.width,
            height=grid.height,
            resolution=grid.xres,
            field=field,
        )
        self.name = name
        self.version = version
        self.data_type: DataType = data_type
        self.grid = grid
        self.uri = base_name + "/{tile_id}.tif"
        self.src = src

    def get_grid_tiles(self) -> Set[Tile]:
        tiles = set()
        with open(os.path.join(os.path.dirname(__file__), "csv/tiles.csv")) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            for row in csv_reader:
                tiles.add(Tile(row[2], row[5], self.grid, self.src, self.uri))
        return tiles

    @staticmethod
    @stage
    def filter_target_tiles(
        tiles: Iterator[Tile], overwrite: bool = True
    ) -> Iterator[Tile]:
        for tile in tiles:
            if tile.uri_exists() and overwrite:
                yield tile

    @stage
    def delete_if_empty(self, tiles: Iterator[Tile]) -> Iterator[Tile]:
        for tile in tiles:
            if tile.is_empty():
                os.remove(tile.uri)
            else:
                yield tile

    @stage
    def upload_file(self, tiles: Iterator[Tile]) -> Iterator[Tile]:

        for tile in tiles:
            s3_path = "s3://" + tile.uri
            cmd: List[str] = ["aws", "s3", "cp", tile.uri, s3_path]
            try:
                logger.info("Upload to " + s3_path)
                sp.check_call(cmd)
            except sp.CalledProcessError as e:
                logger.warning("Could not upload file " + tile.uri)
                logger.warning(e)
            else:
                yield tile

    @staticmethod
    @stage
    def delete_file(tiles: Iterator[Tile]) -> Iterator[Tile]:
        for tile in tiles:
            try:
                logger.info("Delete file " + tile.uri)
                os.remove(tile.uri)
            except Exception as e:
                logger.error("Could not delete file " + tile.uri)
                logger.error(e)
                yield tile
