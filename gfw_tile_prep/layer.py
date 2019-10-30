import csv
import logging
import multiprocessing
import os
import subprocess as sp
from typing import Iterator, List, Set

from gfw_tile_prep import get_module_logger
from gfw_tile_prep.data_type import DataType
from gfw_tile_prep.grid import Grid
from gfw_tile_prep.tile import Tile

logger = get_module_logger(__name__)


class Layer(object):

    workers = multiprocessing.cpu_count()

    def __init__(
        self,
        name: str,
        version: str,
        field: str,
        grid: Grid,
        data_type: DataType,
        src,
        env: str,
    ):

        if env == "dev":
            bucket = "gfw-data-lake-dev"
        else:
            bucket = "gfw-data-lake"

        base_name = "{bucket}/{name}/{version}/raster/{srs_authority}-{srs_code}/{width}x{height}/{resolution}/{field}".format(
            bucket=bucket,
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

    def create_tiles(self, overwrite=True) -> None:
        raise NotImplementedError()

    def get_grid_tiles(self) -> Set[Tile]:
        tiles = set()
        with open(
            os.path.join(os.path.dirname(__file__), "fixures/tiles.csv")
        ) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            for row in csv_reader:
                tiles.add(Tile(int(row[2]), int(row[5]), self.grid, self.src, self.uri))
        return tiles

    @staticmethod
    def filter_target_tiles(
        tiles: Iterator[Tile], overwrite: bool = True
    ) -> Iterator[Tile]:
        for tile in tiles:
            if tile.uri_exists() and overwrite:
                yield tile

    def delete_if_empty(self, tiles: Iterator[Tile]) -> Iterator[Tile]:
        for tile in tiles:
            if tile.is_empty():
                os.remove(tile.uri)
            else:
                yield tile

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
    def delete_file(tiles: Iterator[Tile]) -> Iterator[Tile]:
        for tile in tiles:
            try:
                logger.info("Delete file " + tile.uri)
                os.remove(tile.uri)
            except Exception as e:
                logger.error("Could not delete file " + tile.uri)
                logger.error(e)
                yield tile
