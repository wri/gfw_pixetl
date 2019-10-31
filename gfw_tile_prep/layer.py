import csv
import multiprocessing
import os
import subprocess as sp
from typing import Any, Dict, Iterator, List, Set

import yaml
from parallelpipe import Stage

from gfw_tile_prep import get_module_logger
from gfw_tile_prep.data_type import DataType, data_type_factory
from gfw_tile_prep.grid import Grid
from gfw_tile_prep.tile import Tile, VectorSrcTile, RasterSrcTile
from gfw_tile_prep.source import VectorSource, RasterSource

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

    def create_tiles(self, overwrite=True) -> None:
        raise NotImplementedError()

    @staticmethod
    def filter_target_tiles(
        tiles: Iterator[Tile], overwrite: bool = True
    ) -> Iterator[Tile]:
        for tile in tiles:
            if overwrite:
                if tile.uri_exists():
                    yield tile
            else:
                yield tile

    @staticmethod
    def delete_if_empty(tiles: Iterator[Tile]) -> Iterator[Tile]:
        for tile in tiles:
            if tile.is_empty():
                os.remove(tile.uri)
            else:
                yield tile

    @staticmethod
    def upload_file(tiles: Iterator[Tile]) -> Iterator[Tile]:

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


class VectorLayer(Layer):

    type = "vector"

    def __init__(
        self,
        name: str,
        version: str,
        field: str,
        grid: Grid,
        data_type: DataType,
        order: str = "asc",
        rasterize_method: str = "value",
        env: str = "dev",
    ):
        logger.debug("Initializing Vector layer")
        self.field: str = field
        self.order: str = order
        self.rasterize_method: str = rasterize_method
        self.src: VectorSource = VectorSource("{}_{}".format(name, version))

        super().__init__(name, version, field, grid, data_type, env)
        logger.debug("Initialized Vector layer")

    def create_tiles(self, overwrite=True) -> None:

        logger.debug("Start Vector Pipe")

        pipe = (
            self.get_grid_tiles()
            | Stage(self.filter_src_tiles, workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite, workers=self.workers)
            | Stage(self.rasterize, workers=self.workers, qsize=self.workers)
            | Stage(self.delete_if_empty, workers=self.workers)
            | Stage(self.upload_file, workers=self.workers)
            | Stage(self.delete_file, workers=self.workers)
        )

        for output in pipe.results():
            pass

        logger.debug("Start Finished Pipe")

    def get_grid_tiles(self) -> Set[VectorSrcTile]:
        tiles = set()
        with open(
            os.path.join(os.path.dirname(__file__), "fixures/tiles.csv")
        ) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            for row in csv_reader:
                tiles.add(
                    VectorSrcTile(
                        int(row[2]), int(row[5]), self.grid, self.src, self.uri
                    )
                )
        return tiles

    @staticmethod
    def filter_src_tiles(tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:
        for tile in tiles:
            if tile.src_vector_intersects():
                yield tile

    def rasterize(self, tiles: Iterator[VectorSrcTile]) -> Iterator[VectorSrcTile]:

        if self.rasterize_method == "count":
            cmd_method: List[str] = ["-burn", "1", "-add"]
        else:
            cmd_method = ["-a", self.field]

        if self.data_type.no_data:
            cmd_no_data: List[str] = ["-a_nodata", str(self.data_type.no_data)]
        else:
            cmd_no_data = list()

        for tile in tiles:

            logger.info("Create raster " + tile.uri)

            cmd: List[str] = (
                ["gdal_rasterize"]
                + cmd_method
                + [
                    "-sql",
                    "select * from {name}_{version}__1_1 where tile_id__{grid} = {tile_id}".format(
                        name=self.name,
                        version=self.version,
                        grid=self.grid.name,
                        tile_id=tile.tile_id,
                    ),
                    "-te",
                    str(tile.minx),
                    str(tile.miny),
                    str(tile.maxx),
                    str(tile.maxy),
                    "-tr",
                    str(self.grid.xres),
                    str(self.grid.yres),
                    "-a_srs",
                    "EPSG:4326",
                    "-ot",
                    self.data_type.data_type,
                ]
                + cmd_no_data
                + [
                    "-co",
                    "COMPRESS={}".format(self.data_type.compression),
                    "-co",
                    "TILED=YES",
                    "-co",
                    "BLOCKXSIZE={}".format(self.grid.blockxsize),
                    "-co",
                    "BLOCKYSIZE={}".format(self.grid.blockxsize),
                    # "-co", "SPARSE_OK=TRUE",
                    self.src.conn.pg_conn,
                    tile.uri,
                ]
            )
            try:
                logger.info("Rasterize tile " + tile.tile_id)
                sp.check_call(cmd)
            except sp.CalledProcessError as e:
                logger.warning("Could not rasterize file " + tile.uri)
                logger.warning(e)
            else:
                yield tile


class RasterLayer(Layer):
    type = "raster"

    def __init__(
        self,
        name: str,
        version: str,
        field: str,
        grid: Grid,
        data_type: DataType,
        src_path: str,
        resampling: str = "nearest",
        single_tile: bool = False,
        env: str = "dev",
    ):
        logger.debug("Initializing Raster layer")
        self.resampling = resampling

        if single_tile:
            src_type = "single_tile"
        else:
            src_type = "tiled"
        self.src: RasterSource = RasterSource(src_path, src_type)

        super().__init__(name, version, field, grid, data_type, env)
        logger.debug("Initialized Raster layer")

    def create_tiles(self, overwrite=True) -> None:

        logger.debug("Start Raster Pipe")

        pipe = (
            self.get_grid_tiles()
            | Stage(self.filter_src_tiles, workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite, workers=self.workers)
            | Stage(self.translate, workers=self.workers, qsize=self.workers)
            | Stage(self.delete_if_empty, workers=self.workers)
            | Stage(self.upload_file, workers=self.workers)
            | Stage(self.delete_file, workers=self.workers)
        )

        for output in pipe.results():
            pass

        logger.debug("Finished Raster Pipe")

    def get_grid_tiles(self) -> Set[RasterSrcTile]:
        tiles = set()
        with open(
            os.path.join(os.path.dirname(__file__), "fixures/tiles.csv")
        ) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            for row in csv_reader:
                tiles.add(
                    RasterSrcTile(
                        int(row[2]), int(row[5]), self.grid, self.src, self.uri
                    )
                )
        return tiles

    def filter_src_tiles(
        self, tiles: Iterator[RasterSrcTile]
    ) -> Iterator[RasterSrcTile]:
        for tile in tiles:
            if self.src.type == "tiled" and tile.src_tile_exists():
                yield tile
            elif self.src.type == "single_tile" and tile.src_tile_intersects():
                yield tile

    def translate(self, tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:

        if self.data_type.no_data:
            cmd_no_data: List[str] = ["-a_nodata", str(self.data_type.no_data)]
        else:
            cmd_no_data = list()

        for tile in tiles:

            cmd: List[str] = (
                ["gdal_translate", "-strict", "-ot", self.data_type.data_type]
                + cmd_no_data
                + [
                    "-tr",
                    str(self.grid.xres),
                    str(self.grid.yres),
                    "-projwin",
                    str(tile.minx),
                    str(tile.maxy),
                    str(tile.maxx),
                    str(tile.miny),
                    "-co",
                    "COMPRESS={}".format(self.data_type.compression),
                    "-co",
                    "TILED=YES",
                    "-co",
                    "BLOCKXSIZE={}".format(self.grid.blockxsize),
                    "-co",
                    "BLOCKYSIZE={}".format(self.grid.blockysize),
                    # "-co", "SPARSE_OK=TRUE",
                    "-r",
                    self.resampling,
                    tile.src.uri,
                    tile.uri,
                ]
            )

            try:
                logger.info("Translate tile " + tile.tile_id)
                sp.check_call(cmd)
            except sp.CalledProcessError as e:
                logger.warning("Could not translate file " + tile.uri)
                logger.warning(e)
            else:
                yield tile


def layer_factory(layer_type, **kwargs) -> Layer:

    if layer_type == "vector":
        return _vector_layer_factory(**kwargs)

    elif layer_type == "raster":
        return _raster_layer_factory(**kwargs)
    else:
        raise ValueError("Unknown layer type")


def _vector_layer_factory(**kwargs) -> VectorLayer:

    with open(os.path.join(_cur_dir(), "fixures/vector_sources.yaml"), "r") as stream:
        sources = yaml.load(stream, Loader=yaml.BaseLoader)
    try:
        source = _get_source_by_field(sources[kwargs["name"]], kwargs["field"])
    except KeyError:
        raise ValueError("No such data layer")

    kwargs["field"] = source["field"]
    kwargs["data_type"] = data_type_factory(**source)

    if "order" in source.keys():
        kwargs["order"] = source["order"]
    if "rasterize_method" in source.keys():
        kwargs["rasterize_method"] = source["rasterize_method"]

    return VectorLayer(**kwargs)


def _raster_layer_factory(**kwargs) -> RasterLayer:

    with open(os.path.join(_cur_dir(), "fixures/raster_sources.yaml"), "r") as stream:
        sources = yaml.load(stream, Loader=yaml.BaseLoader)

    try:
        source = _get_source_by_field(sources[kwargs["name"]], kwargs["field"])
    except KeyError:
        raise ValueError("No such data layer")

    kwargs["field"] = source["field"]
    kwargs["data_type"] = data_type_factory(**source)
    kwargs["src_uri"] = source["src_uri"]
    if "single_tile" in source.keys():
        kwargs["single_tile"] = source["single_tile"]
    if "resampling" in source.keys():
        kwargs["resampling"] = source["resampling"]

    return RasterLayer(**kwargs)


def _get_source_by_field(sources, field) -> Dict[str, Any]:

    try:
        if field:
            for source in sources:
                if source["field"] == field:
                    return source
            raise ValueError("No such data field in source definition")
        else:
            return sources[0]
    except KeyError:
        raise ValueError("No such data field in source definition")


def _cur_dir():
    return os.path.dirname(os.path.abspath(__file__))
