import csv
import multiprocessing
import os
import subprocess as sp
from typing import Any, Dict, Iterator, List, Set

import yaml
from parallelpipe import Stage

from gfw_pixetl import get_module_logger
from gfw_pixetl.data_type import DataType, data_type_factory
from gfw_pixetl.grid import Grid
from gfw_pixetl.tile import Tile, VectorSrcTile, RasterSrcTile
from gfw_pixetl.source import VectorSource, RasterSource

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

        self.base_name = "{bucket}/{name}/{version}/raster/{srs_authority}-{srs_code}/{width}x{height}/{resolution}/{field}".format(
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
        if not os.path.exists(self.base_name):
            os.makedirs(self.base_name)
        self.name = name
        self.version = version
        self.data_type: DataType = data_type
        self.grid = grid
        self.uri = self.base_name + "/{tile_id}.tif"

    def create_tiles(self, overwrite=True) -> None:
        raise NotImplementedError()

    @staticmethod
    def filter_target_tiles(
        tiles: Iterator[Tile], overwrite: bool = True
    ) -> Iterator[Tile]:
        for tile in tiles:
            if not overwrite:
                if not tile.uri_exists():
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
            | Stage(self.filter_src_tiles).setup(workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite).setup(
                workers=self.workers
            )
            | Stage(self.rasterize).setup(workers=self.workers, qsize=self.workers)
            | Stage(self.delete_if_empty).setup(workers=self.workers)
            | Stage(self.upload_file).setup(workers=self.workers)
            | Stage(self.delete_file).setup(workers=self.workers)
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
                    "select * from {name}_{version}__{grid} where tile_id__{grid} = '{tile_id}'".format(
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
                    "-q",
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
    def __init__(
        self,
        name: str,
        version: str,
        field: str,
        grid: Grid,
        data_type: DataType,
        src_uri: str,
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
        self.src: RasterSource = RasterSource(src_uri, src_type)

        super().__init__(name, version, field, grid, data_type, env)
        logger.debug("Initialized Raster layer")

    def create_tiles(self, overwrite=True) -> None:

        logger.debug("Start Raster Pipe")

        pipe = (
            self.get_grid_tiles()
            | Stage(self.filter_src_tiles).setup(workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite).setup(
                workers=self.workers
            )
            | Stage(self.translate).setup(workers=self.workers, qsize=self.workers)
            | Stage(self.delete_if_empty).setup(workers=self.workers)
            | Stage(self.upload_file).setup(workers=self.workers)
            | Stage(self.delete_file).setup(workers=self.workers)
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
                    "NBITS={}".format(self.data_type.nbits),
                    "-co",
                    "TILED=YES",
                    "-co",
                    "BLOCKXSIZE={}".format(self.grid.blockxsize),
                    "-co",
                    "BLOCKYSIZE={}".format(self.grid.blockysize),
                    # "-co", "SPARSE_OK=TRUE",
                    "-r",
                    self.resampling,
                    "-q",
                    tile.src_uri,
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


class CalcRasterLayer(RasterLayer):
    def __init__(
        self,
        name: str,
        version: str,
        field: str,
        grid: Grid,
        data_type: DataType,
        src_uri: str,
        calc: str,
        resampling: str = "nearest",
        single_tile: bool = False,
        env: str = "dev",
    ):
        logger.debug("Initializing Calc Raster layer")

        self.calc = calc

        super().__init__(
            name, version, field, grid, data_type, src_uri, resampling, single_tile, env
        )
        logger.debug("Initialized Calc Raster layer")

    def create_tiles(self, overwrite=True) -> None:

        logger.debug("Start TCD Raster Pipe")

        pipe = (
            self.get_grid_tiles()
            | Stage(self.filter_src_tiles).setup(workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite).setup(
                workers=self.workers
            )
            | Stage(self.translate).setup(workers=self.workers, qsize=self.workers)
            | Stage(self.calculate).setup(workers=self.workers, qsize=self.workers)
            | Stage(self.delete_calc_file).setup(workers=self.workers)
            | Stage(self.delete_if_empty).setup(workers=self.workers)
            | Stage(self.upload_file).setup(workers=self.workers)
            | Stage(self.delete_file).setup(workers=self.workers)
        )

        for output in pipe.results():
            pass

        logger.debug("Finished Raster Pipe")

    def translate(self, tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        """
        In this version, we only create the tile using the correct extent and pixel size.
        We do not change the data type, compression or nbits value
        We need the keep the ordinal data type, since we still want to change the values afterwards
        We also unset the no data value, otherwise gdal_calc in the next step will use the default no data value
        in case one of the calculated values is equal to user submitted No Data value
        """

        for tile in tiles:

            cmd: List[str] = (
                [
                    "gdal_translate",
                    "-strict",
                    "-a_nodata",
                    "none",  # ! important
                    "-tr",
                    str(self.grid.xres),
                    str(self.grid.yres),
                    "-projwin",
                    str(tile.minx),
                    str(tile.maxy),
                    str(tile.maxx),
                    str(tile.miny),
                    "-co",
                    "TILED=YES",
                    "-co",
                    "BLOCKXSIZE={}".format(self.grid.blockxsize),
                    "-co",
                    "BLOCKYSIZE={}".format(self.grid.blockysize),
                    "-r",
                    self.resampling,
                    "-q",
                    tile.src_uri,
                    tile.calc_uri,
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

    def calculate(self, tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:

        if self.data_type.no_data:
            cmd_no_data: List[str] = ["--NoDataValue", str(self.data_type.no_data)]
        else:
            cmd_no_data = list()

        for tile in tiles:

            calc_uri = os.path.join(self.base_name, tile.tile_id + "__calc.tif")

            cmd: List[str] = (
                ["gdal_calc.py", "--type", self.data_type.data_type]
                + cmd_no_data
                + [
                    "-A",
                    tile.calc_uri,
                    "--calc={}".format(self.calc),
                    "--outfile={}".format(tile.uri),
                    "--co",
                    "COMPRESS={}".format(self.data_type.compression),
                    "--co",
                    "NBITS={}".format(self.data_type.nbits),
                    "--co",
                    "TILED=YES",
                    "--co",
                    "BLOCKXSIZE={}".format(self.grid.blockxsize),
                    "--co",
                    "BLOCKYSIZE={}".format(self.grid.blockysize),
                    "--quiet",
                ]
            )

            try:
                logger.info("Calculate tile " + tile.tile_id)
                sp.check_call(cmd)
            except sp.CalledProcessError as e:
                logger.exception("Could not calculate file " + calc_uri)
                raise e
            else:
                yield tile

    @staticmethod
    def delete_calc_file(tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        for tile in tiles:
            try:
                logger.info("Delete file " + tile.calc_uri)
                os.remove(tile.calc_uri)
            except Exception as e:
                logger.exception("Could not delete file " + tile.calc_uri)
                raise e
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
        sources: Dict[str, Any] = yaml.load(stream, Loader=yaml.BaseLoader)

    kwargs = _enrich_vector_kwargs(sources, **kwargs)

    return VectorLayer(**kwargs)


def _raster_layer_factory(**kwargs) -> RasterLayer:
    with open(os.path.join(_cur_dir(), "fixures/raster_sources.yaml"), "r") as stream:
        sources: Dict[str, Any] = yaml.load(stream, Loader=yaml.BaseLoader)

    kwargs = _enrich_raster_kwargs(sources, **kwargs)
    if "calc" in kwargs.keys():
        return CalcRasterLayer(**kwargs)
    else:
        return RasterLayer(**kwargs)


def _enrich_vector_kwargs(sources: Dict[str, Any], **kwargs) -> Dict[str, Any]:

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

    return kwargs


def _enrich_raster_kwargs(sources: Dict[str, Any], **kwargs) -> Dict[str, Any]:

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
    if "calc" in source.keys():
        kwargs["calc"] = source["calc"]

    return kwargs


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
