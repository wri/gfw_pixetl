import csv
import math
import multiprocessing
import os
import subprocess as sp
from typing import Any, Callable, Dict, Iterator, List, Optional, Set

import boto3
import numpy as np
import rasterio
import yaml
from botocore.exceptions import ClientError
from parallelpipe import Stage
from retrying import retry


from gfw_pixetl import get_module_logger
from gfw_pixetl.data_type import DataType, data_type_factory
from gfw_pixetl.errors import GDALError, GDALNoneTypeError, retry_if_none_type_error
from gfw_pixetl.grid import Grid
from gfw_pixetl.tile import Tile, VectorSrcTile, RasterSrcTile
from gfw_pixetl.source import VectorSource, RasterSource

logger = get_module_logger(__name__)


class Layer(object):
    def __init__(
        self,
        name: str,
        version: str,
        field: str,
        grid: Grid,
        data_type: DataType,
        env: str,
        subset: Optional[List[str]] = None,
    ):

        if env == "dev":
            bucket = "gfw-data-lake-dev"
        else:
            bucket = "gfw-data-lake"

        srs_authority = grid.srs.to_authority()[0].lower()
        srs_code = grid.srs.to_authority()[1]

        self.base_name = f"{bucket}/{name}/{version}/raster/{srs_authority}-{srs_code}/{grid.width}x{grid.height}/{grid.xres}/{field}"
        if not os.path.exists(self.base_name):
            os.makedirs(self.base_name)
        self.name: str = name
        self.version: str = version
        self.data_type: DataType = data_type
        self.grid: Grid = grid
        self.uri: str = self.base_name + "/{tile_id}.tif"
        self.subset: Optional[List[str]] = subset
        self.workers: int = math.ceil(multiprocessing.cpu_count() / 2)

    def create_tiles(self, overwrite=True) -> None:
        raise NotImplementedError()

    def filter_subset_tiles(self, tiles: Iterator[Tile]) -> Iterator[Tile]:
        for tile in tiles:
            if not self.subset or (self.subset and tile.tile_id in self.subset):
                yield tile

    @staticmethod
    def filter_target_tiles(
        tiles: Iterator[Tile], overwrite: bool = True
    ) -> Iterator[Tile]:
        for tile in tiles:
            if overwrite or not tile.uri_exists():
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

            bucket = tile.uri.split("/")[0]
            obj = "/".join(tile.uri.split("/")[1:])

            s3 = boto3.client("s3")

            try:
                logger.info(f"Upload tile {tile.uri} to s3")
                s3.upload_file(tile.uri, bucket, obj)
            except ClientError:
                logger.exception(f"Could not upload file {tile.uri}")
                raise
            else:
                yield tile

    def delete_file(self, tiles: Iterator[Tile]) -> Iterator[Tile]:
        for tile in tiles:
            self._delete_file(tile.uri)
            yield tile

    @staticmethod
    def create_vrt(uris: List[str]) -> str:

        vrt = "all.vrt"
        tile_list = "tiles.txt"

        with open(tile_list, "w") as input_tiles:
            for uri in uris:
                tile_uri = f"/vsis3/{uri}\n"
                input_tiles.write(tile_uri)

        cmd = ["gdalbuildvrt", "-input_file_list", tile_list, vrt]

        logger.info("Create VRT file")
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        o, e = p.communicate()

        if p.returncode != 0:
            logger.error("Could not create VRT file")
            logger.exception(e)
            raise GDALError(e)
        else:
            return vrt

    @staticmethod
    def _delete_file(f: str) -> None:
        try:
            logger.info(f"Delete file {f}")
            os.remove(f)
        except Exception:
            logger.exception(f"Could not delete file {f}")
            raise


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
        subset: Optional[List[str]] = None,
    ):
        logger.debug("Initializing Vector layer")
        self.field: str = field
        self.order: str = order
        self.rasterize_method: str = rasterize_method
        self.src: VectorSource = VectorSource(f"{name}_{version}")

        super().__init__(name, version, field, grid, data_type, env, subset)
        logger.debug("Initialized Vector layer")

    def create_tiles(self, overwrite=True) -> None:

        logger.debug("Start Vector Pipe")

        pipe = (
            self.get_grid_tiles()
            | Stage(self.filter_subset_tiles).setup(workers=self.workers)
            | Stage(self.filter_src_tiles).setup(workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite).setup(
                workers=self.workers
            )
            | Stage(self.rasterize).setup(workers=self.workers, qsize=self.workers)
            | Stage(self.delete_if_empty).setup(workers=self.workers)
            | Stage(self.upload_file).setup(workers=self.workers)
            | Stage(self.delete_file).setup(workers=self.workers)
        )

        tile_uris: List[str] = list()
        for tile in pipe.results():
            tile_uris.append(tile.uri)

        # vrt: str = self.create_vrt(tile_uris)
        # TODO upload vrt to s3

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
                    f"select * from {self.name}_{self.version}__{tile.grid.name} where tile_id__{tile.grid.name} = '{tile.tile_id}'",
                    "-te",
                    str(tile.minx),
                    str(tile.miny),
                    str(tile.maxx),
                    str(tile.maxy),
                    "-tr",
                    str(tile.grid.xres),
                    str(tile.grid.yres),
                    "-a_srs",
                    "EPSG:4326",
                    "-ot",
                    self.data_type.data_type,
                ]
                + cmd_no_data
                + [
                    "-co",
                    f"COMPRESS={self.data_type.compression}",
                    "-co",
                    "TILED=YES",
                    "-co",
                    f"BLOCKXSIZE={tile.grid.blockxsize}",
                    "-co",
                    f"BLOCKYSIZE={tile.grid.blockxsize}",
                    # "-co", "SPARSE_OK=TRUE",
                    "-q",
                    self.src.conn.pg_conn,
                    tile.uri,
                ]
            )

            logger.info("Rasterize tile " + tile.tile_id)
            p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
            o, e = p.communicate()

            if p.returncode != 0:
                logger.error("Could not rasterize tile " + tile.tile_id)
                logger.exception(e)
                raise GDALError(e)
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
        resampling: str = "near",
        single_tile: bool = False,
        env: str = "dev",
        subset: Optional[List[str]] = None,
    ):
        logger.debug("Initializing Raster layer")
        self.resampling = resampling

        if single_tile:
            src_type = "single_tile"
        else:
            src_type = "tiled"
        self.src: RasterSource = RasterSource(src_uri, src_type)

        super().__init__(name, version, field, grid, data_type, env, subset)
        logger.debug("Initialized Raster layer")

    def create_tiles(self, overwrite=True) -> None:

        logger.debug("Start Raster Pipe")

        pipe = (
            self.get_grid_tiles()
            | Stage(self.filter_subset_tiles).setup(workers=self.workers)
            | Stage(self.filter_src_tiles).setup(workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite).setup(
                workers=self.workers
            )
            | Stage(self.transform).setup(workers=self.workers, qsize=self.workers)
            | Stage(self.delete_if_empty).setup(workers=self.workers)
            | Stage(self.upload_file).setup(workers=self.workers)
            | Stage(self.delete_file).setup(workers=self.workers)
        )

        tile_uris: List[str] = list()
        for tile in pipe.results():
            tile_uris.append(tile.uri)

        # vrt: str = self.create_vrt(tile_uris)
        # TODO upload vrt to s3

        logger.debug("Finished Raster Pipe")

    def get_grid_tiles(self) -> Set[RasterSrcTile]:
        logger.debug("Get grid Tiles")
        tiles = set()
        with open(
            os.path.join(os.path.dirname(__file__), "fixures/tiles.csv")
        ) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            for row in csv_reader:
                origin = self.grid.xyGridOrigin(int(row[2]), int(row[5]))
                tiles.add(
                    RasterSrcTile(
                        int(origin.x), int(origin.y), self.grid, self.src, self.uri
                    )
                )
        logger.debug(len(tiles))
        print(tiles)

        return tiles

    def filter_src_tiles(
        self, tiles: Iterator[RasterSrcTile]
    ) -> Iterator[RasterSrcTile]:
        for tile in tiles:
            if self.src.type == "tiled" and tile.src_tile_exists():
                yield tile
            elif self.src.type == "single_tile" and tile.src_tile_intersects():
                yield tile

    def transform(self, tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:

        if (
            self.data_type.no_data == 0 or self.data_type.no_data
        ):  # 0 evaluate as false, so need to list it here
            cmd_no_data: List[str] = ["-dstnodata", str(self.data_type.no_data)]
        else:
            cmd_no_data = list()

        for tile in tiles:

            cmd: List[str] = (
                [
                    "gdalwarp",
                    "-s_srs",
                    tile.src_profile["crs"].to_proj4(),
                    "-t_srs",
                    tile.grid.srs.srs,
                    "-ot",
                    self.data_type.data_type,
                ]
                + cmd_no_data
                + [
                    "-tr",
                    str(tile.grid.xres),
                    str(tile.grid.yres),
                    "-te",
                    str(tile.minx),
                    str(tile.miny),
                    str(tile.maxx),
                    str(tile.maxy),
                    "-te_srs",
                    tile.grid.srs.srs,
                    "-ovr",
                    "NONE",
                    "-co",
                    f"COMPRESS=NONE",  # {self.data_type.compression}",
                    "-co",
                    f"NBITS={self.data_type.nbits}",
                    "-co",
                    "TILED=YES",
                    "-co",
                    f"BLOCKXSIZE={tile.grid.blockxsize}",
                    "-co",
                    f"BLOCKYSIZE={tile.grid.blockysize}",
                    # "-co", "SPARSE_OK=TRUE",
                    "-r",
                    self.resampling,
                    "-q",
                    "-overwrite",
                    tile.src_uri,
                    tile.uri,
                ]
            )

            logger.info(f"Transform tile {tile.tile_id}")

            try:
                self._transform(cmd, tile)
            except GDALError as e:
                logger.error(f"Could not transform file {tile.uri}")
                logger.exception(e)
                raise
            else:
                yield tile

    @retry(
        retry_on_exception=retry_if_none_type_error,
        stop_max_attempt_number=7,
        wait_fixed=2000,
    )
    def _transform(self, cmd, tile):
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        o, e = p.communicate()

        if p.returncode != 0 and not e:
            raise GDALNoneTypeError(e)
        elif p.returncode != 0:
            raise GDALError(e)


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
        resampling: str = "near",
        single_tile: bool = False,
        env: str = "dev",
        subset: Optional[List[str]] = None,
    ):
        logger.debug("Initializing Calc Raster layer")

        self.calc = calc

        super().__init__(
            name,
            version,
            field,
            grid,
            data_type,
            src_uri,
            resampling,
            single_tile,
            env,
            subset,
        )
        self.workers: int = math.ceil(multiprocessing.cpu_count() / 3)
        logger.debug("Initialized Calc Raster layer")

    def create_tiles(self, overwrite=True) -> None:

        logger.debug("Start Calc Raster Pipe")

        pipe = (
            self.get_grid_tiles()
            | Stage(self.filter_subset_tiles).setup(workers=self.workers)
            | Stage(self.filter_src_tiles).setup(workers=self.workers)
            | Stage(self.filter_target_tiles, overwrite=overwrite).setup(
                workers=self.workers
            )
            | Stage(self.transform).setup(workers=self.workers, qsize=self.workers)
            | Stage(self.delete_calc_if_empty).setup(workers=self.workers)
            | Stage(self.calculate).setup(workers=self.workers, qsize=self.workers)
            | Stage(self.delete_calc).setup(workers=self.workers)
            # | Stage(self.set_no_data).setup(workers=self.workers)
            | Stage(self.upload_file).setup(workers=self.workers)
            | Stage(self.delete_file).setup(workers=self.workers)
        )

        tile_uris: List[str] = list()
        for tile in pipe.results():
            tile_uris.append(tile.uri)

        # vrt: str = self.create_vrt(tile_uris)
        # TODO upload vrt to s3

    logger.debug("Finished Raster Pipe")

    def transform(self, tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
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
                    "gdalwarp",
                    "-s_srs",
                    tile.src_profile["crs"].to_proj4(),
                    "-t_srs",
                    tile.grid.srs.srs,
                    # "-dstnodata",
                    # "None",  # ! important
                    "-tr",
                    str(tile.grid.xres),
                    str(tile.grid.yres),
                    "-te",
                    str(tile.minx),
                    str(tile.miny),
                    str(tile.maxx),
                    str(tile.maxy),
                    "-te_srs",
                    tile.grid.srs.srs,
                    "-ovr",
                    "NONE",
                    "-co",
                    "TILED=YES",
                    "-co",
                    f"BLOCKXSIZE={tile.grid.blockxsize}",
                    "-co",
                    f"BLOCKYSIZE={tile.grid.blockysize}",
                    # "-co", "SPARSE_OK=TRUE",
                    "-r",
                    self.resampling,
                    "-q",
                    "-overwrite",
                    tile.src_uri,
                    tile.calc_uri,
                ]
            )

            logger.info(f"Transform tile {tile.tile_id}")

            try:
                self._transform(cmd, tile)
            except GDALError as e:
                logger.error(f"Could not transform file {tile.uri}")
                logger.exception(e)
                raise
            else:
                yield tile

    def calculate(self, tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:

        for tile in tiles:
            logger.info(f"Calculate tile {tile.tile_id}")
            try:
                self._calc(tile)
            except Exception:
                logger.exception("Calculation failed")
                raise
            else:
                yield tile

    def _calc(self, tile: RasterSrcTile) -> None:
        with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
            src = rasterio.open(tile.calc_uri)

            kwargs = self._dst_meta(src.meta)

            dst = rasterio.open(tile.uri, "w", **kwargs)

            for block_index, window in src.block_windows(1):
                data = src.read(window=window, masked=True)
                data = self._apply_calc(data)
                data = self._set_no_data_calc(data)
                dst.write(data, window=window)
            src.close()
            dst.close()

    def _dst_meta(self, meta):
        kwargs = meta.copy()
        kwargs.update(
            {
                "dtype": self.data_type.to_numpy_dt(),
                "compress": self.data_type.compression,
                "tiled": True,
                "blockxsize": self.grid.blockxsize,
                "blockysize": self.grid.blockysize,
            }
        )
        if self.data_type.no_data == 0 or self.data_type.no_data:
            kwargs.update({"nodata": self.data_type.no_data})
        else:
            kwargs.update({"nodata": None})

        if self.data_type.nbits:
            kwargs.update({"nbits": self.data_type.nbits})

        return kwargs

    def _apply_calc(
        self, data
    ):  # can use type hints here b/c of the way we create function f from string. Mypy would thow an error
        # apply user submitted calculation

        funcstr = f"def f(A: np.ndarray) -> np.ndarray:\n    return {self.calc}"
        exec(funcstr, globals())
        return f(data)  # noqa: F821

    def _set_no_data_calc(self, data):
        # update no data value if wanted
        if self.data_type.no_data == 0 or self.data_type.no_data:
            data = np.ma.filled(data, self.data_type.no_data).astype(
                self.data_type.to_numpy_dt()
            )

        else:
            data = data.data.astype(self.data_type.to_numpy_dt())
        return data

    # def set_no_data(self, tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
    #     for tile in tiles:
    #         if self.data_type.no_data:
    #             cmd: List[str] = [
    #                 "gdal_edit.py",
    #                 "-a_nodata",
    #                 str(self.data_type.no_data),
    #                 tile.uri,
    #             ]
    #
    #             logger.info(f"Set No Data Value for file {tile.uri}")
    #             p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
    #             o, e = p.communicate()
    #
    #             if p.returncode != 0:
    #                 logger.error(f"Could not set No Data value for file {tile.uri}")
    #                 logger.exception(e)
    #                 raise GDALError(e)
    #             else:
    #                 yield tile

    def delete_calc(self, tiles: Iterator[RasterSrcTile]) -> Iterator[RasterSrcTile]:
        for tile in tiles:
            self._delete_file(tile.calc_uri)
            yield tile

    def delete_calc_if_empty(
        self, tiles: Iterator[RasterSrcTile]
    ) -> Iterator[RasterSrcTile]:
        for tile in tiles:
            if tile.calc_is_empty():
                self._delete_file(tile.calc_uri)
            else:
                yield tile


def layer_factory(layer_type, **kwargs) -> Layer:

    if layer_type == "vector":
        return _vector_layer_factory(**kwargs)

    elif layer_type == "raster":
        return _raster_layer_factory(**kwargs)
    else:
        raise ValueError(f"Unknown layer type: {layer_type}")


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
        message = "No such data layer"
        logger.exception(message)
        raise ValueError(message)

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
        message = "No such data layer"
        logger.exception(message)
        raise ValueError(message)

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
            message = "No such data field in source definition"
            logger.exception(message)
            raise ValueError(message)
        else:
            return sources[0]
    except KeyError:
        message = "No such data field in source definition"
        logger.exception()
        raise ValueError(message)


def _cur_dir():
    return os.path.dirname(os.path.abspath(__file__))
