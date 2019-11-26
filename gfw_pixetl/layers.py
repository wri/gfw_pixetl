import os
from typing import Any, Dict, Optional

import yaml

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.data_type import DataType, data_type_factory
from gfw_pixetl.grids import Grid, grid_factory
from gfw_pixetl.sources import VectorSource, RasterSource, get_src


logger = get_module_logger(__name__)


class Layer(object):
    def __init__(self, name: str, version: str, field: str, grid: Grid):
        self.field = field
        self.name = name
        self.version = version
        self.grid = grid

        self.prefix = self._get_prefix()

        if not os.path.exists(self.prefix):
            os.makedirs(self.prefix)

        self._source: Dict[str, Any] = _get_source(self.name, self.field)
        self.dst_profile: Dict[str, Any]

        self._set_dst_profile()

        self.resampling = (
            self._source["resampling"] if "resampling" in self._source.keys() else None
        )
        self.calc = self._source["calc"] if "calc" in self._source.keys() else None
        self.rasterize_method = (
            self._source["rasterize_method"]
            if "rasterize_method" in self._source.keys()
            else None
        )
        self.order = self._source["order"] if "order" in self._source.keys() else None

    def _get_prefix(
        self,
        name: Optional[str] = None,
        version: Optional[str] = None,
        grid: Optional[Grid] = None,
        field: Optional[str] = None,
    ):
        if not name:
            name = self.name
        if not version:
            version = self.version
        if not grid:
            grid = self.grid
        if not field:
            field = self.field

        srs_authority = grid.srs.to_authority()[0].lower()
        srs_code = grid.srs.to_authority()[1]

        return f"{name}/{version}/raster/{srs_authority}-{srs_code}/{grid.width}/{grid.cols}/{field}"

    def _set_dst_profile(self):
        data_type: DataType = data_type_factory(self._source["data_type"])

        self.dst_profile: Dict[str, Any] = {
            "dtype": data_type.to_numpy_dt(),
            "compress": data_type.compression,
            "tiled": True,
            "blockxsize": self.grid.blockxsize,
            "blockysize": self.grid.blockysize,
        }

        if data_type.no_data == 0 or data_type.no_data:
            self.dst_profile.update({"nodata": data_type.no_data})
        else:
            self.dst_profile.update({"nodata": None})

        if data_type.nbits:
            self.dst_profile.update({"nbits": data_type.nbits})


class VectorSrcLayer(Layer):
    def __init__(self, name: str, version: str, field: str, grid: Grid):
        super().__init__(name, version, field, grid)
        self.src: VectorSource = VectorSource(table_name=self.name)


class RasterSrcLayer(Layer):
    def __init__(self, name: str, version: str, field: str, grid: Grid):
        super().__init__(name, version, field, grid)

        if "depends_on" in self._source["grids"][grid.name].keys():
            src_name, src_field, src_grid_name = self._source["grids"][grid.name][
                "depends_on"
            ].split("/")
            src_grid = grid_factory(src_grid_name)
            prefix = self._get_prefix(
                name=src_name, version=version, field=src_field, grid=src_grid
            )
            bucket = utils.get_bucket()

            src_uri = f"/vsis3/{bucket}/{prefix}/all.vrt"
        else:
            src_uri = self._source["grids"][grid.name]["uri"]

        try:
            self.src: RasterSource = get_src(src_uri)
        except FileNotFoundError:
            message = f"The source file {src_uri} for layer {self.name}/{self.field} does not exist"
            logger.error(message)
            raise FileNotFoundError(message)


def layer_factory(name: str, version: str, field: str, grid: Grid) -> Layer:

    source_type: str = _get_source_type(name, field, grid.name)

    if source_type == "vector":
        layer: Layer = VectorSrcLayer(name, version, field, grid)
    elif source_type == "raster":
        layer = RasterSrcLayer(name, version, field, grid)
    else:
        raise NotImplementedError("Unknown source type")

    return layer


def _get_source(name: str, field: str) -> Dict[str, Any]:
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cur_dir, "fixures/sources.yaml"), "r") as stream:
        sources: Dict[str, Any] = yaml.load(stream, Loader=yaml.BaseLoader)
    try:
        return sources[name][field]
    except KeyError:
        message = "No such data layer"
        logger.exception(message)
        raise ValueError(message)


def _get_source_type(name: str, field: str, grid_name: str) -> str:
    source = _get_source(name, field)
    try:
        return source["grids"][grid_name]["type"]
    except KeyError:
        message = "Selected grid is not supported"
        logger.exception(message)
        raise ValueError(message)
