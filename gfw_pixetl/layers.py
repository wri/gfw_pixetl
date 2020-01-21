import os
from typing import Any, Dict, Optional

import yaml

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.data_type import DataType, data_type_factory
from gfw_pixetl.grids import Grid, grid_factory
from gfw_pixetl.sources import VectorSource, RasterSource, get_src


LOGGER = get_module_logger(__name__)


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
        source_grid = self._source["grids"][grid.name]
        self.dst_profile: Dict[str, Any]

        self._set_dst_profile()

        self.resampling = (
            source_grid["resampling"] if "resampling" in source_grid.keys() else None
        )
        self.calc = source_grid["calc"] if "calc" in source_grid.keys() else None
        self.rasterize_method = (
            source_grid["rasterize_method"]
            if "rasterize_method" in source_grid.keys()
            else None
        )
        self.order = source_grid["order"] if "order" in source_grid.keys() else None

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

        nbits = self._source["nbits"] if "nbits" in self._source else None
        no_data = self._source["no_data"] if "no_data" in self._source else None

        data_type: DataType = data_type_factory(
            self._source["data_type"], nbits, no_data
        )

        self.dst_profile: Dict[str, Any] = {
            "dtype": data_type.data_type,
            "compress": data_type.compression,
            "tiled": True,
            "blockxsize": self.grid.blockxsize,
            "blockysize": self.grid.blockysize,
            "pixeltype": "SIGNEDBYTE" if data_type.signed_byte else "DEFAULT",
            "nodata": int(data_type.no_data) if data_type.has_no_data() else None,
        }

        if data_type.nbits:
            self.dst_profile.update({"nbits": int(data_type.nbits)})


class VectorSrcLayer(Layer):
    def __init__(self, name: str, version: str, field: str, grid: Grid):
        super().__init__(name, version, field, grid)
        self.src: VectorSource = VectorSource(table_name=self.name)


class RasterSrcLayer(Layer):
    def __init__(self, name: str, version: str, field: str, grid: Grid):
        super().__init__(name, version, field, grid)

        if "depends_on" in self._source["grids"][grid.name].keys():
            src_name, src_field, src_grid_width, src_grid_cols = self._source["grids"][
                grid.name
            ]["depends_on"].split("/")
            src_grid = grid_factory("/".join([src_grid_width, src_grid_cols]))
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
            LOGGER.error(message)
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
        sources: Dict[str, Any] = yaml.safe_load(stream)
    try:
        return sources[name][field]
    except KeyError:
        message = "No such data layer"
        LOGGER.exception(message)
        raise ValueError(message)


def _get_source_type(name: str, field: str, grid_name: str) -> str:
    source = _get_source(name, field)
    try:
        return source["grids"][grid_name]["type"]
    except KeyError:
        message = "Selected grid is not supported"
        LOGGER.exception(message)
        raise ValueError(message)
