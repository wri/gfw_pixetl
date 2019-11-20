import os
from typing import Any, Dict

import yaml

from gfw_pixetl import get_module_logger
from gfw_pixetl import utils
from gfw_pixetl.data_type import DataType, data_type_factory
from gfw_pixetl.grid import Grid
from gfw_pixetl.source import VectorSource, RasterSource


logger = get_module_logger(__name__)


class Layer(object):
    def __init__(self, name: str, version: str, field: str, grid: Grid):
        self.field = field
        self.name = name
        self.version = version
        self.grid = grid

        srs_authority = grid.srs.to_authority()[0].lower()
        srs_code = grid.srs.to_authority()[1]

        self.prefix = f"{name}/{version}/raster/{srs_authority}-{srs_code}/{grid.width}x{grid.height}/{field}"
        if not os.path.exists(self.prefix):
            os.makedirs(self.prefix)

        self._source: Dict[str, Any]
        self.dst_profile: Dict[str, Any]

        self._get_source()
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

    def _get_source(self):
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(cur_dir, "fixures/sources.yaml"), "r") as stream:
            sources: Dict[str, Any] = yaml.load(stream, Loader=yaml.BaseLoader)

        try:
            self._source = sources[self.name][self.field]
        except KeyError:
            message = "No such data layer"
            logger.exception(message)
            raise ValueError(message)

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
        self.src: RasterSource = utils.get_src(self._source["src"]["uri"])
