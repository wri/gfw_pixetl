import json
import os
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import urlparse

import boto3
import yaml
from rasterio.warp import Resampling
from shapely.geometry import MultiPolygon, shape, Polygon
from shapely.ops import unary_union

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.data_type import DataType, data_type_factory
from gfw_pixetl.grids import Grid, grid_factory
from gfw_pixetl.sources import VectorSource
from gfw_pixetl.resampling import resampling_factory

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
            resampling_factory(source_grid["resampling"])
            if "resampling" in source_grid.keys()
            else Resampling.nearest
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

        return os.path.join(
            name,
            version,
            "raster",
            f"{srs_authority}-{srs_code}",
            f"{grid.width}",
            f"{grid.cols}",
            field,
        )

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

            self._src_uri = f"s3://{bucket}/{prefix}/geotiff/tiles.geojson"
        else:
            self._src_uri = self._source["grids"][grid.name]["uri"]

        # self.input_files = self._input_files()
        # self.geom = self._geom()

    @property
    def input_files(self) -> List[Tuple[Polygon, str]]:
        s3 = boto3.resource("s3")
        input_files = list()

        o = urlparse(self._src_uri, allow_fragments=False)
        bucket: str = o.netloc
        prefix: str = o.path.lstrip("/")

        LOGGER.debug(f"Get input files for layer {self.name} using {bucket} {prefix}")
        obj = s3.Object(bucket, prefix)
        body = obj.get()["Body"].read()

        features = json.loads(body.decode("utf-8"))["features"]
        for feature in features:
            input_files.append(
                (shape(feature["geometry"]), feature["properties"]["name"])
            )
        return input_files

    @property
    def geom(self) -> MultiPolygon:
        LOGGER.debug("Create Polygon from input tile bounds")
        geoms: List[Polygon] = [tile[0] for tile in self.input_files]
        return unary_union(geoms)


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
    with open(os.path.join(cur_dir, "fixures", "sources.yaml"), "r") as stream:
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
