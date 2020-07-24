import json
import os
from typing import Any, Dict, Optional, List, Tuple, Union
from urllib.parse import urlparse

import boto3
from rasterio.warp import Resampling
from shapely.geometry import MultiPolygon, shape, Polygon
from shapely.ops import unary_union

from .models import LayerModel
from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.data_type import DataType, data_type_factory
from gfw_pixetl.grids import Grid, grid_factory
from gfw_pixetl.sources import VectorSource
from gfw_pixetl.resampling import resampling_factory

LOGGER = get_module_logger(__name__)


class Layer(object):
    def __init__(self, layer_def: LayerModel, grid: Grid) -> None:
        self.field: str = layer_def.pixel_meaning
        self.name: str = layer_def.dataset
        self.version: str = layer_def.version
        self.grid: Grid = grid

        self.prefix: str = self._get_prefix()

        if not os.path.exists(self.prefix):
            os.makedirs(self.prefix)

        self.dst_profile: Dict[str, Any] = self._get_dst_profile(layer_def, grid)

        self.resampling: Resampling = (
            resampling_factory(layer_def.resampling)
            if layer_def.resampling is not None
            else Resampling.nearest
        )
        self.calc: Optional[str] = layer_def.calc
        self.rasterize_method: Optional[str] = layer_def.rasterize_method
        self.order: Optional[str] = layer_def.order

    def _get_prefix(
        self,
        name: Optional[str] = None,
        version: Optional[str] = None,
        grid: Optional[Grid] = None,
        field: Optional[str] = None,
    ) -> str:
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

    @classmethod
    def _get_dst_profile(cls, layer_def: LayerModel, grid: Grid) -> Dict[str, Any]:
        nbits = layer_def.nbits
        no_data = layer_def.no_data

        data_type: DataType = data_type_factory(layer_def.data_type, nbits, no_data)

        dst_profile = {
            "dtype": data_type.data_type,
            "compress": data_type.compression,
            "tiled": True,
            "blockxsize": grid.blockxsize,
            "blockysize": grid.blockysize,
            "pixeltype": "SIGNEDBYTE" if data_type.signed_byte else "DEFAULT",
            "nodata": int(data_type.no_data) if data_type.no_data is not None else None,
        }

        if data_type.nbits:
            dst_profile.update({"nbits": int(data_type.nbits)})

        return dst_profile


class VectorSrcLayer(Layer):
    def __init__(self, layer_def: LayerModel, grid: Grid) -> None:
        super().__init__(layer_def, grid)
        self.src: VectorSource = VectorSource(table_name=self.name)


class RasterSrcLayer(Layer):
    def __init__(self, layer_def: LayerModel, grid: Grid) -> None:
        super().__init__(layer_def, grid)

        self._src_uri = layer_def.uri

        # self.input_files = self._input_files()
        # self.geom = self._geom()

    @property
    def input_files(self) -> List[Tuple[Polygon, str]]:
        s3 = boto3.resource("s3")
        input_files = list()

        o = urlparse(self._src_uri, allow_fragments=False)
        bucket: Union[str, bytes] = o.netloc
        prefix: str = str(o.path).lstrip("/")

        LOGGER.debug(
            f"Get input files for layer {self.name} using {str(bucket)} {prefix}"
        )
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


def layer_factory(layer_def: LayerModel) -> Layer:
    source_type: str = layer_def.source_type
    grid: Grid = grid_factory(layer_def.grid)

    if source_type == "vector":
        layer: Layer = VectorSrcLayer(layer_def, grid)
    elif source_type == "raster":
        layer = RasterSrcLayer(layer_def, grid)

    return layer
