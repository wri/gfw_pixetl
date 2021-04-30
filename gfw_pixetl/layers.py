import json
import os
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from geojson import FeatureCollection
from rasterio.warp import Resampling
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely.ops import unary_union

from gfw_pixetl import get_module_logger
from gfw_pixetl.data_type import DataType, data_type_factory
from gfw_pixetl.grids import Grid, grid_factory
from gfw_pixetl.models.pydantic import LayerModel, Symbology
from gfw_pixetl.resampling import resampling_factory
from gfw_pixetl.sources import RasterSource, VectorSource

from .models.enums import DstFormat, PhotometricType
from .settings.globals import GLOBALS
from .utils.aws import get_aws_files, get_s3_client
from .utils.geometry import generate_feature_collection
from .utils.google import get_gs_files
from .utils.utils import DummyTile, intersection

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

        self.resampling: Resampling = resampling_factory(layer_def.resampling)
        self.calc: Optional[str] = layer_def.calc
        self.rasterize_method: Optional[str] = layer_def.rasterize_method
        self.order: Optional[str] = layer_def.order
        self.symbology: Optional[Symbology] = layer_def.symbology
        self.compute_stats: bool = layer_def.compute_stats
        self.compute_histogram: bool = layer_def.compute_histogram
        self.process_locally: bool = layer_def.process_locally
        self.band_count: int = layer_def.band_count
        self.photometric: Optional[PhotometricType] = layer_def.photometric

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

        srs_authority = grid.crs.to_authority()[0].lower()
        srs_code = grid.crs.to_authority()[1]

        return os.path.join(
            name,
            version,
            "raster",
            f"{srs_authority}-{srs_code}",
            f"{grid.name}",
            field,
        )

    @staticmethod
    def _get_dst_profile(layer_def: LayerModel, grid: Grid) -> Dict[str, Any]:
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
            "nodata": data_type.no_data,
        }

        if data_type.nbits:
            dst_profile.update({"nbits": int(data_type.nbits)})

        return dst_profile


class VectorSrcLayer(Layer):
    def __init__(self, layer_def: LayerModel, grid: Grid) -> None:
        super().__init__(layer_def, grid)
        self.src: VectorSource = VectorSource(name=self.name, version=self.version)
        if not self.calc:
            self.calc = self.field


def get_input_files_from_tiles_geojson(bucket, prefix):
    s3_client = get_s3_client()

    response = s3_client.get_object(Bucket=bucket, Key=prefix)
    body = response["Body"].read()

    features = json.loads(body.decode("utf-8"))["features"]

    input_files = list()

    for feature in features:
        LOGGER.debug(f"Found feature: {feature}")
        input_files.append((shape(feature["geometry"]), feature["properties"]["name"]))
    return input_files


def get_input_files_from_folder(provider, bucket, prefix):
    prefix = prefix.rstrip("/") + "/"  # FIXME: Should we instead leave up to user?

    get_files = {"s3": get_aws_files, "gs": get_gs_files}

    file_list = get_files[provider](bucket, prefix)
    tiles: List[DummyTile] = list()
    for uri in file_list:
        LOGGER.debug(f"Adding file {uri}")
        src = RasterSource(uri)
        tiles.append(DummyTile({"geotiff": src}))

    fc: FeatureCollection = generate_feature_collection(
        tiles, DstFormat(GLOBALS.default_dst_format)
    )

    input_files = list()

    for feature in fc["features"]:
        LOGGER.debug(f"Found feature: {feature}")
        input_files.append((shape(feature["geometry"]), feature["properties"]["name"]))
    return input_files


class RasterSrcLayer(Layer):
    def __init__(self, layer_def: LayerModel, grid: Grid) -> None:
        super().__init__(layer_def, grid)

        self._src_uri = layer_def.source_uri
        self.input_bands = self._input_bands()

    def _input_bands(self) -> List[List[Tuple[Polygon, str]]]:
        input_bands = list()
        assert isinstance(self._src_uri, list)

        for src_uri in self._src_uri:
            o = urlparse(src_uri, allow_fragments=False)
            bucket: Union[str, bytes] = o.netloc
            prefix: str = str(o.path).lstrip("/")

            LOGGER.debug(
                f"Get input files for layer {self.name} using {str(bucket)} {prefix}"
            )

            if prefix.endswith(".geojson"):
                LOGGER.debug("Prefix ends with .geojson, assumed to be a geojson file")
                input_files = get_input_files_from_tiles_geojson(bucket, prefix)
            else:
                LOGGER.debug("Prefix does NOT end with .geojson, assumed to be folder")
                input_files = get_input_files_from_folder(o.scheme, bucket, prefix)

            input_bands.append(input_files)

        LOGGER.info(
            f"Using {len(input_bands)} input band(s). Divisor set to {GLOBALS.divisor}."
        )

        return input_bands

    @property
    def geom(self) -> MultiPolygon:
        """Create Multipolygon from union of all input tiles in all bands."""

        LOGGER.debug("Create Polygon from input tile bounds")

        geom: Optional[MultiPolygon] = None
        for band in self.input_bands:
            band_geom: MultiPolygon = unary_union([tile[0] for tile in band])
            geom = intersection(band_geom, geom)

        if not geom:
            raise RuntimeError("Input bands do not overlap")

        return geom


def layer_factory(layer_def: LayerModel) -> Layer:

    layer_constructor = {"vector": VectorSrcLayer, "raster": RasterSrcLayer}

    source_type: str = layer_def.source_type
    grid: Grid = grid_factory(layer_def.grid)

    try:
        layer = layer_constructor[source_type](layer_def, grid)
    except KeyError:
        raise NotImplementedError(
            f"Cannot create layer. Source type {source_type} not implemented."
        )

    return layer
