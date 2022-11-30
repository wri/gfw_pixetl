import json
import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from geojson import FeatureCollection
from rasterio.warp import Resampling
from shapely.geometry import MultiPolygon, shape
from shapely.ops import unary_union

from gfw_pixetl import get_module_logger
from gfw_pixetl.data_type import DataType, data_type_factory
from gfw_pixetl.grids import Grid, grid_factory
from gfw_pixetl.models.pydantic import LayerModel, Symbology
from gfw_pixetl.resampling import resampling_factory
from gfw_pixetl.sources import RasterSource, VectorSource, fetch_metadata

from .models.enums import DstFormat, PhotometricType
from .models.named_tuples import InputBandElement
from .settings.globals import GLOBALS
from .utils.aws import get_aws_files, get_s3_client
from .utils.geometry import generate_feature_collection
from .utils.google import get_gs_files
from .utils.utils import DummyTile, enumerate_bands, intersection, union

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
        self.union_bands: bool = layer_def.union_bands
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


def get_input_files_from_tiles_geojson(
    bucket: str, prefix: str
) -> List[Tuple[Any, str]]:
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=bucket, Key=prefix)
    body = response["Body"].read()

    features = json.loads(body.decode("utf-8"))["features"]

    input_files = list()

    for feature in features:
        LOGGER.debug(f"Found feature: {feature}")
        input_files.append((shape(feature["geometry"]), feature["properties"]["name"]))
    return input_files


def get_input_files_from_folder(
    provider: str, bucket: str, prefix: str
) -> List[Tuple[Any, str]]:
    # Allow pseudo-globbing: If the prefix doesn't end in *, assume the user
    # meant for the prefix to specify a "folder" and add a "/" to enforce
    # that behavior.
    new_prefix: str = prefix
    if new_prefix.endswith("*"):
        new_prefix = new_prefix[:-1]
    elif not new_prefix.endswith("/"):
        new_prefix += "/"

    get_files = {"s3": get_aws_files, "gs": get_gs_files}

    file_list = get_files[provider](bucket, new_prefix)
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
        self.input_bands: List[List[InputBandElement]] = self._input_bands()

    def _input_bands(self) -> List[List[InputBandElement]]:
        assert isinstance(self._src_uri, list)

        input_bands: List[List[InputBandElement]] = list()

        for src_uri in self._src_uri:
            o = urlparse(src_uri, allow_fragments=False)
            bucket: str = str(o.netloc)
            prefix: str = str(o.path).lstrip("/")

            LOGGER.debug(
                f"Get input files for layer {self.name} using {str(bucket)} {prefix}"
            )

            if prefix.endswith(".geojson"):
                LOGGER.debug("Prefix ends with .geojson, assumed to be a geojson file")
                src_files = get_input_files_from_tiles_geojson(bucket, prefix)
            else:
                LOGGER.debug("Prefix does NOT end with .geojson, assumed to be folder")
                src_files = get_input_files_from_folder(str(o.scheme), bucket, prefix)

            # Make sure band count of all files at a src_uri is consistent
            src_band_count: Optional[int] = None
            src_band_elements: List[List[InputBandElement]] = list()

            for geometry, file_uri in src_files:
                _, file_profile = fetch_metadata(file_uri)
                file_band_count: int = file_profile["count"]

                LOGGER.info(
                    f"Found {file_band_count} data band(s) in file {file_uri} of source {src_uri}"
                )

                if file_band_count == 0:
                    raise Exception(
                        f"Input file {file_uri} from src_uri {src_uri} has 0 data bands!"
                    )
                elif src_band_count is None:
                    LOGGER.info(
                        f"Setting band count for src_uri {src_uri} to {file_band_count}"
                    )
                    src_band_count = file_band_count
                    for i in range(file_band_count):
                        src_band_elements.append(list())
                elif file_band_count != src_band_count:
                    raise Exception(
                        f"Inconsistent band count! Previous files of src_uri {src_uri} had band count of {src_band_count}, but {file_uri} has band count of {file_band_count}"
                    )

                for i in range(file_band_count):
                    band_name: str = enumerate_bands(i)[-1]
                    LOGGER.info(
                        f"Adding {file_uri} (band {i+1}) as input band {band_name}"
                    )
                    element = InputBandElement(
                        geometry=geometry, uri=file_uri, band=i + 1
                    )
                    src_band_elements[i].append(element)

            for band in src_band_elements:
                input_bands.append(band)

        LOGGER.info(
            f"Found {len(input_bands)} total input band(s). Divisor set to {GLOBALS.divisor}."
        )

        return input_bands

    @property
    def geom(self) -> MultiPolygon:
        """Create a Multipolygon from the union or intersection of the input
        tiles in all bands."""

        LOGGER.debug("Creating Multipolygon from input tile bounds")

        geom: Optional[MultiPolygon] = None
        for band in self.input_bands:
            band_geom: MultiPolygon = unary_union([tile.geometry for tile in band])

            if self.union_bands:
                geom = union(band_geom, geom)
            else:
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
