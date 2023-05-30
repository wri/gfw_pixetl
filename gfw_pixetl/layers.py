import json
import os
import pathlib
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
from .utils.aws import download_s3, get_aws_files, get_s3_client
from .utils.geometry import generate_feature_collection
from .utils.google import download_gcs, get_gs_files
from .utils.path import create_dir, from_vsi
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
        # LOGGER.debug(f"Found feature: {feature}")
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
        # LOGGER.debug(f"Adding file {uri}")
        src = RasterSource(uri)
        tiles.append(DummyTile({"geotiff": src}))

    fc: FeatureCollection = generate_feature_collection(
        tiles, DstFormat(GLOBALS.default_dst_format)
    )

    input_files = list()

    for feature in fc["features"]:
        # LOGGER.debug(f"Found feature: {feature}")
        input_files.append((shape(feature["geometry"]), feature["properties"]["name"]))
    return input_files


def get_input_files_from_directory(dir_path: str) -> List[Tuple[Any, str]]:
    path_obj = pathlib.Path(dir_path)
    file_list = list(path_obj.rglob("*.tif"))

    tiles: List[DummyTile] = list()

    for path in file_list:
        src = RasterSource(str(path))
        tiles.append(DummyTile({"geotiff": src}))

    fc: FeatureCollection = generate_feature_collection(
        tiles, DstFormat(GLOBALS.default_dst_format)
    )

    input_files = list()

    for feature in fc["features"]:
        input_files.append((shape(feature["geometry"]), feature["properties"]["name"]))
    return input_files


def download_source_file(remote_file: str) -> str:
    """Download remote files."""

    download_constructor = {"gs": download_gcs, "s3": download_s3}

    parts = urlparse(remote_file)

    local_file = os.path.join("/tmp/input", parts.netloc, parts.path[1:])
    create_dir(os.path.dirname(local_file))

    LOGGER.debug(f"Downloading remote file {remote_file} to {local_file}")
    download_constructor[parts.scheme](
        bucket=parts.netloc, key=parts.path[1:], dst=local_file
    )

    return local_file


def download_sources(source_uris: List[str]) -> List[str]:
    assert isinstance(source_uris, list)

    file_uris: List[str] = list()
    local_source_uris: List[str] = list()

    for source_uri in source_uris:
        o = urlparse(source_uri, allow_fragments=False)

        bucket: str = str(o.netloc)
        prefix: str = str(o.path).lstrip("/").rstrip("*")

        LOGGER.debug(f"Getting input files from {str(source_uri)}")

        if prefix.endswith(".geojson"):
            LOGGER.debug("Prefix ends with .geojson, assumed to be a geojson file")
            file_uris += [
                from_vsi(uri)
                for _, uri in get_input_files_from_tiles_geojson(bucket, prefix)
            ]
            local_source_uris.append(
                os.path.join("/tmp/input", bucket, os.path.dirname(prefix))
            )
        else:
            LOGGER.debug("Prefix does NOT end with .geojson, assumed to be folder")
            file_uris += [
                from_vsi(uri)
                for _, uri in get_input_files_from_folder(str(o.scheme), bucket, prefix)
            ]
            local_source_uris.append(os.path.join("/tmp/input", bucket, prefix))

    for file_uri in file_uris:
        download_source_file(file_uri)

    return local_source_uris


class RasterSrcLayer(Layer):
    def __init__(self, layer_def: LayerModel, grid: Grid) -> None:
        super().__init__(layer_def, grid)

        assert isinstance(layer_def.source_uri, List)
        _source_paths: List[str] = download_sources(layer_def.source_uri)
        LOGGER.info(f"SOURCE PATHS AFTER DOWNLOADING: {_source_paths}")
        self.input_bands: List[List[InputBandElement]] = self._input_bands(
            _source_paths
        )

    def _input_bands(self, source_paths: List[str]) -> List[List[InputBandElement]]:
        assert isinstance(source_paths, list)

        input_bands: List[List[InputBandElement]] = list()

        for source_path in source_paths:
            LOGGER.debug(
                f"Getting input files for layer {self.name} from {source_path}"
            )

            src_files = get_input_files_from_directory(source_path)

            # Make sure band count of all files at a src_uri is consistent
            src_band_count: Optional[int] = None
            src_band_elements: List[List[InputBandElement]] = list()

            for geometry, file_path in src_files:
                _, file_profile = fetch_metadata(file_path)
                file_band_count: int = file_profile["count"]

                LOGGER.info(
                    f"Found {file_band_count} data band(s) in file {file_path} of source {source_path}"
                )

                if file_band_count == 0:
                    raise Exception(
                        f"Input file {file_path} from src_uri {source_path} has 0 data bands!"
                    )
                elif src_band_count is None:
                    LOGGER.info(
                        f"Setting band count for source_path {source_path} to {file_band_count}"
                    )
                    src_band_count = file_band_count
                    for i in range(file_band_count):
                        src_band_elements.append(list())
                elif file_band_count != src_band_count:
                    raise Exception(
                        f"Inconsistent band count! Previous files of source_path {source_path} had band count of {src_band_count}, but {file_path} has band count of {file_band_count}"
                    )

                for i in range(file_band_count):
                    band_name: str = enumerate_bands(i + 1)[-1]
                    LOGGER.info(
                        f"Adding {file_path} (band {i+1}) as input band {band_name}"
                    )
                    element = InputBandElement(
                        geometry=geometry, uri=file_path, band=i + 1
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

        if geom is None:
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
