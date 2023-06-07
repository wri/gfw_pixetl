import functools
import os
from typing import Any, Dict, List, Optional

from rasterio.warp import Resampling
from shapely.geometry import MultiPolygon
from shapely.ops import unary_union

from gfw_pixetl import get_module_logger
from gfw_pixetl.data_type import DataType, data_type_factory
from gfw_pixetl.grids import Grid, grid_factory
from gfw_pixetl.models.pydantic import LayerModel, Symbology
from gfw_pixetl.resampling import resampling_factory
from gfw_pixetl.sources import VectorSource, fetch_metadata

from .models.enums import PhotometricType
from .models.named_tuples import InputBandElement
from .utils.sources import download_sources, get_shape_path_pairs_under_directory
from .utils.utils import enumerate_bands, intersection, union

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
            src_files = get_shape_path_pairs_under_directory(source_path)

            # Make sure the band count of all files at a src_uri is consistent
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
                        f"Inconsistent band count! Previous files of {source_path} "
                        f"had band count of {src_band_count}, but {file_path} has "
                        f"band count of {file_band_count}"
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

        LOGGER.info(f"Found {len(input_bands)} total input band(s).")

        return input_bands

    @property
    def geom(self) -> MultiPolygon:
        """Create a Multipolygon from the union or intersection of the input
        tiles in all bands."""
        band_geoms: List[Optional[MultiPolygon]] = []
        for band in self.input_bands:
            band_geoms.append(unary_union([tile.geometry for tile in band]))

        if self.union_bands:
            final_geom = functools.reduce(union, band_geoms, None)
        else:
            final_geom = intersection(band_geoms)

        if final_geom is None:
            raise RuntimeError("Input bands do not overlap")

        return final_geom


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
