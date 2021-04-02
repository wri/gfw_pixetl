import math
from typing import Any, Dict, List, Optional, Tuple, Union

from geojson import Feature, FeatureCollection
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely.ops import unary_union

from gfw_pixetl import get_module_logger
from gfw_pixetl.models.types import FeatureTuple

# from gfw_pixetl.tiles import Tile

LOGGER = get_module_logger(__name__)


def generate_feature_collection(tiles, dst_format: str) -> FeatureCollection:
    geoms: List[Tuple[Polygon, Dict[str, Any]]] = _extract_geoms(tiles, dst_format)
    fc: FeatureCollection = _to_feature_collection(geoms)
    return fc


def _extract_geoms(tiles, dst_format: str) -> List[Tuple[Polygon, Dict[str, Any]]]:

    LOGGER.debug("Collect Polygon from tile bounds")

    geoms: List[Tuple[Polygon, Dict[str, Any]]] = list()

    for tile in tiles:
        if dst_format not in tile.dst:
            continue
        properties = tile.metadata.get(dst_format, dict())
        properties["name"] = tile.dst[dst_format].url
        geoms.append(
            (
                tile.dst[dst_format].geom,
                properties,
            )
        )

    return geoms


def _union_tile_geoms(fc: FeatureCollection) -> FeatureCollection:
    """Union tiles bounds into a single geometry."""

    LOGGER.debug("Create Polygon from tile bounds")

    polygons: List[Polygon] = [shape(feature["geometry"]) for feature in fc["features"]]
    extent: Union[Polygon, MultiPolygon] = unary_union(polygons)
    return _to_feature_collection([(extent, None)])


def _sanitize_props(props: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    # Non-numeric float values (like NaN) are not JSON-legal
    if props is None:
        return None
    for band in props.get("bands", list()):
        no_data = band.get("no_data")
        if no_data is not None and math.isnan(no_data):
            band["no_data"] = "nan"
    return props


def _to_feature_collection(geoms: FeatureTuple) -> FeatureCollection:
    """Convert list of features to feature collection."""

    features: List[Feature] = [
        Feature(geometry=item[0], properties=_sanitize_props(item[1])) for item in geoms
    ]
    return FeatureCollection(features)
