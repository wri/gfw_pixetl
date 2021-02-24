import math
import os
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from geojson import Feature, FeatureCollection, dumps
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely.ops import unary_union

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.models.enums import DstFormat
from gfw_pixetl.models.types import FeatureTuple
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.aws import get_s3_client

LOGGER = get_module_logger(__name__)
S3 = get_s3_client()


def _uris_per_dst_format(tiles) -> Dict[str, List[str]]:

    uris: Dict[str, List[str]] = dict()

    for tile in tiles:
        for dst_format in tile.dst.keys():
            if dst_format not in uris.keys():
                uris[dst_format] = list()
            uris[dst_format].append(f"{tile.dst[dst_format].url}")

    return uris


def upload_geojsons(
    processed_tiles: List[Tile],
    existing_tiles: List[Tile],
    prefix: str,
    bucket: str = utils.get_bucket(),
    ignore_existing_tiles=False,
) -> List[Dict[str, Any]]:
    """Create geojson listing all tiles and upload to S3."""

    response: List[Dict[str, Any]] = list()

    if ignore_existing_tiles:
        all_tiles = processed_tiles
    else:
        all_tiles = processed_tiles + existing_tiles

    # Upload a tiles.geojson for the default format even if no tiles
    dst_formats: Set[DstFormat] = {DstFormat(GLOBALS.default_dst_format)}
    for tile in all_tiles:
        for fmt in tile.dst.keys():
            dst_formats.add(DstFormat(fmt))

    for dst_format in dst_formats:
        fc: FeatureCollection = generate_feature_collection(all_tiles, dst_format)

        response.append(_upload_extent(fc, prefix=prefix, dst_format=dst_format))

        key = os.path.join(prefix, dst_format, "tiles.geojson")
        response.append(_upload_geojson(fc, bucket, key))
    return response


def generate_feature_collection(
    tiles: List[Tile], dst_format: str
) -> FeatureCollection:
    geoms: List[Tuple[Polygon, Dict[str, Any]]] = _extract_geoms(tiles, dst_format)
    fc: FeatureCollection = _to_feature_collection(geoms)
    return fc


def _extract_geoms(
    tiles: List[Tile], dst_format: str
) -> List[Tuple[Polygon, Dict[str, Any]]]:

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


def _upload_geojson(fc: FeatureCollection, bucket: str, key: str) -> Dict[str, Any]:

    LOGGER.info(f"Upload geometry to {bucket} {key}")
    return S3.put_object(
        Body=str.encode(dumps(fc)),
        Bucket=bucket,
        Key=key,
    )


def _upload_extent(
    fc: FeatureCollection,
    prefix: str,
    dst_format: str,
    bucket: str = utils.get_bucket(),
) -> Dict[str, Any]:
    """Create geojson file for tileset extent and upload to S3."""

    extent_fc = _union_tile_geoms(fc)
    key = os.path.join(prefix, dst_format, "extent.geojson")

    return _upload_geojson(extent_fc, bucket, key)


def _upload_vrt(key: str, vrt: str, prefix: str) -> Dict[str, Any]:

    bucket = utils.get_bucket()
    key = os.path.join(prefix, key, vrt)

    LOGGER.info(f"Upload vrt to {bucket} {key}")
    return S3.upload_file(vrt, bucket, key)
