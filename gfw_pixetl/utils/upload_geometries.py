import json
import os
from typing import Any, Dict, List, Tuple, Union

from botocore.exceptions import ClientError
from geojson import Feature, FeatureCollection, dumps
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.models import FeatureTuple
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.aws import get_s3_client
from gfw_pixetl.utils.gdal import create_vrt

LOGGER = get_module_logger(__name__)
S3 = get_s3_client()


def upload_vrt(tiles: List[Tile], prefix) -> List[Dict[str, Any]]:
    """Create VRT file for input file and upload to S3."""
    response = list()
    uris: Dict[str, List[str]] = _uris_per_dst_format(tiles)

    for key in uris.keys():
        vrt = create_vrt(uris[key])
        response.append(_upload_vrt(key, vrt, prefix))

    return response


def upload_geom(
    tiles: List[Tile], prefix: str, bucket: str = utils.get_bucket()
) -> List[Dict[str, Any]]:
    """Create geojson file for tile extent and upload to S3."""

    response: List[Dict[str, Any]] = list()

    extent: Dict[str, Union[Polygon, MultiPolygon]] = _union_tile_geoms(tiles)
    for dst_format in extent.keys():
        fc: FeatureCollection = _to_feature_collection([(extent[dst_format], None)])

        if os.path.basename(prefix) == "extent.geojson":
            key = prefix
        else:
            key = os.path.join(prefix, dst_format, "extent.geojson")

        response.append(_upload_geom(fc, bucket, key))
    return response


def upload_tile_geoms(
    tiles: List[Tile], prefix: str, bucket: str = utils.get_bucket()
) -> List[Dict[str, Any]]:
    """Create geojson listing all tiles and upload to S3."""

    response: List[Dict[str, Any]] = list()

    geoms: Dict[str, List[Tuple[Polygon, Dict[str, Any]]]] = _geoms_uris_per_dst_format(
        tiles
    )
    for dst_format in geoms.keys():
        fc: FeatureCollection = _to_feature_collection(geoms[dst_format])

        if os.path.basename(prefix) == "tiles.geojson":
            key = prefix
        else:
            key = os.path.join(prefix, dst_format, "tiles.geojson")

        fc = _merge_feature_collections(fc, bucket, key)

        response.append(_upload_geom(fc, bucket, key))
    return response


def _merge_feature_collections(
    fc: FeatureCollection, bucket: str, key: str
) -> FeatureCollection:
    """Add existing tiles to from S3 to Feature Collection.

    This will allow us to skip computing stats and histogram for already
    processed tiles. We assume here that tiles.json represents all
    existing files in give data lake folder and that stats and histogram
    were already computed.
    """

    names = [feature["properties"]["name"] for feature in fc["features"]]
    client = get_s3_client()

    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise
    else:
        old_fc = json.loads(obj["Body"].read())
        for feature in old_fc["features"]:
            if feature["properties"]["name"] not in names:
                fc["features"].append(feature)
    return fc


def _uris_per_dst_format(tiles) -> Dict[str, List[str]]:

    uris: Dict[str, List[str]] = dict()

    for tile in tiles:
        for dst_format in tile.dst.keys():
            if dst_format not in uris.keys():
                uris[dst_format] = list()
            uris[dst_format].append(f"{tile.dst[dst_format].url}")

    return uris


def _geoms_per_dst_format(tiles) -> Dict[str, List[Polygon]]:

    geoms: Dict[str, List[Polygon]] = dict()
    for tile in tiles:
        for dst_format in tile.dst.keys():
            if dst_format not in geoms.keys():
                geoms[dst_format] = list()
            geoms[dst_format].append(tile.dst[dst_format].geom)
    return geoms


def _geoms_uris_per_dst_format(
    tiles: List[Tile],
) -> Dict[str, List[Tuple[Polygon, Dict[str, Any]]]]:

    LOGGER.debug("Collect Polygon from tile bounds")

    geoms: Dict[str, List[Tuple[Polygon, Dict[str, Any]]]] = dict()

    for tile in tiles:
        for dst_format in tile.dst.keys():
            if dst_format not in geoms.keys():
                geoms[dst_format] = list()
            properties = tile.metadata.get(dst_format, dict())
            properties["name"] = tile.dst[dst_format].url
            geoms[dst_format].append(
                (
                    tile.dst[dst_format].geom,
                    properties,
                )
            )

    return geoms


def _union_tile_geoms(tiles: List[Tile]) -> Dict[str, Union[Polygon, MultiPolygon]]:
    """Union tiles bounds into a single geometry."""

    LOGGER.debug("Create Polygon from tile bounds")

    geoms: Dict[str, Union[Polygon, MultiPolygon]] = dict()
    polygons: Dict[str, List[Polygon]] = _geoms_per_dst_format(tiles)

    for dst_format in polygons.keys():
        geoms[dst_format] = unary_union(polygons[dst_format])

    return geoms


def _to_feature_collection(geoms: FeatureTuple) -> FeatureCollection:
    """Convert list of features to feature collection."""

    features: List[Feature] = [
        Feature(geometry=item[0], properties=item[1]) for item in geoms
    ]
    return FeatureCollection(features)


def _upload_geom(fc: FeatureCollection, bucket: str, key: str) -> Dict[str, Any]:

    LOGGER.info(f"Upload geometry to {bucket} {key}")
    return S3.put_object(
        Body=str.encode(dumps(fc)),
        Bucket=bucket,
        Key=key,
    )


def _upload_vrt(key: str, vrt: str, prefix: str) -> Dict[str, Any]:

    bucket = utils.get_bucket()
    key = os.path.join(prefix, key, vrt)

    LOGGER.info(f"Upload vrt to {bucket} {key}")
    return S3.upload_file(vrt, bucket, key)
