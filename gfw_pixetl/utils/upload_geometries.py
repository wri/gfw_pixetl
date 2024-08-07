import os
from typing import Any, Dict, List, Set

from geojson import FeatureCollection, dumps

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.decorators import processify
from gfw_pixetl.models.enums import DstFormat
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.aws import get_s3_client
from gfw_pixetl.utils.geometry import _union_tile_geoms, generate_feature_collection

LOGGER = get_module_logger(__name__)


def _uris_per_dst_format(tiles) -> Dict[str, List[str]]:

    uris: Dict[str, List[str]] = dict()

    for tile in tiles:
        for dst_format in tile.dst.keys():
            if dst_format not in uris.keys():
                uris[dst_format] = list()
            uris[dst_format].append(f"{tile.dst[dst_format].url}")

    return uris


@processify
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


def _upload_geojson(fc: FeatureCollection, bucket: str, key: str) -> Dict[str, Any]:
    LOGGER.info(f"Uploading geometry to {bucket} {key}")
    s3_client = get_s3_client()
    return s3_client.put_object(
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
