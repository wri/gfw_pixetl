import json
import math
import multiprocessing
import os
import posixpath
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Union

from botocore.exceptions import ClientError
from geojson import Feature, FeatureCollection, dumps
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely.ops import unary_union

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.models.enums import DstFormat
from gfw_pixetl.models.types import FeatureTuple
from gfw_pixetl.pixetl_prep import DummyTile, get_aws_files
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils import get_bucket
from gfw_pixetl.utils.aws import get_s3_client
from gfw_pixetl.utils.gdal import create_vrt, get_metadata

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


def upload_geojsons(
    tiles: List[Tile],
    prefix: str,
    bucket: str = utils.get_bucket(),
    ignore_existing_tiles=False,
) -> List[Dict[str, Any]]:
    """Create geojson listing all tiles and upload to S3."""

    response: List[Dict[str, Any]] = list()

    geoms: Dict[str, List[Tuple[Polygon, Dict[str, Any]]]] = _geoms_uris_per_dst_format(
        tiles
    )
    for dst_format in geoms.keys():
        LOGGER.info(f"Building FeatureCollection for dst_format {dst_format}")
        fc: FeatureCollection = _to_feature_collection(geoms[dst_format])

        key = os.path.join(prefix, dst_format, "tiles.geojson")

        LOGGER.info(f"Creating tiles.geojson at {key}")

        if not ignore_existing_tiles:
            fc = _merge_feature_collections(fc, bucket, key)

        response.append(_upload_geojson(fc, bucket, key))
        response.append(_upload_extent(fc, prefix=prefix, dst_format=dst_format))
    return response


def _merge_feature_collections(
    fc: FeatureCollection, bucket: str, key: str
) -> FeatureCollection:
    """Add existing tiles from S3 to Feature Collection.

    This will allow us to skip computing stats and histogram for already
    processed tiles. We assume here that tiles.json represents all
    existing files in give data lake folder and that stats and histogram
    were already computed.
    """

    names = [feature["properties"]["name"] for feature in fc["features"]]
    client = get_s3_client()
    new_fc = deepcopy(fc)
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise
    else:
        old_fc = json.loads(obj["Body"].read())
        for feature in old_fc["features"]:
            if feature["properties"]["name"] not in names:
                new_fc["features"].append(feature)
    return new_fc


def _uris_per_dst_format(tiles) -> Dict[str, List[str]]:

    uris: Dict[str, List[str]] = dict()

    for tile in tiles:
        for dst_format in tile.dst.keys():
            if dst_format not in uris.keys():
                uris[dst_format] = list()
            uris[dst_format].append(f"{tile.dst[dst_format].url}")

    return uris


def _geoms_uris_per_dst_format(
    tiles: List[Tile],
) -> Dict[str, List[Tuple[Polygon, Dict[str, Any]]]]:

    LOGGER.debug("Collect Polygon from tile bounds")

    geoms: Dict[str, List[Tuple[Polygon, Dict[str, Any]]]] = {
        GLOBALS.default_dst_format: list()
    }

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
    for band in props.get("bands", dict()):
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


def prep_dst_prefix(prefix, compute_stats, compute_histogram):
    # Why not just see if there's a tiles.geojson already in the
    # destination prefix? Because there may have already been a resume
    # which added MORE files, but didn't finish (and thus didn't write a
    # new tiles.geojson). Therefore create one from scratch.
    bucket = get_bucket()
    for dst_format in (DstFormat.geotiff, DstFormat.gdal_geotiff):

        LOGGER.info(f"dst_format = {dst_format}")
        final_prefix = posixpath.join(prefix, dst_format)
        LOGGER.info(f"final_prefix = {final_prefix}")

        uris = get_aws_files(bucket, final_prefix, (".tif",))
        # LOGGER.info(f"dst_format specific URIs: {uris}")

        with multiprocessing.Pool(processes=GLOBALS.cores) as pool:
            dummy_tiles: List[DummyTile] = pool.starmap(
                _create_dummy_tile,
                [(dst_format, uri, compute_stats, compute_histogram) for uri in uris],
            )

        # Now that we have stats files for each TIFF, create a tiles.geojson and
        # extent.geojson for the existing files.
        upload_geojsons(
            dummy_tiles,  # type: ignore
            bucket=bucket,
            prefix=f"{prefix}/",
            ignore_existing_tiles=True,
        )


def _create_dummy_tile(dst_format, uri, compute_stats, compute_histogram) -> DummyTile:
    src = RasterSource(uri)
    tile = DummyTile({dst_format: src})
    if compute_stats or compute_histogram:
        tile.metadata = get_metadata(uri, compute_stats, compute_histogram)
    return tile
