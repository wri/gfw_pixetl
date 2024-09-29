import json
from typing import Any, List, Tuple

from geojson import FeatureCollection
from shapely.geometry import shape

from gfw_pixetl.layers import LOGGER
from gfw_pixetl.models.enums import DstFormat
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.utils.aws import get_aws_files, get_s3_client
from gfw_pixetl.utils.geometry import generate_feature_collection
from gfw_pixetl.utils.google import get_gs_files
from gfw_pixetl.utils.utils import DummyTile


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
