import json
import os
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse

from geojson import FeatureCollection
from shapely.geometry import shape

from gfw_pixetl import get_module_logger
from gfw_pixetl.models.enums import DstFormat
from gfw_pixetl.models.types import ShapePathPair
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.utils.aws import download_s3, get_aws_files, get_s3_client
from gfw_pixetl.utils.geometry import generate_feature_collection
from gfw_pixetl.utils.google import download_gcs, get_gs_files
from gfw_pixetl.utils.path import create_dir, from_vsi
from gfw_pixetl.utils.utils import DummyTile

LOGGER = get_module_logger(__name__)


def get_file_list_from_tiles_geojson(bucket: str, prefix: str) -> List[str]:
    """Fetches a geojson file from AWS and returns the filenames contained
    therein (whatever their format, but note that we generally store GDAL-
    style /vsi URLs)"""
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=bucket, Key=prefix)
    body = response["Body"].read()

    features = json.loads(body.decode("utf-8"))["features"]

    return [feature["properties"]["name"] for feature in features]


def get_file_list_from_cloud_folder(
    provider: str, bucket: str, prefix: str
) -> List[str]:
    # Allow pseudo-globbing: If the prefix doesn't end in *, assume the user
    # meant for the prefix to specify a "folder" and add a "/" to enforce
    # that behavior.
    new_prefix: str = prefix
    if new_prefix.endswith("*"):
        new_prefix = new_prefix[:-1]
    elif not new_prefix.endswith("/"):
        new_prefix += "/"

    get_files_constructor = {"s3": get_aws_files, "gs": get_gs_files}

    return get_files_constructor[provider](bucket, new_prefix)


def get_shape_path_pairs_under_directory(dir_path: str) -> List[ShapePathPair]:
    path_obj = Path(dir_path)
    path_list = list(path_obj.rglob("*.tif"))

    tiles: List[DummyTile] = list()

    for path in path_list:
        src = RasterSource(str(path))
        tiles.append(DummyTile({"geotiff": src}))

    fc: FeatureCollection = generate_feature_collection(
        tiles, DstFormat(GLOBALS.default_dst_format)
    )

    return [
        (shape(feature["geometry"]), feature["properties"]["name"])
        for feature in fc["features"]
    ]


def download_source_file(remote_file: str, basedir: str = "/tmp/input") -> Path:
    """Download remote AWS or GCS files."""

    download_constructor = {"gs": download_gcs, "s3": download_s3}

    parts = urlparse(remote_file)

    local_file = Path(os.path.join(basedir, parts.netloc, parts.path[1:]))
    create_dir(os.path.dirname(local_file))

    LOGGER.debug(f"Downloading remote file {remote_file} to {local_file}")
    download_constructor[parts.scheme](
        bucket=parts.netloc, key=parts.path[1:], dst=str(local_file)
    )

    return local_file


def download_sources(source_uris: List[str]) -> List[str]:
    """Given a list of source URIs (pointing to any combination of
    tiles.geojsons and cloud storage folders), download all indicated files to
    the local filesystem and return a new list of source_uris pointing to those
    local directories."""
    assert isinstance(source_uris, list)

    file_uris: List[Tuple[str, str]] = list()
    local_source_uris: List[str] = list()

    for i, source_uri in enumerate(source_uris):
        o = urlparse(source_uri, allow_fragments=False)

        bucket: str = str(o.netloc)
        prefix: str = str(o.path).lstrip("/").rstrip("*")

        local_source_dir = f"/tmp/input/source{i}"

        if prefix.endswith(".geojson"):
            file_uris += [
                (from_vsi(file_uri), local_source_dir)
                for file_uri in get_file_list_from_tiles_geojson(bucket, prefix)
            ]
        else:
            file_uris += [
                (from_vsi(str(file_uri)), local_source_dir)
                for file_uri in get_file_list_from_cloud_folder(
                    str(o.scheme), bucket, prefix
                )
            ]
        create_dir(local_source_dir)
        local_source_uris.append(local_source_dir)

    LOGGER.info(f"Complete list of file_uris to download: {file_uris}")

    for file_uri, target_dir in file_uris:

        download_source_file(file_uri, basedir=target_dir)

    return local_source_uris
