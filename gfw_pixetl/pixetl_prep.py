import os
from typing import Dict, List
from urllib.parse import urlparse

import click
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage
from retrying import retry

from gfw_pixetl.errors import MissingGCSKeyError, retry_if_missing_gcs_key_error
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.utils import get_bucket, upload_geometries
from gfw_pixetl.utils.aws import get_s3_client


class DummyTile(object):
    """A dummy tile."""

    def __init__(self, dst: str) -> None:
        self.dst: Dict = {"geotiff": dst}
        self.metadata: Dict = {}


def get_aws_files(bucket: str, prefix: str) -> List[str]:
    """Get all geotiffs in S3."""
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    objs = response.get("Contents", [])
    files = [
        f"/vsis3/{bucket}/{obj['Key']}"
        for obj in objs
        if os.path.splitext(obj["Key"])[1] == ".tif"
    ]

    return files


@retry(
    retry_on_exception=retry_if_missing_gcs_key_error,
    stop_max_attempt_number=2,
)
def get_gs_files(bucket: str, prefix: str) -> List[str]:
    """Get all geotiffs in GCS."""

    try:
        storage_client = storage.Client()
    except DefaultCredentialsError:
        raise MissingGCSKeyError()

    blobs = storage_client.list_blobs(bucket, prefix=prefix)
    files = [
        f"/vsigs/{bucket}/{blob.name}"
        for blob in blobs
        if os.path.splitext(blob.name)[1] == ".tif"
    ]
    return files


def get_key_from_vsi(vsi_path: str) -> str:
    key = vsi_path.split("/")[3:]
    return "/".join(key)


def create_geojsons(
    bucket: str,
    key: str,
    provider: str,
    dataset: str,
    version: str,
    prefix: str,
    merge_existing: bool,
) -> None:

    get_files = {"s3": get_aws_files, "gs": get_gs_files}
    files = get_files[provider](bucket, key)
    tiles = list()

    for uri in files:
        src = RasterSource(uri)
        tiles.append(DummyTile(src))  # type: ignore

    data_lake_bucket = get_bucket()
    upload_geometries.upload_geojsons(
        tiles,  # type: ignore
        bucket=data_lake_bucket,
        prefix=f"{dataset}/{version}/{prefix.strip('/')}/",
        ignore_existing_tiles=not merge_existing,
    )


@click.command()
@click.argument("resource", type=str)
@click.option(
    "--dataset", type=str, required=True, help="Dataset name of target tileset."
)
@click.option(
    "--version", type=str, required=True, help="Version name of target tileset."
)
@click.option(
    "--prefix",
    type=str,
    default="raw",
    help="Path prefix for output location. Will always be in data lake bucket at {dataset}/{version}/{prefix}",
)
@click.option(
    "--merge_existing",
    type=bool,
    is_flag=True,
    default=False,
    help="Merge new features with features already present in existing geojson files.",
)
def cli(
    resource: str, dataset: str, version: str, prefix: str, merge_existing: bool
) -> None:
    """Retrieve all geotiffs under given resources and generate tiles.geojson
    and extent.geojson at s3//{data-lake}/{dataset}/{version}/{prefix}/geotiff.

    RESOURCE: path to cloud resource. Must use `s3://` or `gs://` protocol.
    """

    o = urlparse(resource, allow_fragments=False)
    if o.scheme and o.scheme in ["s3", "gs"]:
        provider = o.scheme
    else:
        raise ValueError(
            f"Resource {resource} not supported. Must use `s3://` or `gs://` protocol."
        )

    bucket = o.netloc
    key = o.path.lstrip("/")

    create_geojsons(bucket, key, provider, dataset, version, prefix, merge_existing)
