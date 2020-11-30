import os
from typing import List

import click
from google.cloud import storage

from gfw_pixetl.sources import RasterSource
from gfw_pixetl.utils import get_bucket, upload_geometries
from gfw_pixetl.utils.aws import get_s3_client


class DummyTile(object):
    def __init__(self, dst):
        self.dst = {"geotiff": dst}


def get_aws_files(bucket: str, prefix: str) -> List[str]:
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    objs = response["Contents"]
    files = [
        f"/vsis3/{bucket}/{obj['Key']}"
        for obj in objs
        if os.path.splitext(obj["Key"])[1] == ".tif"
    ]

    return files


def get_gs_files(bucket: str, prefix: str) -> List[str]:

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket, prefix=prefix)

    files = [
        f"/vsigs/{bucket}/{blob.name}"
        for blob in blobs
        if os.path.splitext(blob.name)[1] == ".tif"
    ]
    return files


def get_key_from_vsi(vsi_path):
    key = vsi_path.split("/")[3:]
    return "/".join(key)


def get_extent(bucket, prefix, provider, dataset, version, ignore_existing_tiles):

    get_files = {"aws": get_aws_files, "gs": get_gs_files}
    files = get_files[provider](bucket, prefix)
    tiles = list()

    for uri in files:
        key = get_key_from_vsi(uri)
        # first we need the full URI to fetch metadata and calculate extent
        src = RasterSource(uri)
        # We will then append the src as dst to our dummy file. Here we don't want protocol and bucket in the URI
        src.uri = key

        tiles.append(DummyTile(src))

    data_lake_bucket = get_bucket()
    upload_geometries.upload_geojsons(
        tiles,  # type: ignore
        bucket=data_lake_bucket,
        prefix=f"{dataset}/{version}/raw/",
        ignore_existing_tiles=ignore_existing_tiles,
    )


@click.command()
@click.argument("bucket", type=str)
@click.argument("prefix", type=str)
@click.option("--provider", type=str, default="aws")
@click.option("--dataset", type=str, required=True)
@click.option("--version", type=str, required=True)
@click.option("--ignore_existing_tiles", type=bool, default=True)
def cli(bucket, prefix, provider, dataset, version, ignore_existing_tiles):

    if not dataset:
        dataset = bucket
    if not version:
        dataset = prefix

    get_extent(bucket, prefix, provider, dataset, version, ignore_existing_tiles)
