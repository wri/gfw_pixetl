from typing import List, Tuple
from urllib.parse import urlparse

import click

from gfw_pixetl.sources import RasterSource
from gfw_pixetl.utils import get_bucket, upload_geometries
from gfw_pixetl.utils.aws import get_aws_files
from gfw_pixetl.utils.google import get_gs_files
from gfw_pixetl.utils.utils import DummyTile


def get_key_from_vsi(vsi_path: str) -> str:
    key = vsi_path.split("/")[3:]
    return "/".join(key)


def create_geojsons(
    resources: List[Tuple[str, str, str]],
    dataset: str,
    version: str,
    prefix: str,
    merge_existing: bool,
) -> None:
    get_files = {"s3": get_aws_files, "gs": get_gs_files}

    tiles: List[DummyTile] = list()

    for provider, bucket, key in resources:
        files = get_files[provider](bucket, key)

        for uri in files:
            src = RasterSource(uri)
            tiles.append(DummyTile({"geotiff": src}))

    data_lake_bucket = get_bucket()
    target_prefix = f"{dataset}/{version}/{prefix.strip('/')}/"

    # Don't bother checking for existing tiles unless we're going to use them
    existing_tiles = list()
    if merge_existing:
        existing_uris = get_aws_files(data_lake_bucket, target_prefix)
        for uri in existing_uris:
            src = RasterSource(uri)
            existing_tiles.append(DummyTile({"geotiff": src}))

    upload_geometries.upload_geojsons(
        tiles,  # type: ignore
        existing_tiles,  # type: ignore
        bucket=data_lake_bucket,
        prefix=target_prefix,
        ignore_existing_tiles=not merge_existing,
    )


@click.command()
@click.argument("urls", type=str)
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
    help="Merge features from resources with features already present in S3 folder.",
)
def cli(
    urls: str, dataset: str, version: str, prefix: str, merge_existing: bool
) -> None:
    """Retrieve all geotiffs under given resources and generate tiles.geojson
    and extent.geojson at s3://{data-
    lake}/{dataset}/{version}/{prefix}/geotiff.

    URLS: Comma-separated paths to cloud resources. Must use `s3://` or `gs://` protocol.
    """

    resources: List[Tuple[str, str, str]] = list()

    for url in urls.split(","):
        o = urlparse(url, allow_fragments=False)
        if not o.scheme or o.scheme not in ["s3", "gs"]:
            raise ValueError(
                f"URL {url} not supported. Must use `s3://` or `gs://` protocol."
            )
        provider = o.scheme
        bucket = o.netloc
        key = o.path.lstrip("/")
        resources.append((provider, bucket, key))

    create_geojsons(resources, dataset, version, prefix, merge_existing)
