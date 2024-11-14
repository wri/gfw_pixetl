from typing import List, Tuple
from urllib.parse import urlparse

import click

from gfw_pixetl import get_module_logger
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.utils import get_bucket, upload_geometries
from gfw_pixetl.utils.aws import get_aws_files
from gfw_pixetl.utils.google import get_gs_files
from gfw_pixetl.utils.utils import DummyTile
from gfw_pixetl.settings.globals import GLOBALS
from concurrent.futures import ProcessPoolExecutor, as_completed

LOGGER = get_module_logger(__name__)


def get_key_from_vsi(vsi_path: str) -> str:
    key = vsi_path.split("/")[3:]
    return "/".join(key)


def parallel_raster_source(uri) -> RasterSource:
    return RasterSource(uri)


def parallel_get_tiles(files) -> List[DummyTile]:
    '''Given a potentially large list of tiles files, return the list of
    DummyTile/RasterSource objects associated with each tile. Creating the
    RasterSource objects can be slow, since it requires fetching meta-data for
    each tile file (on S3).
    '''

    future_tiles = {}
    tiles: List[DummyTile] = list()

    with ProcessPoolExecutor(max_workers=min(16, GLOBALS.num_processes)) as executor:
        for uri in files:
            future_tiles[executor.submit(parallel_raster_source, uri)] = uri
    for future in as_completed(future_tiles):
        src = future.result()
        tiles.append(DummyTile({"geotiff": src}))
    return tiles

def create_geojsons(
    resources: List[Tuple[str, str, str]],
    dataset: str,
    version: str,
    prefix: str,
    merge_existing: bool,
) -> None:
    '''Create the tiles.geojson and extent.geojson associated with the TIF files in
    the paths in resources list. If merge_existing is Ture, also look at the tiles
    already under data-lake/{dataset}/{version}/{prefix}.
    '''
    get_files = {"s3": get_aws_files, "gs": get_gs_files}

    tiles: List[DummyTile] = list()

    for provider, bucket, key in resources:
        LOGGER.info(f"Fetch file names for {bucket}, {key}")
        files = get_files[provider](bucket, key)

        LOGGER.info("Fetching tile meta-data")
        tiles.extend(parallel_get_tiles(files))
        LOGGER.info("Done fetching tile meta-data")

    data_lake_bucket = get_bucket()
    target_prefix = f"{dataset}/{version}/{prefix.strip('/')}/"

    # Don't bother checking for existing tiles unless we're going to use them
    existing_tiles = list()
    if merge_existing:
        existing_uris = get_aws_files(data_lake_bucket, target_prefix)
        existing_tiles = parallel_get_tiles(existing_uris)

    upload_geometries.upload_geojsons(
        tiles,  # type: ignore
        existing_tiles,  # type: ignore
        bucket=data_lake_bucket,
        prefix=target_prefix,
        ignore_existing_tiles=not merge_existing,
    )


# Example command that could be run locally to complete processing:
#
# ENV=production python ./gfw_pixetl/pixetl_prep.py --dataset jrc_global_forest_cover --version v2020 --prefix raster/epsg-3857/zoom_14/is_default2 s3://gfw-data-lake/jrc_global_forest_cover/v2020/raster/epsg-3857/zoom_14/is_default2/geotiff

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


if __name__ == "__main__":
    cli()
