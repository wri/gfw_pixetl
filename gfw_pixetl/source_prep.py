import os

import boto3
import click

from gfw_pixetl.sources import RasterSource
from gfw_pixetl.utils import upload_geometries


class DummyTile(object):
    def __init__(self, dst):
        self.dst = {"geotiff": dst}


def get_extent(bucket, prefix):
    print(bucket)
    print(prefix)
    s3 = boto3.client("s3")
    files = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    tiles = list()

    for f in files["Contents"]:
        key = f["Key"]
        if os.path.splitext(key)[1] == ".tif":
            uri = f"/vsis3/{bucket}/{key}"

            # first we need the full URI to fetch metadata and calculate extent
            src = RasterSource(uri)
            # We will then append the src as dst to our dummy file. Here we don't want protocol and bucket in the URI
            src.uri = key

            tiles.append(DummyTile(src))

    upload_geometries.upload_tile_geoms(tiles, bucket=bucket, prefix=prefix + "tiles.geojson")  # type: ignore
    upload_geometries.upload_geom(tiles, bucket=bucket, prefix=prefix + "extent.geojson")  # type: ignore


@click.command()
@click.argument("bucket", type=str)
@click.argument("prefix", type=str)
def cli(bucket, prefix):
    get_extent(bucket, prefix)
