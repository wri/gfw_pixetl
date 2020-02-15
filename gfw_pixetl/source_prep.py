import os

import boto3
import click
import rasterio

from gfw_pixetl.sources import RasterSource
from gfw_pixetl.pipes import Pipe


class DummyLayer(object):
    grid = None


class DummyTile(object):
    def __init__(self, dst):
        self.dst = dst


layer = DummyLayer()
pipe = Pipe(layer)  # type: ignore


def get_extent(bucket, prefix):

    print(bucket)
    print(prefix)
    s3 = boto3.client("s3")
    files = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    tiles = list()

    for f in files["Contents"]:
        key = f["Key"]
        if os.path.splitext(key)[1] == ".tif":
            uri = f"s3://{bucket}/{key}"
            with rasterio.open(uri, "r") as img:
                src = RasterSource(img.profile, img.bounds, uri)
            tiles.append(DummyTile(src))

    pipe.upload_geom(tiles, bucket=bucket, key=prefix + "/extent.geojson")  # type: ignore


@click.command()
@click.argument("bucket", type=str)
@click.argument("prefix", type=str)
def cli(bucket, prefix):
    get_extent(bucket, prefix)
