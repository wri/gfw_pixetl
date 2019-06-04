import rasterio
import boto3
import logging
from gfw_tile_prep.utils import get_left, get_top


def fix_bounds(tiles, **kwargs):

    src = kwargs["target"]
    bucket = src.split("/")[1]
    key = "/".join(src.split("/")[-3:])

    s3 = boto3.resource("s3")

    for tile in tiles:

        row, col, min_x, min_y, max_x, max_y = tile
        tile_id = "{}_{}".format(get_top(int(max_y)), get_left(int(min_x)))
        tile_src = src.format(protocol="/vsis3", tile_id=tile_id)

        try:
            with rasterio.open(tile_src) as src:
                bounds = src.bounds
                if (
                    round(bounds.left) != bounds.left
                    or round(bounds.right) != bounds.right
                    or round(bounds.bottom) != bounds.bottom
                    or round(bounds.top) != bounds.top
                ):

                    filename = tile_id + ".tif"
                    logging.info("Donwload " + tile_src)
                    s3.download_file(bucket, key.format(tile_id=tile_id), filename)

                    logging.info("Update transformation for " + tile_src)
                    with rasterio.open(filename, "r+") as src:
                        t = src.transform
                        src.transform = t.translation(round(t[2]), round(t[5]))

                    data = open(filename, "rb")
                    logging.info("Upload " + tile_src)
                    s3.Bucket(bucket).put_object(
                        Key=key.format(tile_id=tile_id), Body=data
                    )

        except rasterio.errors.RasterioIOError:
            logging.info("Tile {} does not exit".format(tile_src))
