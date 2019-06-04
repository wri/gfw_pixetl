from gfw_tile_prep.utils import get_top, get_left
import rasterio
import boto3
import logging


def delete_empty_tiles(tiles, **kwargs):

    src = kwargs["target"]
    bucket = src.split("/")[1]
    old_key = "/".join(src.split("/")[-3:])
    new_key = "/".join(src.split("/")[-3:-2] + ["delete"] + src.split("/")[-2:])

    s3 = boto3.resource("s3")

    for tile in tiles:

        row, col, min_x, min_y, max_x, max_y = tile
        tile_id = "{}_{}".format(get_top(int(max_y)), get_left(int(min_x)))
        tile_src = src.format(protocol="/vsis3", tile_id=tile_id)

        try:
            with rasterio.open(tile_src) as src:
                msk = src.read_masks(1).astype(bool)
            if msk[msk].size == 0:
                logging.info("Delete tile: " + tile_src)
                s3.Object(bucket, new_key.format(tile_id=tile_id)).copy_from(
                    CopySource="{}/{}".format(bucket, old_key.format(tile_id=tile_id))
                )
                s3.Object(bucket, old_key.format(tile_id=tile_id)).delete()
            else:
                logging.info("Keep tile: " + tile_src)
            msk = None
        except rasterio.errors.RasterioIOError:
            logging.info("Tile {} does not exit".format(tile_src))
