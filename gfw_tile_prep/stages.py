from gfw_tile_prep.utils import get_top, get_left, get_tile_id
import os
import logging
import subprocess as sp


def rasterize(tiles, layer, **kwargs):

    tile_size = kwargs["tile_size"]
    pg_conn = kwargs["pg_conn"]
    pixel_size = kwargs["pixel_size"]
    nodata = kwargs["nodata"]
    data_type = kwargs["nodata"]

    for tile in tiles:

        row, col, min_x, min_y, max_x, max_y = tile
        tile_id = "{}_{}".format(get_top(int(max_y)), get_left(int(min_x)))
        output = "{}_{}.tif".format(layer, tile_id)

        logging.info("Create raster " + output)
        cmd = [
            "gdal_rasterize",
            "-a",
            "oid",
            "-sql",
            "select * from {}_10_10 where row={} and col={}".format(layer, row, col),
            "-te",
            min_x,
            min_y,
            max_x,
            max_y,
            "-tr",
            str(pixel_size),
            str(pixel_size),
            "-a_srs",
            "EPSG:4326",
            "-ot",
            data_type,
            "-a_nodata",
            str(nodata),
            "-co",
            "COMPRESS=LZW",
            "-co",
            "TILED=YES",
            "-co",
            "BLOCKXSIZE={}".format(tile_size),
            "-co",
            "BLOCKYSIZE={}".format(tile_size),
            # "-co", "SPARSE_OK=TRUE",
            pg_conn,
            output,
        ]
        try:
            logging.info("Rasterize tile " + tile_id)
            sp.check_call(cmd)
        except sp.CalledProcessError as e:
            logging.warning("Could not rasterize file " + output)
            logging.warning(e)
        else:
            yield output


def translate(tiles, name, **kwargs):

    src = kwargs["src"]
    tile_size = kwargs["tile_size"]
    data_type = kwargs["data_type"]
    nodata = kwargs["nodata"]

    for tile in tiles:

        row, col, min_x, min_y, max_x, max_y = tile
        tile_id = "{}_{}".format(get_top(int(max_y)), get_left(int(min_x)))
        output = "{}_{}.tif".format(name, tile_id)

        cmd = [
            "gdal_translate",
            "-ot",
            data_type,
            "-a_nodata",
            str(nodata),
            "-co",
            "COMPRESS=LZW",
            "-co",
            "TILED=YES",
            "-co",
            "BLOCKXSIZE={}".format(tile_size),
            "-co",
            "BLOCKYSIZE={}".format(tile_size),
            # "-co", "SPARSE_OK=TRUE",
            src,
            output,
        ]

        try:
            logging.info("Translate tile " + tile_id)
            sp.check_call(cmd)
        except sp.CalledProcessError as e:
            logging.warning("Could not translate file " + output)
            logging.warning(e)
        else:
            yield output


def upload_file(tiles, **kwargs):
    target = kwargs["target"]

    for tile in tiles:
        tile_id = get_tile_id(tile)
        s3_path = target.format(tile_id=tile_id)
        cmd = ["aws", "s3", "cp", tile, s3_path]
        try:
            logging.info("Upload to " + s3_path)
            sp.check_call(cmd)
        except sp.CalledProcessError as e:
            logging.warning("Could not upload file " + tile)
            logging.warning(e)
        else:
            yield tile


def delete_file(tiles, **kwargs):
    for tile in tiles:
        try:
            logging.info("Delete file " + tile)
            os.remove(tile)
        except Exception as e:
            logging.error("Could not delete file " + tile)
            logging.error(e)
            yield tile
