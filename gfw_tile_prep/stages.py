from gfw_tile_prep.utils import get_top, get_left, get_tile_id
from pathlib import Path
import xml.etree.ElementTree as ET
import os
import logging
import subprocess as sp
import psycopg2


def rasterize(tiles, layer, **kwargs):

    tile_size = kwargs["tile_size"]
    pg_conn = kwargs["pg_conn"]
    pixel_size = kwargs["pixel_size"]
    nodata = kwargs["nodata"]
    data_type = kwargs["data_type"]
    oid = kwargs["oid"]
    order = kwargs["order"]
    src = kwargs["src"]

    for tile in tiles:

        row, col, min_x, min_y, max_x, max_y = tile
        tile_id = "{}_{}".format(get_top(int(max_y)), get_left(int(min_x)))
        output = "{}_{}.tif".format(layer, tile_id)

        logging.info("Create raster " + output)
        cmd = [
            "gdal_rasterize",
            "-a",
            oid,
            "-sql",
            "select * from {} where row={} and col={} order by {} {}".format(
                src, row, col, oid, order
            ),
            "-te",
            str(min_x),
            str(min_y),
            str(max_x),
            str(max_y),
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


def info(tiles, path, include_existing=True, exclude_missing=True, **kwargs):

    for tile in tiles:

        row, col, min_x, min_y, max_x, max_y = tile
        tile_id = "{}_{}".format(get_top(int(max_y)), get_left(int(min_x)))

        src = path.format(protocol="/vsis3", tile_id=tile_id)

        if kwargs["single_tile"]:

            found = False
            for x in [min_x, max_x]:
                for y in [min_y, max_y]:
                    cmd = ["gdallocationinfo", "-xml", "-wgs84", src, x, y]
                    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
                    o, e = p.communicate()
                    if p.returncode == 0 and ET.fromstring(o)[0].tag == "BandReport":
                        found = True
            if found:
                logging.info("Tile {} intersects with {}".format(tile_id, src))
                yield tile
            else:
                logging.warning(
                    "Tile {} does not intersect with {}".format(tile_id, src)
                )

        elif kwargs["is_vector"] and include_existing:

            conn = psycopg2.connect(
                dbname=kwargs["dbname"],
                user=kwargs["dbuser"],
                password=kwargs["password"],
                host=kwargs["host"],
                port=kwargs["port"],
            )
            cursor = conn.cursor()
            exists_query = "select exists (select 1 from {} where row = {} and col = {})".format(
                kwargs["src"], row, col
            )
            cursor.execute(exists_query)
            exists = cursor.fetchone()[0]
            cursor.close()
            conn.close()

            print(row, col, exists)
            if exists:
                yield tile

        else:
            cmd = ["gdalinfo", src]

            try:
                logging.info("Check if tile exist " + src)
                sp.check_call(cmd)
            except sp.CalledProcessError as pe:
                logging.warning("Could not find tile file " + src)
                logging.warning(pe)

                if not exclude_missing:
                    yield tile
            else:
                if include_existing:
                    yield tile


def translate(tiles, name, **kwargs):

    src = kwargs["src"]
    tile_size = kwargs["tile_size"]
    data_type = kwargs["data_type"]
    nodata = kwargs["nodata"]
    xres = kwargs["pixel_size"]
    yres = kwargs["pixel_size"]

    for tile in tiles:

        row, col, min_x, min_y, max_x, max_y = tile
        tile_id = "{}_{}".format(get_top(int(max_y)), get_left(int(min_x)))
        output = "{}_{}.tif".format(name, tile_id)

        cmd = [
            "gdal_translate",
            "-strict",
            "-ot",
            data_type,
            "-a_nodata",
            str(nodata),
            # "-outsize",
            # "40000",
            # "40000",
            "-tr",
            str(xres),
            str(yres),
            "-projwin",
            str(min_x),
            str(max_y),
            str(max_x),
            str(min_y),
            "-co",
            "COMPRESS=LZW",
            "-co",
            "TILED=YES",
            "-co",
            "BLOCKXSIZE={}".format(tile_size),
            "-co",
            "BLOCKYSIZE={}".format(tile_size),
            # "-co", "SPARSE_OK=TRUE",
            src.format(protocol="/vsis3", tile_id=tile_id),
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
    target = kwargs["s3_target"]

    for tile in tiles:
        tile_id = get_tile_id(tile)
        s3_path = target.format(protocol="s3:/", tile_id=tile_id)
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
