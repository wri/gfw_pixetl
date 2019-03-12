from parallelpipe import stage
from gfw_tile_prep.utils import get_top, get_left, get_tile_id
import os
import logging
import subprocess as sp


WORKERS = 1


@stage(workers=WORKERS, qsize=WORKERS)
def import_vector(files, name, **kwargs):
    for src in files:
        cmd = [
            "ogr2ogr",
            "-overwrite",
            "-t_srs",
            "EPSG:4326",
            "-f",
            "PostgreSQL",
            "PG:dbname=gfw port=5432 host=localhost user=postgres password=postgres",
            src,
            "-nln",
            name,
        ]
        sp.check_call(cmd)
        yield name


@stage(workers=WORKERS, qsize=WORKERS)
def rasterize(tiles, name, **kwargs):

    tile_size = kwargs["tile_size"]
    for tile in tiles:

        row, col, min_x, min_y, max_x, max_y = tile
        tile_id = "{}_{}".format(get_top(int(max_y)), get_left(int(min_x)))
        output = "{}_{}.tif".format(name, tile_id)

        logging.info("Create raster " + output)
        cmd = [
            "gdal_rasterize",
            "-a",
            "id",
            "-sql",
            "select * from {}_xy where row={} and col={}".format(name, row, col),
            "-te",
            min_x,
            min_y,
            max_x,
            max_y,
            "-tr",
            "0.00025",
            "0.00025",
            "-a_srs",
            "EPSG:4326",
            "-ot",
            "UInt16",
            "-a_nodata",
            "0",
            "-co",
            "COMPRESS=LZW",
            "-co",
            "TILED=YES",
            "-co",
            "BLOCKXSIZE={}".format(tile_size),
            "-co",
            "BLOCKYSIZE={}".format(tile_size),
            # "-co", "SPARSE_OK=TRUE",
            "PG:dbname=gfw port=5432 host=localhost user=postgres password=postgres",
            output,
        ]
        sp.check_call(cmd)
        yield output


@stage(workers=WORKERS, qsize=WORKERS)
def translate(tiles, name, data_type="UInt16", nodata=0, **kwargs):

    tile_size = kwargs["tile_size"]

    for tile in tiles:

        tile_id = get_tile_id(tile)

        output = "{}_{}.tif".format(name, tile_id)
        print("Create raster " + output)
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
            tile,
            output,
        ]

        sp.check_call(cmd)
        yield output


@stage(workers=WORKERS)
def upload_file(tiles, **kwargs):
    for tile in tiles:
        s3_path = "s3://gfw2-data/analyses/gadm/tiles/adm2/{}".format(tile)
        print("Upload to " + s3_path)
        cmd = ["aws", "s3", "cp", tile, s3_path]
        sp.check_call(cmd)
        yield tile


@stage(workers=WORKERS)
def delete_file(tiles, **kwargs):
    for tile in tiles:
        print("Delete file " + tile)
        os.remove(tile)
        yield tile
