from gfw_tile_prep.stages import rasterize, translate, upload_file, delete_file
from gfw_tile_prep.postgis import prep_layers, import_vector
from parallelpipe import Stage
import csv
import logging
import argparse

# parallel workers
WORKERS = 10

# tile specifications
TILE_SIZE = 400
PIXEL_SIZE = 0.00025

# local PG settings
HOST = "localhost"
PORT = 5432
DBNAME = "gfw"
DBUSER = "postgres"
PASSWORD = "postgres"
PG_CONN = "PG:dbname={} port={} host={} user={} password={}".format(
    DBNAME, PORT, HOST, DBUSER, PASSWORD
)

# data sources
SRC = {
    "loss": {
        "type": "raster",
        "src": "",
        "target": "",
        "data_type": "Byte",
        "nodata": 0,
    },
    "tcd": {
        "type": "raster",
        "src": "",
        "target": "",
        "data_type": "Byte",
        "nodata": 0,
    },
    "co2_pixel": {
        "type": "raster",
        "src": "",
        "target": "",
        "data_type": "Byte",
        "nodata": 0,
    },
    "primary_forest": {
        "type": "raster",
        "src": "",
        "target": "",
        "data_type": "Byte",
        "nodata": 0,
    },
    "ifl": {
        "type": "raster",
        "src": "",
        "target": "",
        "data_type": "Byte",
        "nodata": 0,
    },
    "gadm2": {
        "type": "raster",
        "src": "",
        "target": "",
        "data_type": "Byte",
        "nodata": 0,
    },
    "wdpa": {
        "type": "vector",
        "src": "",
        "target": "",
        "oid": None,
        "data_type": "Byte",
        "nodata": 0,
    },
    "plantations": {
        "type": "vector",
        "src": "",
        "target": "",
        "oid": None,
        "data_type": "Byte",
        "nodata": 0,
    },
    "logging": {
        "type": "vector",
        "src": "",
        "target": "",
        "oid": None,
        "data_type": "Byte",
        "nodata": 0,
    },
    "mining": {
        "type": "vector",
        "src": "",
        "target": "",
        "oid": None,
        "data_type": "Byte",
        "nodata": 0,
    },
}


def get_tiles():
    tiles = list()
    with open("tiles.csv") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        for row in csv_reader:
            tiles.append(row)
    return tiles


def raster_pipe(layer):

    kwargs = SRC["layer"]
    kwargs["tile_size"] = TILE_SIZE
    kwargs["pg_conn"] = PG_CONN
    kwargs["pixel_size"] = PIXEL_SIZE

    tiles = get_tiles()
    pipe = (
        tiles
        | Stage(translate, layer, **kwargs).setup(workers=WORKERS, qsize=WORKERS)
        | Stage(upload_file, **kwargs).setup(workers=WORKERS)
        | Stage(delete_file, **kwargs).setup(workers=WORKERS)
    )

    for output in pipe.results():
        logging.info(output)


def vector_pipe(layer):

    kwargs = SRC["layer"]
    kwargs["tile_size"] = TILE_SIZE
    kwargs["pixel_size"] = PIXEL_SIZE
    kwargs["pg_conn"] = PG_CONN
    kwargs["host"] = HOST
    kwargs["port"] = PORT
    kwargs["dbname"] = DBNAME
    kwargs["dbuser"] = DBUSER
    kwargs["password"] = PASSWORD

    import_vector(layer, **kwargs)
    prep_layers(layer, **kwargs)

    tiles = get_tiles()

    pipe = (
        tiles
        | Stage(rasterize, layer, **kwargs).setup(workers=WORKERS, qsize=WORKERS)
        | Stage(upload_file, **kwargs).setup(workers=WORKERS)
        | Stage(delete_file, **kwargs).setup(workers=WORKERS)
    )

    for output in pipe.results():
        logging.info(output)


if __name__ == "__main__":

    layers = list(SRC.keys())

    parser = argparse.ArgumentParser(description="Prepare GFW tiles for SPARK pipeline")

    parser.add_argument("--layer", "-l", type=str, choices=layers)

    args = parser.parse_args()

    if SRC[args.layer]["type"] == "raster":  # type: ignore
        raster_pipe(args.layer)
    else:
        vector_pipe(args.layer)
