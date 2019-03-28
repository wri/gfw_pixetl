from gfw_tile_prep.stages import rasterize, translate, upload_file, delete_file, info
from gfw_tile_prep.postgis import prep_layers, import_vector
from gfw_tile_prep.utils import str2bool
from parallelpipe import Stage
import csv
import logging
import argparse
import os

# parallel workers
WORKERS = 5

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
        "src": "{protocol}/gfw2-data/forest_change/hansen_2018/{tile_id}.tif",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/loss/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
    },
    "gain": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/forest_change/tree_cover_gain/gaindata_2012/Hansen_GFC2015_gain_{tile_id}.tif",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/gain/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
    },
    "tcd_2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/forest_cover/2000_treecover/Hansen_GFC2014_treecover2000_{tile_id}.tif",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/tcd_2000/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
    },
    "tcd_2010": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/forest_cover/2010_treecover_27m/treecover2010_{tile_id}.tif",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/tcd_2010/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
    },
    "co2_pixel": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/WHRC_biomass/WHRC_V4/t_co2_pixel/{tile_id}_t_co2_pixel_2000.tif",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/co2_pixel/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "biomass": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/{tile_id}_t_aboveground_biomass_ha_2000.tif",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/biomass/{tile_id}.tif",
        "data_type": "Int16",
        "nodata": 0,
    },
    "mangrove_biomass": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/mangrove_biomass/processed/20190220/{tile_id}_mangrove_agb_t_ha_2000.tif",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/mangrove_biomass/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "drivers": {
        "type": "raster",
        "src": "{protocol}/wri-users/tmaschler/prep_tiles/raw/drivers/drivers.vrt",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/drivers/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 16,
        "single_tile": True,
    },
    "global_landcover": {
        "type": "raster",
        "src": "{protocol}/wri-users/tmaschler/prep_tiles/raw/global_landcover/global_landcover.vrt",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/global_landcover/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
        "single_tile": True,
    },
    "primary_forest": {
        "type": "raster",
        "src": "{protocol}/wri-users/tmaschler/prep_tiles/raw/primary_forest/primary_forest.vrt",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/primary_forest/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
        "single_tile": True,
    },
    "erosion": {
        "type": "raster",
        "src": "{protocol}/wri-users/tmaschler/prep_tiles/raw/erosion/erosion.vrt",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/erosion/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
        "single_tile": True,
    },
    "ifl": {
        "type": "raster",
        "src": "",
        "s3_target": "",
        "data_type": "Byte",
        "nodata": 0,
    },
    "gadm36": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/analyses/gadm/tiles/adm2/gadm_adm2_{tile_id}.tif",
        "s3_target": "{protocol}/wri-users/tmaschler/prep_tiles/gadm36/{tile_id}.tif",
        "data_type": "UInt16",
        "nodata": 0,
    },
    "wdpa": {
        "type": "vector",
        "src": "",
        "s3_target": "",
        "oid": None,
        "data_type": "Byte",
        "nodata": 0,
    },
    "plantations": {
        "type": "vector",
        "src": "",
        "s3_target": "",
        "oid": None,
        "data_type": "Byte",
        "nodata": 0,
    },
    "logging": {
        "type": "vector",
        "src": "",
        "s3_target": "",
        "oid": None,
        "data_type": "Byte",
        "nodata": 0,
    },
    "mining": {
        "type": "vector",
        "src": "",
        "s3_target": "",
        "oid": None,
        "data_type": "Byte",
        "nodata": 0,
    },
}


def get_tiles(overwrite=False, **kwargs):
    tiles = list()

    if "single_tile" in kwargs.keys():
        single_tile = kwargs["single_tile"]
    else:
        single_tile = False
    dir = os.path.dirname(__file__)
    with open(os.path.join(dir, "csv/tiles.csv")) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        for row in csv_reader:
            tiles.append(row)

    pipe = (
        tiles
        | Stage(info, kwargs["src"], single_tile=single_tile).setup(workers=WORKERS)
        | Stage(
            info, kwargs["s3_target"], include_existing=overwrite, exclude_missing=False
        ).setup(workers=WORKERS)
    )

    tiles_to_process = list()
    for output in pipe.results():
        tiles_to_process.append(output)

    return tiles_to_process


def raster_pipe(layer, overwrite):

    kwargs = SRC[layer]
    kwargs["tile_size"] = TILE_SIZE
    kwargs["pg_conn"] = PG_CONN
    kwargs["pixel_size"] = PIXEL_SIZE

    tiles = get_tiles(overwrite, **kwargs)
    pipe = (
        tiles
        | Stage(translate, name=layer, **kwargs).setup(workers=WORKERS, qsize=WORKERS)
        | Stage(upload_file, **kwargs).setup(workers=WORKERS)
        | Stage(delete_file, **kwargs).setup(workers=WORKERS)
    )

    for output in pipe.results():
        logging.info(output)


def vector_pipe(layer, overwrite):

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

    tiles = get_tiles(overwrite, **kwargs)

    pipe = (
        tiles
        | Stage(rasterize, name=layer, **kwargs).setup(workers=WORKERS, qsize=WORKERS)
        | Stage(upload_file, **kwargs).setup(workers=WORKERS)
        | Stage(delete_file, **kwargs).setup(workers=WORKERS)
    )

    for output in pipe.results():
        logging.info(output)


if __name__ == "__main__":

    layers = list(SRC.keys())

    parser = argparse.ArgumentParser(description="Prepare GFW tiles for SPARK pipeline")

    parser.add_argument("--layer", "-l", type=str, choices=layers)

    parser.add_argument(
        "--overwrite",
        type=str2bool,
        nargs="?",
        default=False,
        const=True,
        help="Overwrite existing output files",
    )

    args = parser.parse_args()

    if SRC[args.layer]["type"] == "raster":  # type: ignore
        raster_pipe(args.layer, args.overwrite)
    else:
        vector_pipe(args.layer, args.overwrite)
