from parallelpipe import stage
import subprocess as sp
import csv
import os
import logging

WORKERS = 10
TILE_SIZE = 400

SRC = {
    "loss": {"type": "raster", "src": "", "target": ""},
    "tcd": {"type": "raster", "src": "", "target": ""},
    "co2_pixel": {"type": "raster", "src": "", "target": ""},
    "primary_forest": {"type": "raster", "src": "", "target": ""},
    "ifl": {"type": "raster", "src": "", "target": ""},
    "gadm2": {"type": "raster", "src": "", "target": ""},
    "wdpa": {"type": "vector", "src": "", "target": ""},
    "plantations": {"type": "vector", "src": "", "target": ""},
    "logging": {"type": "vector", "src": "", "target": ""},
    "mining": {"type": "vector", "src": "", "target": ""},
}


def get_tiles():
    tiles = list()
    with open("tiles.csv") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        for row in csv_reader:
            tiles.append(row)
    return tiles


if __name__ == "__main__":
    tiles = get_tiles()
    # pipe = tiles | rename_s3 # write_raster | upload_file | delete_file

    # for output in pipe.results():
    #    pass
