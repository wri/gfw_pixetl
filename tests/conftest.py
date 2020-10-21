import os
import shutil

import boto3
import pytest

from gfw_pixetl.settings.globals import AWS_REGION, ENDPOINT_URL

BUCKET = "gfw-data-lake-test"
GEOJSON_NAME = "tiles.geojson"
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "fixtures", GEOJSON_NAME)
GEOJSON_2_NAME = "world.geojson"
GEOJSON_2_PATH = os.path.join(os.path.dirname(__file__), "fixtures", GEOJSON_2_NAME)
TILE_1_NAME = "10N_010E.tif"
TILE_1_PATH = os.path.join(os.path.dirname(__file__), "fixtures", TILE_1_NAME)
TILE_2_NAME = "10N_010W.tif"
TILE_2_PATH = os.path.join(os.path.dirname(__file__), "fixtures", TILE_2_NAME)
TILE_3_NAME = "world.tif"
TILE_3_PATH = os.path.join(os.path.dirname(__file__), "fixtures", TILE_3_NAME)

import numpy as np
import rasterio
from affine import Affine
from rasterio.crs import CRS

########### World.tif
geotransform = (-180, 1, 0.0, 90, 0.0, -1)
data = np.random.randint(20, size=(180, 360)).astype("uint8")
profile = {
    "driver": "GTiff",
    "height": 180,
    "width": 360,
    "count": 1,
    "dtype": "uint8",
    "crs": CRS.from_epsg(4326),
    "transform": Affine.from_gdal(*geotransform),
}

with rasterio.open(TILE_3_PATH, "w", **profile) as dst:
    dst.write(data, 1)


@pytest.fixture(autouse=True, scope="session")
def copy_fixtures():
    # Upload file to mocked S3 bucket
    s3_client = boto3.client("s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)

    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.upload_file(GEOJSON_PATH, BUCKET, GEOJSON_NAME)
    s3_client.upload_file(GEOJSON_2_PATH, BUCKET, GEOJSON_2_NAME)
    s3_client.upload_file(TILE_1_PATH, BUCKET, TILE_1_NAME)
    s3_client.upload_file(TILE_2_PATH, BUCKET, TILE_2_NAME)
    s3_client.upload_file(TILE_3_PATH, BUCKET, TILE_3_NAME)
    s3_client.upload_file(
        TILE_1_PATH,
        BUCKET,
        f"whrc_aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/geotiff/{TILE_1_NAME}",
    )

    yield

    # Delete intermediate files
    try:
        shutil.rmtree(
            os.path.join(
                os.path.dirname(__file__),
                "fixtures",
                "tmp",
                "whrc_aboveground_biomass_stock_2000",
            )
        )
    except FileNotFoundError:
        pass
