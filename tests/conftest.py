import os
import shutil

import boto3
import pytest

from gfw_pixetl.settings.globals import AWS_REGION, ENDPOINT_URL

BUCKET = "gfw-data-lake-test"
GEOJSON_NAME = "tiles.geojson"
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "fixtures", GEOJSON_NAME)
TILE_1_NAME = "10N_010E.tif"
TILE_1_PATH = os.path.join(os.path.dirname(__file__), "fixtures", TILE_1_NAME)
TILE_2_NAME = "10N_010W.tif"
TILE_2_PATH = os.path.join(os.path.dirname(__file__), "fixtures", TILE_2_NAME)


@pytest.fixture(autouse=True, scope="session")
def copy_fixtures():
    # Upload file to mocked S3 bucket
    s3_client = boto3.client("s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)

    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.upload_file(GEOJSON_PATH, BUCKET, GEOJSON_NAME)
    s3_client.upload_file(TILE_1_PATH, BUCKET, TILE_1_NAME)
    s3_client.upload_file(TILE_2_PATH, BUCKET, TILE_2_NAME)
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
