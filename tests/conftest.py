import os
import shutil
from copy import deepcopy

import numpy as np
import pytest
import rasterio
from affine import Affine
from rasterio.crs import CRS

from gfw_pixetl.layers import layer_factory
from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.pipes import RasterPipe
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.aws import get_s3_client

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
TILE_4_NAME = "01N_001E.tif"
TILE_4_PATH = os.path.join(os.path.dirname(__file__), "fixtures", TILE_4_NAME)


########### World.tif
geotransform = (-180.0, 1.0, 0.0, 90.0, 0.0, -1.0)
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

with rasterio.open(TILE_3_PATH, "w", sharing=False, **profile) as dst:
    dst.write(data, 1)

############ 01N_001E.tif
geotransform = (1.0, 0.00025, 0.0, 1.0, 0.0, -0.00025)
data = np.random.randint(5, size=(4000, 4000)).astype("uint8")
profile = {
    "driver": "GTiff",
    "height": 4000,
    "width": 4000,
    "count": 1,
    "dtype": "uint8",
    "crs": CRS.from_epsg(4326),
    "transform": Affine.from_gdal(*geotransform),
    "nodata": 0,
}

with rasterio.open(TILE_4_PATH, "w", sharing=False, **profile) as dst:
    dst.write(data, 1)


@pytest.fixture(autouse=True, scope="session")
def copy_fixtures():
    # Upload file to mocked S3 bucket
    s3_client = get_s3_client()

    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.upload_file(GEOJSON_PATH, BUCKET, GEOJSON_NAME)
    s3_client.upload_file(GEOJSON_2_PATH, BUCKET, GEOJSON_2_NAME)
    s3_client.upload_file(TILE_1_PATH, BUCKET, TILE_1_NAME)
    s3_client.upload_file(TILE_2_PATH, BUCKET, TILE_2_NAME)
    s3_client.upload_file(TILE_3_PATH, BUCKET, TILE_3_NAME)
    s3_client.upload_file(TILE_1_PATH, BUCKET, f"folder/{TILE_1_NAME}")
    s3_client.upload_file(TILE_2_PATH, BUCKET, f"folder/{TILE_2_NAME}")
    s3_client.upload_file(
        TILE_1_PATH,
        BUCKET,
        f"whrc_aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/geotiff/{TILE_1_NAME}",
    )
    s3_client.upload_file(
        TILE_1_PATH,
        BUCKET,
        f"aqueduct_erosion_risk/v201911/raster/epsg-4326/1/4000/level/geotiff/{TILE_1_NAME}",
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


@pytest.fixture(autouse=True)
def cleanup_tmp():

    yield

    folder = "/tmp"
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if (
                os.path.isfile(file_path) or os.path.islink(file_path)
            ) and "coverage" not in filename:
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print("Failed to delete %s. Reason: %s" % (file_path, e))
    open("/tmp/.gitkeep", "a").close()


#########


minimal_layer_dict = {
    "dataset": "whrc_aboveground_biomass_stock_2000",
    "version": "v4",
    "pixel_meaning": "Mg_ha-1",
    "data_type": "uint16",
    "grid": "10/40000",
    "source_type": "raster",
    # "no_data": 0,
    "source_uri": [f"s3://{BUCKET}/{GEOJSON_NAME}"],
}

LAYER_DICT = {
    **minimal_layer_dict,
    "dataset": "aqueduct_erosion_risk",
    "version": "v201911",
    "pixel_meaning": "level",
    "grid": "1/4000",
    "no_data": 0,
}

SUBSET_1x1 = ["10N_010E", "11N_010E", "11N_011E"]
SUBSET_10x10 = ["10N_010E", "20N_010E", "30N_010E"]


@pytest.fixture()
def LAYER():
    layer_def = LayerModel.parse_obj(LAYER_DICT)
    yield layer_factory(layer_def)


@pytest.fixture()
def LAYER_WM():
    layer_dict_wm = deepcopy(LAYER_DICT)
    layer_dict_wm["grid"] = "zoom_14"

    yield layer_factory(LayerModel(**layer_dict_wm))


@pytest.fixture()
def LAYER_MULTI():
    layer_dict_multi = deepcopy(LAYER_DICT)
    layer_dict_multi["source_uri"] = [
        f"s3://{BUCKET}/{GEOJSON_NAME}",
        f"s3://{BUCKET}/{GEOJSON_NAME}",
    ]
    layer_dict_multi["calc"] = "A + B"

    yield layer_factory(LayerModel(**layer_dict_multi))


@pytest.fixture()
def PIPE(LAYER):
    yield RasterPipe(LAYER, SUBSET_1x1)


@pytest.fixture()
def PIPE_10x10(LAYER):
    yield RasterPipe(LAYER, SUBSET_10x10)


@pytest.fixture()
def TILE(LAYER):
    yield Tile("10N_010E", LAYER.grid, LAYER)
