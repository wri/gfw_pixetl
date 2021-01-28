import os
from datetime import datetime

import rasterio
from dateutil.tz import tzutc
from pyproj import CRS

from gfw_pixetl.errors import GDALNoneTypeError
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.utils.cwd import set_cwd
from gfw_pixetl.utils.gdal import create_vrt, run_gdal_subcommand
from gfw_pixetl.utils.path import get_aws_s3_endpoint
from gfw_pixetl.utils.utils import (
    available_memory_per_process_bytes,
    available_memory_per_process_mb,
    get_bucket,
    world_bounds,
)
from tests.conftest import BUCKET, TILE_1_NAME, TILE_2_NAME

os.environ["ENV"] = "test"
URIS = [f"/vsis3/{BUCKET}/{TILE_1_NAME}", f"/vsis3/{BUCKET}/{TILE_2_NAME}"]


class Client(object):
    def __init__(self, service):
        pass

    @staticmethod
    def assume_role(RoleArn, RoleSessionName):
        return {
            "Credentials": {
                "Expiration": datetime.now(tz=tzutc()),
                "AccessKeyId": "test",
                "SecretAccessKey": "test",
                "SessionToken": "test",
            }
        }


def test_get_bucket():
    os.environ["ENV"] = "production"
    bucket: str = get_bucket()
    assert bucket == "gfw-data-lake"

    os.environ["ENV"] = "staging"
    bucket = get_bucket()
    assert bucket == "gfw-data-lake-staging"

    os.environ["ENV"] = "dev"
    bucket = get_bucket()
    assert bucket == "gfw-data-lake-dev"

    os.environ["ENV"] = "test"
    bucket = get_bucket()
    assert bucket == "gfw-data-lake-test"


def test_set_cwd():
    cwd = os.getcwd()
    new_dir = set_cwd()
    assert cwd != os.getcwd()
    assert os.path.join(cwd, new_dir) == os.getcwd()
    os.chdir(cwd)
    os.rmdir(new_dir)


def test_set_workers():
    cores = GLOBALS.cores
    GLOBALS.workers = cores
    assert GLOBALS.workers == cores

    GLOBALS.workers = cores + 1
    assert GLOBALS.workers == cores

    GLOBALS.workers = cores - 1
    if cores == 1:
        assert GLOBALS.workers == 1
    else:
        assert GLOBALS.workers == cores - 1


def test_available_memory_per_process():
    GLOBALS.workers = 1
    assert available_memory_per_process_bytes() == GLOBALS.max_mem * 1000000
    assert available_memory_per_process_mb() == GLOBALS.max_mem

    GLOBALS.workers = 2
    assert available_memory_per_process_bytes() == GLOBALS.max_mem * 1000000 / 2
    assert available_memory_per_process_mb() == GLOBALS.max_mem / 2


def test__create_vrt():
    vrt = create_vrt(URIS)
    assert vrt == "all.vrt"
    with rasterio.open(vrt, "r") as src:
        assert src.bounds == (-10, 0, 20, 10)

    vrt = create_vrt(URIS, vrt="new_name.vrt", extent=(-20, -10, 30, 20))
    assert vrt == "new_name.vrt"
    with rasterio.open(vrt, "r") as src:
        assert src.bounds == (-20, -10, 30, 20)


def test_world_bounds():
    crs = CRS(4326)
    left, bottom, right, top = world_bounds(crs)
    assert left == -180
    assert bottom == -90
    assert right == 180
    assert top == 90

    crs = CRS(3857)
    left, bottom, right, top = world_bounds(crs)
    assert left == -20037508.342789244
    assert bottom == -20048966.1040146
    assert right == 20037508.342789244
    assert top == 20048966.104014594


def test_get_aws_s3_endpoint():
    """get_endpoint_url should optionally return server name without
    protocol."""

    assert get_aws_s3_endpoint(None) is None
    assert get_aws_s3_endpoint("http://motoserver:5000") == "motoserver:5000"
    assert get_aws_s3_endpoint("motoserver:5000") == "motoserver:5000"


def test__run_gdal_subcommand():
    cmd = ["/bin/bash", "-c", "echo test"]
    assert run_gdal_subcommand(cmd) == ("test\n", "")

    try:
        cmd = ["/bin/bash", "-c", "exit 1"]
        run_gdal_subcommand(cmd)
    except GDALNoneTypeError as e:
        assert str(e) == ""
