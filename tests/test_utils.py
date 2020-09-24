import multiprocessing
import os
from datetime import datetime
from unittest import mock

from dateutil.tz import tzutc
from pyproj import CRS

from gfw_pixetl.utils.utils import (  # set_aws_credentials,
    _write_tile_list,
    available_memory_per_process,
    create_vrt,
    get_aws_s3_endpoint,
    get_bucket,
    get_workers,
    set_available_memory,
    set_cwd,
    set_workers,
    world_bounds,
)

os.environ["ENV"] = "test"
URIS = [
    f"/vsis3/{get_bucket()}/test/uri1",
    f"/vsis3/{get_bucket()}/test/uri2",
    f"/vsis3/{get_bucket()}/test/uri3",
]


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


#
# def test_set_aws_credentials():
#     env = os.environ.copy()
#     result = set_aws_credentials()
#
#     assert env == result
#     # Only checking for session token, since key and secret might be available on github
#     # assert "AWS_ACCESS_KEY_ID" not in result.keys()
#     # assert "AWS_SECRET_ACCESS_KEY" not in result.keys()
#     assert "AWS_SESSION_TOKEN" not in result.keys()
#
#     os.environ["AWS_BATCH_JOB_ID"] = "test"
#     os.environ["JOB_ROLE_ARN"] = "test"
#     env = os.environ.copy()
#
#     with mock.patch("boto3.client", return_value=Client):
#         result = set_aws_credentials()
#
#     assert env != result
#     # Only checking for session token, since key and secret might be available on github
#     # assert "AWS_ACCESS_KEY_ID" in result.keys()
#     # assert "AWS_SECRET_ACCESS_KEY" in result.keys()
#     assert "AWS_SESSION_TOKEN" in result.keys()
#
#     del os.environ["AWS_BATCH_JOB_ID"]
#     del os.environ["JOB_ROLE_ARN"]


def test_set_cwd():
    cwd = os.getcwd()
    new_dir = set_cwd()
    assert cwd != os.getcwd()
    assert os.path.join(cwd, new_dir) == os.getcwd()
    os.chdir(cwd)
    os.rmdir(new_dir)


def test_set_available_memory():
    mem = set_available_memory()
    assert isinstance(mem, int)
    assert mem == set_available_memory()


def test_set_workers():
    cores = multiprocessing.cpu_count()
    set_workers(cores)
    assert get_workers() == cores

    set_workers(cores + 1)
    assert get_workers() == cores

    set_workers(cores - 1)
    if cores == 1:
        assert get_workers() == 1
    else:
        assert get_workers() == cores - 1


def test_available_memory_per_process():
    mem = set_available_memory()
    set_workers(1)
    assert available_memory_per_process() == mem

    set_workers(2)
    assert available_memory_per_process() == mem / 2


def test__write_tile_list():

    tile_list = "test_tile_list.txt"
    _write_tile_list(tile_list, URIS)
    with open(tile_list, "r") as src:
        lines = src.readlines()
    assert lines == [
        f"/vsis3/{get_bucket()}/test/uri1\n",
        f"/vsis3/{get_bucket()}/test/uri2\n",
        f"/vsis3/{get_bucket()}/test/uri3\n",
    ]
    os.remove(tile_list)


def test__create_vrt():

    with mock.patch("subprocess.Popen", autospec=True) as MockPopen:
        MockPopen.return_value.communicate.return_value = ("", "")
        MockPopen.return_value.returncode = 0
        vrt = create_vrt(URIS)
        assert vrt == "all.vrt"


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
