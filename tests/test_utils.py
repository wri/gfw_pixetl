import logging
import os
import string
from datetime import datetime
from unittest.mock import Mock

import pytest
import rasterio
from _pytest.monkeypatch import MonkeyPatch
from dateutil.tz import tzutc
from pyproj import CRS
from shapely.geometry import MultiPolygon, Polygon

import gfw_pixetl.utils.gdal
from gfw_pixetl.errors import GDALNoneTypeError
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.utils.cwd import set_cwd
from gfw_pixetl.utils.gdal import create_vrt, run_gdal_subcommand
from gfw_pixetl.utils.utils import (
    available_memory_per_process_bytes,
    available_memory_per_process_mb,
    enumerate_bands,
    get_bucket,
    intersection,
    world_bounds,
)
from tests.conftest import BUCKET, TILE_1_NAME, TILE_2_NAME
from tests.utils import compare_multipolygons

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
                "SecretAccessKey": "test",  # pragma: allowlist secret
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
    num_processes = GLOBALS.num_processes
    GLOBALS.workers = num_processes
    assert GLOBALS.workers == num_processes

    GLOBALS.workers = num_processes + 1
    assert GLOBALS.workers == num_processes

    GLOBALS.workers = num_processes - 1
    if num_processes == 1:
        assert GLOBALS.workers == 1
    else:
        assert GLOBALS.workers == num_processes - 1


def test_available_memory_per_process():
    GLOBALS.workers = 1
    assert available_memory_per_process_bytes() == GLOBALS.max_mem * 1000000
    assert available_memory_per_process_mb() == GLOBALS.max_mem

    GLOBALS.workers = 2
    assert available_memory_per_process_bytes() == GLOBALS.max_mem * 1000000 / 2
    assert available_memory_per_process_mb() == GLOBALS.max_mem / 2


def test_create_vrt():
    vrt = create_vrt(URIS)
    assert vrt == "all.vrt"
    with rasterio.open(vrt) as src:
        assert src.bounds == (-10, 0, 20, 10)

    vrt = create_vrt(URIS, vrt="new_name.vrt", extent=(-20, -10, 30, 20))
    assert vrt == "new_name.vrt"
    with rasterio.open(vrt) as src:
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
    assert bottom == -20048966.104014594
    assert right == 20037508.342789244
    assert top == 20048966.104014594


def test_run_gdal_subcommand_nonzero_exit():
    cmd = ["/bin/bash", "-c", "echo test"]
    assert run_gdal_subcommand(cmd) == ("test\n", "")

    try:
        cmd = ["/bin/bash", "-c", "exit 1"]
        run_gdal_subcommand(cmd)
        assert False
    except GDALNoneTypeError as e:
        assert str(e) == ""


def test_run_gdal_subcommand_zero_exit_with_stderr():
    cmd = ["/bin/bash", "-c", "echo test"]
    assert run_gdal_subcommand(cmd) == ("test\n", "")

    logger = logging.getLogger()
    logger.warning = Mock()
    MonkeyPatch().setattr(gfw_pixetl.utils.gdal, "LOGGER", logger)

    # write to stderr
    cmd = ["/bin/bash", "-c", ">&2 echo ERROR 1: SSL SYSCALL"]
    o, e = run_gdal_subcommand(cmd)

    assert "ERROR 1: SSL SYSCALL" in str(e)
    assert logger.warning.called is True


def test_intersection():
    polygon1 = Polygon([(0, 0), (0, 2), (2, 2), (2, 0)])
    polygon2 = Polygon([(1, 1), (1, 3), (3, 3), (3, 1)])

    # assert polygon1.intersection(polygon2).bounds == (1.0, 1.0, 2.0, 2.0)

    # Make sure intersection of just one multi is itself
    multi1 = MultiPolygon([polygon1])
    expected_inters = multi1
    inters = intersection([multi1, None])
    compare_multipolygons(inters, expected_inters)

    # Basic test of two overlapping multis
    multi2 = MultiPolygon([polygon2])
    inters1 = intersection([multi1, multi2])
    expected_inters = MultiPolygon([Polygon([(1, 1), (1, 2), (2, 2), (2, 1)])])
    compare_multipolygons(inters1, expected_inters)

    # Make sure polys of a multi are unioned before intersection with other multi is taken
    # (verifies fix for GTC-1236)
    polygon4 = Polygon([(2, 2), (2, 4), (4, 4), (4, 2)])
    multi3 = MultiPolygon([polygon1, polygon4])
    inters2 = intersection([multi2, multi3])
    expected_inters = MultiPolygon(
        [
            Polygon([(1, 1), (1, 2), (2, 2), (2, 1)]),
            Polygon([(2, 2), (2, 3), (3, 3), (3, 2)]),
        ]
    )
    compare_multipolygons(inters2, expected_inters)

    # Sometimes Shapely generates GeometryCollections because of funky intersections
    # (for example when polygons intersect on an edge but also overlap elsewhere)
    # Our intersection function should filter out extraneous bits so the result fits in
    # a MultiPolygon
    multi6 = MultiPolygon(
        [
            Polygon([(0, 0), (0, 2), (1, 2), (1, 0)]).union(
                Polygon([(1, 0), (1, 1), (2, 1), (2, 0)])
            )
        ]
    )
    multi7 = MultiPolygon([Polygon([(1, 0), (1, 2), (2, 2), (2, 0)])])

    # This doesn't test OUR code, just making sure it does what I think it does
    geo_col = multi6.intersection(multi7)
    assert geo_col.type == "GeometryCollection"
    assert any(geo.type == "LineString" for geo in geo_col.geoms)

    # Now test our function
    inters3 = intersection([multi6, multi7])
    expected_inters = MultiPolygon([Polygon([(1, 0), (1, 1), (2, 1), (2, 0)])])
    compare_multipolygons(inters3, expected_inters)


def test_enumerate_bands_simple_case_1():
    assert enumerate_bands(1) == ["A"]


def test_enumerate_bands_verify_1_through_26():
    assert enumerate_bands(26) == [x for x in string.ascii_uppercase]


def test_enumerate_bands_more_than_26():
    assert len(enumerate_bands(27)) == 27
    assert enumerate_bands(27)[-1] == "AA"


def test_enumerate_bands_unique():
    assert len(set([x for x in enumerate_bands(67)])) == 67


def test_enumerate_bands_invalid_num_bands():
    with pytest.raises(ValueError):
        enumerate_bands(None)  # noqa
