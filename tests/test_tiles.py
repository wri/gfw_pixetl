import os
from typing import Any, Dict

from shapely.geometry import Point

from gfw_pixetl import layers, utils
from gfw_pixetl.errors import GDALError, GDALNoneTypeError
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.tiles import Tile


os.environ["ENV"] = "test"

GRID = grid_factory("10/40000")
RASTER_LAYER: Dict[str, Any] = {
    "name": "aboveground_biomass_stock_2000",
    "version": "v201911",
    "field": "Mg_ha-1",
    "grid": GRID,
}

LAYER_TYPE = layers._get_source_type(
    RASTER_LAYER["name"], RASTER_LAYER["field"], RASTER_LAYER["grid"].name
)

LAYER = layers.layer_factory(**RASTER_LAYER)

TILE = Tile(Point(10, 10), GRID, LAYER)


def test_tile():
    assert isinstance(TILE, Tile)


def test_dst_exists():
    assert TILE.dst_exists()


def test_set_local_src():
    try:
        TILE.set_local_src("test")
        # TILE.local_src == f"{TILE.layer.prefix}/{TILE.tile_id}__test.tif"
    except FileNotFoundError as e:
        assert (
            str(e)
            == "File does not exist: aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/10N_010E__test.tif"
        )


def test_local_src_is_empty():
    pass  # TODO: need to mock rasterio.open()


def test_get_stage_uri():
    assert (
        TILE.get_stage_uri("test")
        == "aboveground_biomass_stock_2000/v201911/raster/epsg-4326/10/40000/Mg_ha-1/10N_010E__test.tif"
    )


def test_upload():
    pass  # TODO: need to mock s3 upload


def test_rm_local_src():
    pass  # TODO: need to mock deletion of local file


def test__run_gdal_subcommand():
    cmd = ["/bin/bash", "-c", "echo test"]
    assert TILE._run_gdal_subcommand(cmd) == ("test\n", "")

    try:
        cmd = ["/bin/bash", "-c", "exit 1"]
        TILE._run_gdal_subcommand(cmd)
    except GDALNoneTypeError as e:
        assert str(e) == ""


def test__dst_has_no_data():
    print(LAYER.dst_profile)
    assert TILE._dst_has_no_data()
