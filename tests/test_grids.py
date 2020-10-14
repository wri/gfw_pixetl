import os

import pytest

from gfw_pixetl.grids import Grid, LatLngGrid, WebMercatorGrid, grid_factory

os.environ["ENV"] = "test"


def test_grid_factory():

    grid: Grid = grid_factory("3/33600")
    assert isinstance(grid, Grid)
    assert grid.width == 3
    assert grid.height == 3
    assert grid.cols == 33600
    assert grid.rows == 33600
    assert grid.blockxsize == 480
    assert grid.blockysize == 480
    assert grid.crs.to_string() == "EPSG:4326"

    grid: Grid = grid_factory("10/40000")
    assert isinstance(grid, Grid)
    assert grid.width == 10
    assert grid.height == 10
    assert grid.cols == 40000
    assert grid.rows == 40000
    assert grid.blockxsize == 400
    assert grid.blockysize == 400
    assert grid.crs.to_string() == "EPSG:4326"

    grid: Grid = grid_factory("8/32000")
    assert isinstance(grid, Grid)
    assert grid.width == 8
    assert grid.height == 8
    assert grid.cols == 32000
    assert grid.rows == 32000
    assert grid.blockxsize == 400
    assert grid.blockysize == 400
    assert grid.crs.to_string() == "EPSG:4326"

    grid: Grid = grid_factory("90/27008")
    assert isinstance(grid, Grid)
    assert grid.width == 90
    assert grid.height == 90
    assert grid.cols == 27008
    assert grid.rows == 27008
    assert grid.blockxsize == 128
    assert grid.blockysize == 128
    assert grid.crs.to_string() == "EPSG:4326"

    grid: Grid = grid_factory("90/9984")
    assert isinstance(grid, Grid)
    assert grid.width == 90
    assert grid.height == 90
    assert grid.cols == 9984
    assert grid.rows == 9984
    assert grid.blockxsize == 416
    assert grid.blockysize == 416
    assert grid.crs.to_string() == "EPSG:4326"


def test_get_tile_id():

    # Grid IDs for 10x10 degree grid, 40000x40000 pixels
    grid: Grid = grid_factory("10/40000")

    assert isinstance(grid, LatLngGrid)

    grid_id: str = grid.xy_to_tile_id(0, 0)
    assert grid_id == "00N_000E"

    grid_id = grid.xy_to_tile_id(1, 1)
    assert grid_id == "10N_000E"

    grid_id = grid.xy_to_tile_id(90, 90)
    assert grid_id == "90N_090E"

    grid_id = grid.xy_to_tile_id(-1, -1)
    assert grid_id == "00N_010W"

    grid_id = grid.xy_to_tile_id(-90, -90)
    assert grid_id == "90S_090W"

    # Grid IDs for 8x8 degree grid, 32000x32000 pixels
    # This grid edges do not intersect with equator or central meridian
    grid = grid_factory("8/32000")

    assert isinstance(grid, LatLngGrid)

    grid_id = grid.xy_to_tile_id(0, 0)
    assert grid_id == "04N_004W"

    grid_id = grid.xy_to_tile_id(1, 1)
    assert grid_id == "04N_004W"

    grid_id = grid.xy_to_tile_id(-1, -1)
    assert grid_id == "04N_004W"

    grid_id = grid.xy_to_tile_id(-5, 5)
    assert grid_id == "12N_012W"

    grid_id = grid.xy_to_tile_id(5, -5)
    assert grid_id == "04S_004E"

    grid_id = grid.xy_to_tile_id(-1, -1)
    assert grid_id == "04N_004W"

    with pytest.raises(AssertionError):
        grid.xy_to_tile_id(90, 90)

    with pytest.raises(AssertionError):
        grid.xy_to_tile_id(-90, -90)


def test_wm_grids():
    grid = grid_factory("zoom_1")
    assert isinstance(grid, WebMercatorGrid)
    assert len(grid.get_tile_ids()) == 1 == grid.nb_tiles

    grid = grid_factory("zoom_10")
    assert isinstance(grid, WebMercatorGrid)
    assert len(grid.get_tile_ids()) == 16 == grid.nb_tiles

    grid = grid_factory("zoom_14")
    assert isinstance(grid, WebMercatorGrid)
    assert len(grid.get_tile_ids()) == 4096 == grid.nb_tiles

    with pytest.raises(ValueError):
        grid_factory("zoom_30")
