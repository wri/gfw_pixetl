import os

from shapely.geometry import Point

from gfw_pixetl.grids import Grid, grid_factory

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
    assert grid.srs.to_string() == "EPSG:4326"

    grid: Grid = grid_factory("10/40000")
    assert isinstance(grid, Grid)
    assert grid.width == 10
    assert grid.height == 10
    assert grid.cols == 40000
    assert grid.rows == 40000
    assert grid.blockxsize == 400
    assert grid.blockysize == 400
    assert grid.srs.to_string() == "EPSG:4326"

    grid: Grid = grid_factory("8/32000")
    assert isinstance(grid, Grid)
    assert grid.width == 8
    assert grid.height == 8
    assert grid.cols == 32000
    assert grid.rows == 32000
    assert grid.blockxsize == 400
    assert grid.blockysize == 400
    assert grid.srs.to_string() == "EPSG:4326"

    grid: Grid = grid_factory("90/27008")
    assert isinstance(grid, Grid)
    assert grid.width == 90
    assert grid.height == 90
    assert grid.cols == 27008
    assert grid.rows == 27008
    assert grid.blockxsize == 128
    assert grid.blockysize == 128
    assert grid.srs.to_string() == "EPSG:4326"

    grid: Grid = grid_factory("90/9984")
    assert isinstance(grid, Grid)
    assert grid.width == 90
    assert grid.height == 90
    assert grid.cols == 9984
    assert grid.rows == 9984
    assert grid.blockxsize == 416
    assert grid.blockysize == 416
    assert grid.srs.to_string() == "EPSG:4326"


def test_get_tile_id():

    # Grid IDs for 10x10 degree grid, 40000x40000 pixels
    grid: Grid = grid_factory("10/40000")

    point: Point = Point(0, 0)
    grid_id: str = grid.point_grid_id(point)
    assert grid_id == "00N_000E"

    point = Point(1, 1)
    grid_id = grid.point_grid_id(point)
    assert grid_id == "10N_000E"

    point = Point(90, 90)
    grid_id = grid.point_grid_id(point)
    assert grid_id == "90N_090E"

    point = Point(-1, -1)
    grid_id = grid.point_grid_id(point)
    assert grid_id == "00N_010W"

    point = Point(-90, -90)
    grid_id = grid.point_grid_id(point)
    assert grid_id == "90S_090W"

    # Grid IDs for 8x8 degree grid, 32000x32000 pixels
    # This grid edges do not intersect with equator or central meridian
    grid = grid_factory("8/32000")

    point = Point(0, 0)
    grid_id = grid.point_grid_id(point)
    assert grid_id == "04N_004W"

    point = Point(1, 1)
    grid_id = grid.point_grid_id(point)
    assert grid_id == "04N_004W"

    point = Point(-1, -1)
    grid_id = grid.point_grid_id(point)
    assert grid_id == "04N_004W"

    point = Point(-5, 5)
    grid_id = grid.point_grid_id(point)
    assert grid_id == "12N_012W"

    point = Point(5, -5)
    grid_id = grid.point_grid_id(point)
    assert grid_id == "04S_004E"

    point = Point(-1, -1)
    grid_id = grid.point_grid_id(point)
    assert grid_id == "04N_004W"

    point = Point(90, 90)
    try:
        grid.point_grid_id(point)
    except Exception as e:
        assert isinstance(e, AssertionError)

    point = Point(-90, -90)
    try:
        grid.point_grid_id(point)
    except Exception as e:
        assert isinstance(e, AssertionError)
