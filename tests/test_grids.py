from shapely.geometry import Point

from gfw_pixetl.grids import Grid, grid_factory


def test_grid_factory():

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
    grid: Grid = grid_factory("10/40000")

    point: Point = Point(0, 0)
    grid_id: str = grid.point_grid_id(point)
    assert grid_id == "00N_000E"

    point: Point = Point(1, 1)
    grid_id: str = grid.point_grid_id(point)
    assert grid_id == "10N_000E"

    point: Point = Point(90, 90)
    grid_id: str = grid.point_grid_id(point)
    assert grid_id == "90N_090E"

    point: Point = Point(-1, -1)
    grid_id: str = grid.point_grid_id(point)
    assert grid_id == "00N_010W"

    point: Point = Point(-90, -90)
    grid_id: str = grid.point_grid_id(point)
    assert grid_id == "90S_090W"

    grid: Grid = grid_factory("8/32000")

    point: Point = Point(0, 0)
    grid_id: str = grid.point_grid_id(point)
    assert grid_id == "00N_000E"

    point: Point = Point(1, 1)
    grid_id: str = grid.point_grid_id(point)
    assert grid_id == "10N_000E"

    point: Point = Point(90, 90)
    grid_id: str = grid.point_grid_id(point)
    assert grid_id == "90N_090E"

    point: Point = Point(-1, -1)
    grid_id: str = grid.point_grid_id(point)
    assert grid_id == "00N_010W"

    point: Point = Point(-90, -90)
    grid_id: str = grid.point_grid_id(point)
    assert grid_id == "90S_090W"
