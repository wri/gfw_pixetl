import os
from typing import Any, Dict, Set
from unittest import mock

from shapely.geometry import box, Polygon

from gfw_pixetl import layers
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.pipes import Pipe
from gfw_pixetl.tiles import Tile


GRID = grid_factory("1/4000")
RASTER_LAYER: Dict[str, Any] = {
    "name": "erosion_risk",
    "version": "v201911",
    "field": "level",
    "grid": GRID,
}

LAYER_TYPE = layers._get_source_type(
    RASTER_LAYER["name"], RASTER_LAYER["field"], RASTER_LAYER["grid"].name
)

LAYER = layers.layer_factory(**RASTER_LAYER)
SUBSET = ["10N_010E", "11N_010E", "12N_010E"]
PIPE = Pipe(LAYER, SUBSET)
TILES = PIPE.get_grid_tiles()


def test_pipe():
    assert isinstance(PIPE, Pipe)


def test_create_tiles():
    try:
        PIPE.create_tiles()
    except NotImplementedError as e:
        assert isinstance(e, NotImplementedError)


def test_get_grid_tiles():
    assert len(TILES) == 64800

    grid = grid_factory("10/40000")
    raster_layer: Dict[str, Any] = {
        "name": "erosion_risk",
        "version": "v201911",
        "field": "level",
        "grid": grid,
    }

    layer = layers.layer_factory(**raster_layer)
    pipe = Pipe(layer)
    tiles = pipe.get_grid_tiles()
    assert len(tiles) == 648


def test_filter_subset_tiles():
    tiles = PIPE.filter_subset_tiles(TILES)

    i = 0
    for tile in tiles:
        i += 1
        assert isinstance(tile, Tile)
    assert i == len(SUBSET)


def test_filter_target_tiles():
    with mock.patch.object(Tile, "dst_exists", return_value=True):
        tiles = _get_subset_tiles()
        tiles = PIPE.filter_target_tiles(tiles, overwrite=False)
        i = 0
        for tile in tiles:
            i += 1
            assert isinstance(tile, Tile)
        assert i == 0

    with mock.patch.object(Tile, "dst_exists", return_value=False):
        tiles = _get_subset_tiles()
        tiles = PIPE.filter_target_tiles(tiles, overwrite=False)
        i = 0
        for tile in tiles:
            i += 1
            assert isinstance(tile, Tile)
        assert i == 4

    with mock.patch.object(Tile, "dst_exists", return_value=True):
        tiles = _get_subset_tiles()
        tiles = PIPE.filter_target_tiles(tiles, overwrite=True)
        i = 0
        for tile in tiles:
            i += 1
            assert isinstance(tile, Tile)
        assert i == 4

    with mock.patch.object(Tile, "dst_exists", return_value=False):
        tiles = _get_subset_tiles()
        tiles = PIPE.filter_target_tiles(tiles, overwrite=True)
        i = 0
        for tile in tiles:
            i += 1
            assert isinstance(tile, Tile)
        assert i == 4


def test_delete_if_empty():
    with mock.patch.object(Tile, "local_src_is_empty", return_value=False):
        tiles = _get_subset_tiles()
        tiles = PIPE.delete_if_empty(tiles)
        i = 0
        for tile in tiles:
            i += 1
            assert isinstance(tile, Tile)
        assert i == 4

    with mock.patch.object(Tile, "local_src_is_empty", return_value=True):
        with mock.patch.object(Tile, "rm_local_src", return_value=None):
            tiles = _get_subset_tiles()
            tiles = PIPE.delete_if_empty(tiles)
            i = 0
            for tile in tiles:
                i += 1
                assert isinstance(tile, Tile)
            assert i == 0


def test_upload_file():
    with mock.patch.object(Tile, "upload", return_value=None):
        tiles = _get_subset_tiles()
        tiles = PIPE.upload_file(tiles)
        i = 0
        for tile in tiles:
            i += 1
            assert isinstance(tile, Tile)
        assert i == 4


def test_delete_file():
    with mock.patch.object(Tile, "rm_local_src", return_value=None):
        tiles = _get_subset_tiles()
        tiles = PIPE.delete_file(tiles)
        i = 0
        for tile in tiles:
            i += 1
            assert isinstance(tile, Tile)
        assert i == 4


def test_create_vrt():
    uris = ["test/uri1", "test/uri2", "test/uri3"]

    with mock.patch("subprocess.Popen", autospec=True) as MockPopen:
        MockPopen.return_value.communicate.return_value = ("", "")
        MockPopen.return_value.returncode = 0
        vrt = PIPE.create_vrt(uris)
        assert vrt == "all.vrt"


def test_create_extent():
    tiles = _get_subset_tiles()
    extent = PIPE.create_extent(tiles)
    assert isinstance(extent, Polygon)
    assert extent.bounds == (10, 9, 12, 11)


def test__write_tile_list():
    uris = ["test/uri1", "test/uri2", "test/uri3"]
    tile_list = "test_tile_list.txt"
    PIPE._write_tile_list(tile_list, uris)
    with open(tile_list, "r") as src:
        lines = src.readlines()
    assert lines == ["/vsis3/test/uri1\n", "/vsis3/test/uri2\n", "/vsis3/test/uri3\n"]
    os.remove(tile_list)


def test__bounds_to_polygon():
    bounds = (10, 9, 12, 11)
    result = PIPE._bounds_to_polygon(bounds)
    assert isinstance(result, Polygon)
    assert result.bounds == bounds


def _get_subset_tiles() -> Set[Tile]:
    tiles = set()
    for i in range(10, 12):
        for j in range(10, 12):
            origin = PIPE.grid.xy_grid_origin(j, i)
            tiles.add(Tile(origin=origin, grid=PIPE.grid, layer=PIPE.layer))
    return tiles
