import os
from typing import Set
from unittest import mock

from shapely.geometry import Polygon

from gfw_pixetl import layers
from gfw_pixetl.models import LayerModel
from gfw_pixetl.pipes import Pipe, RasterPipe
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils import upload_geometries
from gfw_pixetl.sources import Destination
from tests import minimal_layer_dict


os.environ["ENV"] = "test"


LAYER_DICT = {
    **minimal_layer_dict,
    "dataset": "aqueduct_erosion_risk",
    "version": "v201911",
    "pixel_meaning": "level",
    "grid": "1/4000",
    "no_data": 0,
}
LAYER_DEF = LayerModel.parse_obj(LAYER_DICT)
LAYER = layers.layer_factory(LAYER_DEF)
SUBSET = ["10N_010E", "11N_010E", "11N_011E"]
PIPE = Pipe(LAYER, SUBSET)


def test_pipe():
    assert isinstance(PIPE, Pipe)


def test_create_tiles():
    try:
        PIPE.create_tiles(overwrite=False)
    except NotImplementedError as e:
        assert isinstance(e, NotImplementedError)


def test_get_grid_tiles():
    message = ""
    try:
        len(PIPE.get_grid_tiles(min_x=10, min_y=10, max_x=12, max_y=12))
    except NotImplementedError:
        message = "not implemented"
    assert message == "not implemented"

    layer_dict = {
        **LAYER_DICT,
        "grid": "10/40000",
    }
    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))

    pipe = RasterPipe(layer)
    assert len(pipe.get_grid_tiles(min_x=0, min_y=0, max_x=20, max_y=20)) == 4


def test_filter_subset_tiles():
    pipe = _get_subset_tiles() | PIPE.filter_subset_tiles(PIPE.subset)
    i = 0
    for tile in pipe.results():
        if tile.status == "pending":
            i += 1
            assert isinstance(tile, Tile)
    assert i == len(SUBSET)


def test_filter_target_tiles():
    tiles = _get_subset_tiles()
    with mock.patch.object(Destination, "exists", return_value=True):
        pipe = tiles | PIPE.filter_target_tiles(overwrite=False)
        i = 0
        for tile in pipe.results():
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, Tile)
        assert i == 0

    with mock.patch.object(Destination, "exists", return_value=False):
        pipe = tiles | PIPE.filter_target_tiles(overwrite=False)
        i = 0
        for tile in pipe.results():
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, Tile)
        assert i == 4

    with mock.patch.object(Destination, "exists", return_value=True):
        pipe = tiles | PIPE.filter_target_tiles(overwrite=True)
        i = 0
        for tile in pipe.results():
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, Tile)
        assert i == 4

    with mock.patch.object(Destination, "exists", return_value=False):

        pipe = tiles | PIPE.filter_target_tiles(overwrite=True)
        i = 0
        for tile in pipe.results():
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, Tile)
        assert i == 4


def test_upload_file():
    tiles = _get_subset_tiles()
    with mock.patch.object(Tile, "upload", return_value=None):

        pipe = tiles | PIPE.upload_file()
        i = 0
        for tile in pipe.results():
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, Tile)
        assert i == 4


def test_delete_file():
    tiles = _get_subset_tiles()
    with mock.patch.object(Tile, "rm_local_src", return_value=None):
        pipe = tiles | PIPE.delete_file()
        i = 0
        for tile in pipe.results():
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, Tile)
        assert i == 4


def test__to_polygon():
    tiles = list(_get_subset_tiles())
    extent = upload_geometries._union_tile_geoms(tiles)
    for dst_format in extent.keys():
        assert isinstance(extent[dst_format], Polygon)
        assert extent[dst_format].bounds == (10, 9, 12, 11)


def _get_subset_tiles() -> Set[Tile]:
    tiles = set()
    for i in range(10, 12):
        for j in range(10, 12):
            origin = PIPE.grid.xy_grid_origin(j, i)
            tiles.add(Tile(origin=origin, grid=PIPE.grid, layer=PIPE.layer))
    return tiles
