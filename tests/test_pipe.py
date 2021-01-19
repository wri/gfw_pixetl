import os
from typing import Set
from unittest import mock

from gfw_pixetl import layers
from gfw_pixetl.grids import LatLngGrid
from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.pipes import Pipe, RasterPipe
from gfw_pixetl.sources import Destination
from gfw_pixetl.tiles import Tile
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
PIPE = RasterPipe(LAYER, SUBSET)


def test_pipe():
    assert isinstance(PIPE, Pipe)


def test_get_grid_tiles():
    # message = ""
    # try:
    #     len(PIPE.get_grid_tiles())
    # except NotImplementedError:
    #     message = "not implemented"
    # assert message == "not implemented"

    layer_dict = {
        **LAYER_DICT,
        "grid": "10/40000",
    }
    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))

    pipe = RasterPipe(layer)
    assert len(pipe.get_grid_tiles()) == 648


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


def test_delete_work_dir():
    tiles = _get_subset_tiles()

    for tile in tiles:
        assert os.path.isdir(tile.work_dir)

    pipe = tiles | PIPE.delete_work_dir()

    for tile in pipe.results():
        assert not os.path.isdir(tile.work_dir)


def _get_subset_tiles() -> Set[Tile]:
    tiles = set()
    for i in range(10, 12):
        for j in range(10, 12):
            assert isinstance(PIPE.grid, LatLngGrid)
            tile_id = PIPE.grid.xy_to_tile_id(j, i)
            tiles.add(Tile(tile_id=tile_id, grid=PIPE.grid, layer=PIPE.layer))
    return tiles
