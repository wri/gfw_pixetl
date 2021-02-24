import os
from typing import List, Set
from unittest import mock

import pytest

from gfw_pixetl import layers
from gfw_pixetl.grids import LatLngGrid
from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.pipes import Pipe, RasterPipe
from gfw_pixetl.sources import Destination
from gfw_pixetl.tiles.tile import Tile
from gfw_pixetl.utils.aws import get_s3_client
from tests.conftest import BUCKET, LAYER_DICT, TILE_1_PATH, SUBSET_1x1
from tests.utils import delete_s3_files


def test_pipe(PIPE):
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


def test_filter_subset_tiles(PIPE):
    pipe = _get_subset_tiles(PIPE) | PIPE.filter_subset_tiles(PIPE.subset)
    i = 0
    for tile in pipe.results():
        if tile.status == "pending":
            i += 1
            assert isinstance(tile, Tile)
    assert i == len(SUBSET_1x1)


def test_filter_target_tiles(PIPE, _upload_pipe_fixtures):
    tiles = _get_subset_tiles(PIPE)
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

    pipe = tiles | PIPE.filter_target_tiles(overwrite=False)
    i = 0
    for tile in pipe.results():
        if tile.status == "existing":
            i += 1
            assert isinstance(tile, Tile)
    assert i == 4


def test_upload_file(PIPE):
    tiles = _get_subset_tiles(PIPE)
    # FIXME: I can't get this mock to work, despite trying to patch in many places
    with mock.patch.object(Tile, "upload", return_value=None):  # as mock_upload:
        pipe = tiles | PIPE.upload_file()
        i = 0
        for tile in pipe.results():
            if tile.status == "pending":
                i += 1

    assert i == 4
    # assert mock_upload.call_count == 4


def test_delete_work_dir(PIPE):
    tiles = _get_subset_tiles(PIPE)

    for tile in tiles:
        assert os.path.isdir(tile.work_dir)

    pipe = tiles | PIPE.delete_work_dir()

    for tile in pipe.results():
        assert not os.path.isdir(tile.work_dir)


def _get_subset_tile_ids(PIPE) -> List[str]:
    tile_ids = list()
    for i in range(10, 12):
        for j in range(10, 12):
            assert isinstance(PIPE.grid, LatLngGrid)
            tile_id = PIPE.grid.xy_to_tile_id(j, i)
            tile_ids.append(tile_id)
    return tile_ids


def _get_subset_tiles(PIPE) -> Set[Tile]:
    tiles: Set[Tile] = set()
    for tile_id in _get_subset_tile_ids(PIPE):
        tiles.add(Tile(tile_id=tile_id, grid=PIPE.grid, layer=PIPE.layer))

    return tiles


@pytest.fixture
def _upload_pipe_fixtures(PIPE):
    s3_client = get_s3_client()
    prefix = "aqueduct_erosion_risk/v201911/raster/epsg-4326/1/4000/level"  # pragma: allowlist secret
    delete_s3_files(BUCKET, prefix)
    for tile in _get_subset_tiles(PIPE):
        for dst_format in tile.dst.keys():
            s3_client.upload_file(
                TILE_1_PATH,
                BUCKET,
                f"{prefix}/{dst_format}/{tile.tile_id}.tif",
            )
