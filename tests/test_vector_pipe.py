from typing import List
from unittest import mock

import pytest

from gfw_pixetl.grids import LatLngGrid, grid_factory
from gfw_pixetl.layers import VectorSrcLayer, layer_factory
from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.pipes import VectorPipe
from gfw_pixetl.tiles import VectorSrcTile
from tests.utils import get_subset_tile_ids

base_vector_layer_dict = {
    "dataset": "some_dataset",
    "version": "v4",
    "grid": "1/4000",
    "pixel_meaning": "gfw_fid",
    "source_type": "vector",
    "no_data": 0,
    "data_type": "uint32",
}

layer: VectorSrcLayer = layer_factory(LayerModel.parse_obj(base_vector_layer_dict))
some_grid: LatLngGrid = grid_factory("1/4000")

SUBSET_1x1_IDS: List[str] = get_subset_tile_ids(some_grid, 10, 55, 5)
SUBSET_1x1_TILES: List[VectorSrcTile] = [
    VectorSrcTile(tile_id, some_grid, layer) for tile_id in SUBSET_1x1_IDS
]


def test_create_tiles_with_data(sample_vector_data, in_process_pipeline):
    # Mock get_grid_tiles (not collect_tiles) so collect_tiles() actually
    # runs its real filtering -- including the DB intersects check -- against
    # a small, manageable subset instead of generating the zillion tiles in
    # this grid. _build_pipe() must not repeat that filtering (see
    # vector_pipe.py), so this exercises the real end-to-end classification.
    with mock.patch.object(
        VectorPipe, "get_grid_tiles", return_value=set(SUBSET_1x1_TILES)
    ):
        pipe: VectorPipe = VectorPipe(layer, SUBSET_1x1_IDS)

        tiles, skipped_tiles, failed_tiles, existing_tiles = pipe.create_tiles(
            overwrite=False
        )
    assert len(tiles) == 1
    assert len(skipped_tiles) == 24  # 25 in subset, minus 1 with data
    assert len(failed_tiles) == 0
    assert len(existing_tiles) == 0


def test_group_raster_work_fans_in_completed_parent():
    from gfw_pixetl.tiles.vector_src_tile import (
        VectorRasterAssemblyItem,
        VectorRasterWorkItem,
    )

    parent = mock.Mock(spec=VectorSrcTile)
    parent.tile_id = "54N_010E"
    parts = [
        VectorRasterWorkItem(
            parent, index, (0, 0, 1, 1), f"src-{index}", f"dst-{index}"
        )
        for index in range(16)
    ]

    result = list(VectorPipe.group_raster_work(iter(parts)))

    assert len(result) == 1
    assert isinstance(result[0], VectorRasterAssemblyItem)
    assert result[0].parent is parent
    assert [part.index for part in result[0].parts] == list(range(16))


def test_group_raster_work_rejects_incomplete_parent():
    from gfw_pixetl.tiles.vector_src_tile import VectorRasterWorkItem

    parent = mock.Mock(spec=VectorSrcTile)
    parent.tile_id = "54N_010E"
    parts = [
        VectorRasterWorkItem(
            parent, index, (0, 0, 1, 1), f"src-{index}", f"dst-{index}"
        )
        for index in range(15)
    ]

    with pytest.raises(RuntimeError, match=r"54N_010E \(15/16\)"):
        list(VectorPipe.group_raster_work(iter(parts)))


def test_raster_part_bounds_are_fixed_4x4_grid():
    tile = mock.Mock(spec=VectorSrcTile)
    tile.bounds.left = -10.0
    tile.bounds.bottom = 50.0
    tile.bounds.right = 0.0
    tile.bounds.top = 60.0

    bounds = VectorSrcTile._raster_part_bounds(tile)

    assert len(bounds) == 16
    assert bounds[0] == (-10.0, 57.5, -7.5, 60.0)
    assert bounds[3] == (-2.5, 57.5, 0.0, 60.0)
    assert bounds[12] == (-10.0, 50.0, -7.5, 52.5)
    assert bounds[15] == (-2.5, 50.0, 0.0, 52.5)
