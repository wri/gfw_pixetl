from typing import List
from unittest import mock

from gfw_pixetl.grids import LatLngGrid, grid_factory
from gfw_pixetl.layers import VectorSrcLayer, layer_factory
from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.pipes import VectorPipe
from gfw_pixetl.tiles import VectorSrcTile
from tests.utils import get_subset_tile_ids

base_vector_layer_dict = {
    "dataset": "public",
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


def test_create_tiles_with_data(sample_vector_data):
    # Mock collect_tiles to avoid generating the zillion tiles in this grid
    with mock.patch.object(VectorPipe, "collect_tiles", return_value=SUBSET_1x1_TILES):
        pipe: VectorPipe = VectorPipe(layer, SUBSET_1x1_IDS)

        (tiles, skipped_tiles, failed_tiles, existing_tiles) = pipe.create_tiles(
            overwrite=False
        )
    assert len(tiles) == 1
    assert len(skipped_tiles) == 24  # 25 in subset, minus 1 with data
    assert len(failed_tiles) == 0
    assert len(existing_tiles) == 0
