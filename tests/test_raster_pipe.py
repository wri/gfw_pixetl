import os
from typing import Any, Dict, Set
from unittest import mock

from gfw_pixetl import layers
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.pipes import RasterPipe
from gfw_pixetl.tiles import RasterSrcTile

os.environ["ENV"] = "test"

GRID_10 = grid_factory("10/40000")
GRID_1 = grid_factory("1/4000")

RASTER_LAYER: Dict[str, Any] = {
    "name": "erosion_risk",
    "version": "v201911",
    "field": "level",
    "grid": GRID_10,
}

LAYER_TYPE = layers._get_source_type(
    RASTER_LAYER["name"], RASTER_LAYER["field"], RASTER_LAYER["grid"].name
)

LAYER = layers.layer_factory(**RASTER_LAYER)
SUBSET = ["10N_010E", "20N_010E", "30N_010E"]
PIPE = RasterPipe(LAYER, SUBSET)


def test_create_tiles_subset():
    with mock.patch.object(RasterSrcTile, "src_tile_intersects", return_value=True):
        with mock.patch.object(RasterSrcTile, "dst_exists", return_value=False):
            with mock.patch.object(RasterSrcTile, "transform", return_value=None):
                with mock.patch.object(
                    RasterSrcTile, "local_src_is_empty", return_value=False
                ):
                    with mock.patch.object(
                        RasterSrcTile, "compress", return_value=None
                    ):
                        with mock.patch.object(
                            RasterSrcTile, "upload", return_value=None
                        ):
                            with mock.patch.object(
                                RasterSrcTile, "rm_local_src", return_value=None
                            ):
                                result = PIPE.create_tiles()
                                assert len(result) == 3


def test_create_tiles_all():
    pipe = RasterPipe(LAYER)
    with mock.patch.object(RasterSrcTile, "src_tile_intersects", return_value=True):
        with mock.patch.object(RasterSrcTile, "dst_exists", return_value=False):
            with mock.patch.object(RasterSrcTile, "transform", return_value=None):
                with mock.patch.object(
                    RasterSrcTile, "local_src_is_empty", return_value=False
                ):
                    with mock.patch.object(
                        RasterSrcTile, "compress", return_value=None
                    ):
                        with mock.patch.object(
                            RasterSrcTile, "upload", return_value=None
                        ):
                            with mock.patch.object(
                                RasterSrcTile, "rm_local_src", return_value=None
                            ):
                                result = pipe.create_tiles()
                                assert len(result) == 648


def test_filter_src_tiles():
    with mock.patch.object(RasterSrcTile, "src_tile_intersects", return_value=False):
        tiles = _get_subset_tiles()
        tiles = PIPE.filter_src_tiles(tiles)
        i = 0
        for tile in tiles:
            i += 1
            assert isinstance(tile, RasterSrcTile)
        assert i == 0

    with mock.patch.object(RasterSrcTile, "src_tile_intersects", return_value=True):
        tiles = _get_subset_tiles()
        tiles = PIPE.filter_src_tiles(tiles)
        i = 0
        for tile in tiles:
            i += 1
            assert isinstance(tile, RasterSrcTile)
        assert i == 4


def test_transform():
    with mock.patch.object(RasterSrcTile, "transform", return_value=None):
        tiles = _get_subset_tiles()
        tiles = PIPE.transform(tiles)
        i = 0
        for tile in tiles:
            i += 1
            assert isinstance(tile, RasterSrcTile)
        assert i == 4


def test_compress():
    with mock.patch.object(RasterSrcTile, "compress", return_value=None):
        tiles = _get_subset_tiles()
        tiles = PIPE.compress(tiles)
        i = 0
        for tile in tiles:
            i += 1
            assert isinstance(tile, RasterSrcTile)
        assert i == 4


def _get_subset_tiles() -> Set[RasterSrcTile]:
    raster_layer: Dict[str, Any] = {
        "name": "erosion_risk",
        "version": "v201911",
        "field": "level",
        "grid": GRID_1,
    }

    layer = layers.layer_factory(**raster_layer)

    assert isinstance(layer, layers.RasterSrcLayer)

    pipe = RasterPipe(layer)

    tiles = set()
    for i in range(10, 12):
        for j in range(10, 12):
            origin = pipe.grid.xy_grid_origin(j, i)
            tiles.add(RasterSrcTile(origin=origin, grid=pipe.grid, layer=layer))

    return tiles
