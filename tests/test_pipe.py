import os
from typing import Any, Dict

from gfw_pixetl import layers, get_module_logger
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.pipes import Pipe

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

    result = PIPE.filter_subset_tiles(TILES)

    i = 0
    for r in result:
        i += 1
    assert i == len(SUBSET)
