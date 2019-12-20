import os
from typing import Any, Dict

from gfw_pixetl import layers
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.pipes import CalcRasterPipe, RasterPipe, pipe_factory

os.environ["ENV"] = "test"

GRID_10 = grid_factory("10/40000")
GRID_1 = grid_factory("1/4000")

SUBSET = ["10N_010E", "20N_010E", "30N_010E"]


def test_pipe_factory_vector_src_layer():
    pass


def test_pipe_factory_raster_src_layer():
    raster_layer: Dict[str, Any] = {
        "name": "erosion_risk",
        "version": "v201911",
        "field": "level",
        "grid": GRID_10,
    }

    layer = layers.layer_factory(**raster_layer)

    pipe = pipe_factory(layer)
    assert isinstance(pipe, RasterPipe)
    assert pipe.__class__.__name__ == "RasterPipe"

    pipe = pipe_factory(layer, SUBSET)
    assert isinstance(pipe, RasterPipe)
    assert pipe.__class__.__name__ == "RasterPipe"


def test_pipe_factory_calc_raster_src_layer():
    calc_raster_layer: Dict[str, Any] = {
        "name": "tcs_drivers_2000-2015",
        "version": "v201911",
        "field": "type",
        "grid": GRID_10,
    }

    layer = layers.layer_factory(**calc_raster_layer)

    pipe = pipe_factory(layer)
    assert isinstance(pipe, CalcRasterPipe)
    assert pipe.__class__.__name__ == "CalcRasterPipe"

    pipe = pipe_factory(layer, SUBSET)
    assert isinstance(pipe, CalcRasterPipe)
    assert pipe.__class__.__name__ == "CalcRasterPipe"
