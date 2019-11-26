import os

from gfw_pixetl import layers
from gfw_pixetl.grids import grid_factory

os.environ["ENV"] = "test"

LAYER = "aboveground_biomass_stock_2000"
VERSION = "v201911"
FIELD = "Mg_ha-1"
GRID = grid_factory("10/40000")


def test__get_source_type():
    assert layers._get_source_type(LAYER, FIELD, GRID.name) == "raster"


def test_raster_layer_uri():
    layer = layers.layer_factory(LAYER, VERSION, FIELD, GRID)
    assert isinstance(layer, layers.RasterSrcLayer)


def test_raster_layer_depended():
    grid = grid_factory("90/27008")
    layer = layers.layer_factory(LAYER, VERSION, FIELD, grid)
    assert isinstance(layer, layers.RasterSrcLayer)
