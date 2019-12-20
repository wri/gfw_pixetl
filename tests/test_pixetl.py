from unittest import mock

from gfw_pixetl.pipes import CalcRasterPipe, RasterPipe, Pipe
from gfw_pixetl.grids import grid_factory, Grid
from gfw_pixetl.layers import layer_factory, Layer
from gfw_pixetl.pixetl import pixetl

GRID_NAME = "1/4000"
GRID: Grid = grid_factory(GRID_NAME)
FIELD = "level"
NAME = "erosion_risk"
VERSION = "v201911"
SUBSET = ["10N_010E"]

LAYER: Layer = layer_factory(name=NAME, version=VERSION, grid=GRID, field=FIELD)


def test_pixetl():
    with mock.patch.object(RasterPipe, "create_tiles", return_value=list()):
        tiles = pixetl(
            name=NAME,
            version=VERSION,
            source_type="raster",
            field=FIELD,
            grid_name=GRID_NAME,
            env="test",
            subset=SUBSET,
            overwrite=True,
            debug=True,
            cwd="/tmp",
        )
        assert tiles == list()
