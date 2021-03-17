import os
from copy import deepcopy
from unittest import mock

from gfw_pixetl.models.pydantic import RasterLayerModel
from gfw_pixetl.pipes import RasterPipe
from gfw_pixetl.pixetl import pixetl
from tests.conftest import minimal_layer_dict

LAYER_DICT = deepcopy(minimal_layer_dict)
LAYER_DICT.update(
    {
        "dataset": "aqueduct_erosion_risk",
        "version": "v201911",
        "pixel_meaning": "level",
        "grid": "1/4000",
    }
)

RASTER_LAYER_DEF = RasterLayerModel(**LAYER_DICT)

SUBSET = ["10N_010E"]


def test_pixetl():

    cwd = os.getcwd()

    with mock.patch.object(
        RasterPipe, "create_tiles", return_value=(list(), list(), list(), list())
    ):
        tiles, skipped_tiles, failed_tiles, existing_tiles = pixetl(
            RASTER_LAYER_DEF,
            subset=SUBSET,
            overwrite=True,
        )

    assert tiles == list()
    assert skipped_tiles == list()
    assert failed_tiles == list()
    assert existing_tiles == list()
    assert cwd == os.getcwd()

    os.chdir(cwd)
