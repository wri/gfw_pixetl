import os
from unittest import mock

from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.pipes import RasterPipe
from gfw_pixetl.pixetl import pixetl
from tests import minimal_layer_dict

os.environ["ENV"] = "test"

LAYER_DICT = {
    **minimal_layer_dict,
    "dataset": "aqueduct_erosion_risk",
    "version": "v201911",
    "pixel_meaning": "level",
    "grid": "1/4000",
}
RASTER_LAYER_DEF = LayerModel.parse_obj(LAYER_DICT)

SUBSET = ["10N_010E"]


def test_pixetl():

    cwd = os.getcwd()

    with mock.patch.object(
        RasterPipe, "create_tiles", return_value=(list(), list(), list())
    ):
        tiles, skipped_tiles, failed_tiles = pixetl(
            RASTER_LAYER_DEF,
            subset=SUBSET,
            overwrite=True,
        )

    assert tiles == list()
    assert skipped_tiles == list()
    assert failed_tiles == list()
    assert cwd == os.getcwd()

    os.chdir(cwd)
