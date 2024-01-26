import os
from copy import deepcopy
from unittest import mock

import pytest

from gfw_pixetl.models.pydantic import LayerModel
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

RASTER_LAYER_DEF = LayerModel.parse_obj(LAYER_DICT)

SUBSET = ["10N_010E"]


@pytest.mark.parametrize("skip_upload", [True, False, None])
@pytest.mark.parametrize("skip_delete", [True, False, None])
def test_pixetl(skip_upload, skip_delete):

    cwd = os.getcwd()

    opt_kwargs = {}
    if skip_delete:
        opt_kwargs["skip_deletion"] = skip_delete
    if skip_upload:
        opt_kwargs["skip_upload"] = skip_upload

    with mock.patch.object(
        RasterPipe, "create_tiles", return_value=(list(), list(), list(), list())
    ) as mock_create_tiles, mock.patch(
        "gfw_pixetl.pixetl.remove_work_directory", return_value=None
    ) as mock_remove:
        tiles, skipped_tiles, failed_tiles, existing_tiles = pixetl(
            RASTER_LAYER_DEF, subset=SUBSET, overwrite=True, **opt_kwargs
        )
    mock_create_tiles.assert_called_once_with(
        True, remove_work=not skip_delete, upload=not skip_upload
    )
    assert tiles == list()
    assert skipped_tiles == list()
    assert failed_tiles == list()
    assert existing_tiles == list()
    assert cwd == os.getcwd()
    assert mock_remove.call_count == (0 if skip_delete else 1)

    os.chdir(cwd)
