import os

from pydantic import ValidationError

from gfw_pixetl.models import LayerModel
from tests import minimal_layer_dict

os.environ["ENV"] = "test"


def test_models_bad_resamp():
    LAYER_DICT_BAD_VALS = {
        **minimal_layer_dict,
        "dataset": "aqueduct_erosion_risk",
        "version": "v201911",
        "pixel_meaning": "level",
        "grid": "1/4000",
        "resampling": "chaos",
    }
    try:
        _ = LayerModel.parse_obj(LAYER_DICT_BAD_VALS)
    except ValidationError:
        pass
