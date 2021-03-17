from gfw_pixetl import layers
from gfw_pixetl.models.pydantic import RasterLayerModel
from gfw_pixetl.pipes import RasterPipe, pipe_factory
from tests.conftest import minimal_layer_dict

SUBSET = ["10N_010E", "20N_010E", "30N_010E"]


def test_pipe_factory_vector_src_layer():
    pass


def test_pipe_factory_raster_src_layer():
    layer_dict = {
        **minimal_layer_dict,
        "dataset": "aqueduct_erosion_risk",
        "version": "v201911",
        "pixel_meaning": "level",
        "no_data": 0,
        "source_type": "raster",
    }
    layer = layers.layer_factory(RasterLayerModel.parse_obj(layer_dict))

    pipe = pipe_factory(layer)
    assert isinstance(pipe, RasterPipe)
    assert pipe.__class__.__name__ == "RasterPipe"

    pipe = pipe_factory(layer, SUBSET)
    assert isinstance(pipe, RasterPipe)
    assert pipe.__class__.__name__ == "RasterPipe"
