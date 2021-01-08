import os

from rasterio.warp import Resampling

from gfw_pixetl import layers
from gfw_pixetl.models.pydantic import LayerModel
from tests import minimal_layer_dict

os.environ["ENV"] = "test"


def test_raster_layer_uri():
    layer_dict = {
        **minimal_layer_dict,
        "no_data": 0,
    }
    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))

    assert isinstance(layer, layers.RasterSrcLayer)
    assert layer.__class__.__name__ == "RasterSrcLayer"
    assert layer.dst_profile["dtype"] == "uint16"
    assert layer.dst_profile["compress"] == "DEFLATE"
    assert layer.dst_profile["tiled"] is True
    assert layer.dst_profile["blockxsize"] == 400
    assert layer.dst_profile["blockysize"] == 400
    assert layer.dst_profile["pixeltype"] == "DEFAULT"
    assert layer.dst_profile["nodata"] == 0
    assert layer.resampling == Resampling.nearest
    assert layer.calc is None
    assert layer.rasterize_method is None
    assert layer.order is None


def test_raster_layer_depended():
    layer_dict = {
        **minimal_layer_dict,
        "data_type": "uint8",
        "nbits": 3,
        "grid": "90/27008",
        "no_data": 0,
        "calc": "1*(A>10)+1*(A>15)+1*(A>20)+1*(A>25)+1*(A>30)+1*(A>50)+1*(A>75)",
    }
    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))

    assert isinstance(layer, layers.RasterSrcLayer)
    assert layer.__class__.__name__ == "RasterSrcLayer"
    assert layer.dst_profile["dtype"] == "uint8"
    assert layer.dst_profile["compress"] == "DEFLATE"
    assert layer.dst_profile["tiled"] is True
    assert layer.dst_profile["blockxsize"] == 128
    assert layer.dst_profile["blockysize"] == 128
    assert layer.dst_profile["pixeltype"] == "DEFAULT"
    assert layer.dst_profile["nodata"] == 0
    assert layer.dst_profile["nbits"] == 3
    assert layer.resampling == Resampling.nearest
    assert (
        layer.calc == "1*(A>10)+1*(A>15)+1*(A>20)+1*(A>25)+1*(A>30)+1*(A>50)+1*(A>75)"
    )
    assert layer.order is None


def test_raster_calc_layer():
    layer_dict = {
        **minimal_layer_dict,
        "data_type": "uint8",
        "nbits": 3,
        "no_data": 0,
        "calc": "1*(A>10)+1*(A>15)+1*(A>20)+1*(A>25)+1*(A>30)+1*(A>50)+1*(A>75)",
        "resampling": "nearest",
    }
    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))

    assert isinstance(layer, layers.RasterSrcLayer)
    assert layer.__class__.__name__ == "RasterSrcLayer"
    assert layer.dst_profile["dtype"] == "uint8"
    assert layer.dst_profile["compress"] == "DEFLATE"
    assert layer.dst_profile["tiled"] is True
    assert layer.dst_profile["blockxsize"] == 400
    assert layer.dst_profile["blockysize"] == 400
    assert layer.dst_profile["pixeltype"] == "DEFAULT"
    assert layer.dst_profile["nodata"] == 0
    assert layer.dst_profile["nbits"] == 3
    assert layer.resampling == Resampling.nearest
    assert (
        layer.calc == "1*(A>10)+1*(A>15)+1*(A>20)+1*(A>25)+1*(A>30)+1*(A>50)+1*(A>75)"
    )
    assert layer.rasterize_method is None
    assert layer.order is None


def test_vector_layer():
    layer_dict = {
        **minimal_layer_dict,
        "source_type": "vector",
        "no_data": 0,
        "nbits": 2,
        "data_type": "uint8",
        "order": "desc",
    }
    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))

    assert isinstance(layer, layers.VectorSrcLayer)
    assert layer.__class__.__name__ == "VectorSrcLayer"
    assert layer.dst_profile["dtype"] == "uint8"
    assert layer.dst_profile["compress"] == "DEFLATE"
    assert layer.dst_profile["tiled"] is True
    assert layer.dst_profile["blockxsize"] == 400
    assert layer.dst_profile["blockysize"] == 400
    assert layer.dst_profile["pixeltype"] == "DEFAULT"
    assert layer.dst_profile["nodata"] == 0
    assert layer.dst_profile["nbits"] == 2
    assert layer.calc == "Mg_ha-1"
    assert layer.resampling == Resampling.nearest
    assert layer.rasterize_method is None
    assert layer.order == "desc"
