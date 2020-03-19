import os

from rasterio.warp import Resampling

from gfw_pixetl import layers
from gfw_pixetl.grids import grid_factory

os.environ["ENV"] = "test"

RASTER_LAYER = {
    "name": "whrc_aboveground_biomass_stock_2000",
    "version": "v201911",
    "field": "Mg_ha-1",
    "grid": grid_factory("10/40000"),
}


RASTER_CALC_LAYER = {
    "name": "umd_tree_cover_density_2000",
    "version": "v1.6",
    "field": "threshold",
    "grid": grid_factory("10/40000"),
}


# VECTOR_LAYER = {
#     "name": "wdpa_protected_areas",
#     "version": "v201911",
#     "field": "iucn_cat",
#     "grid": grid_factory("10/40000"),
# }


def test__get_source_type():
    assert (
        layers._get_source_type(
            RASTER_LAYER["name"], RASTER_LAYER["field"], RASTER_LAYER["grid"].name
        )
        == "raster"
    )


def test_raster_layer_uri():
    layer = layers.layer_factory(
        RASTER_LAYER["name"],
        RASTER_LAYER["version"],
        RASTER_LAYER["field"],
        RASTER_LAYER["grid"],
    )
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
    grid = grid_factory("90/27008")
    layer = layers.layer_factory(
        RASTER_CALC_LAYER["name"],
        RASTER_CALC_LAYER["version"],
        RASTER_CALC_LAYER["field"],
        grid,
    )
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
    layer = layers.layer_factory(
        RASTER_CALC_LAYER["name"],
        RASTER_CALC_LAYER["version"],
        RASTER_CALC_LAYER["field"],
        RASTER_CALC_LAYER["grid"],
    )
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


#
# def test_vector_layer():
#     layer = layers.layer_factory(
#         VECTOR_LAYER["name"],
#         VECTOR_LAYER["version"],
#         VECTOR_LAYER["field"],
#         VECTOR_LAYER["grid"],
#     )
#     assert isinstance(layer, layers.VectorSrcLayer)
#     assert layer.__class__.__name__ == "VectorSrcLayer"
#     assert layer.dst_profile["dtype"] == "uint8"
#     assert layer.dst_profile["compress"] == "DEFLATE"
#     assert layer.dst_profile["tiled"] is True
#     assert layer.dst_profile["blockxsize"] == 400
#     assert layer.dst_profile["blockysize"] == 400
#     assert layer.dst_profile["pixeltype"] == "DEFAULT"
#     assert layer.dst_profile["nodata"] == 0
#     assert layer.dst_profile["nbits"] == 2
#     assert layer.resampling is None
#     assert layer.calc is None
#     assert layer.rasterize_method is None
#     assert layer.order == "desc"
