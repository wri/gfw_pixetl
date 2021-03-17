import copy
import math

import pytest
from pydantic import ValidationError
from rasterio.enums import Resampling

from gfw_pixetl.data_type import DataTypeEnum
from gfw_pixetl.models.pydantic import LayerModel, RasterLayerModel, VectorLayerModel
from gfw_pixetl.resampling import resampling_factory
from tests.conftest import minimal_layer_dict


def test_models_good_rasterize_method():
    good_layer_dict = {
        **minimal_layer_dict,
        "source_type": "vector",
        "rasterize_method": "count",
    }
    VectorLayerModel(**good_layer_dict)


def test_models_bad_rasterize_method():
    bad_layer_dict = {
        **minimal_layer_dict,
        "source_type": "vector",
        "rasterize_method": "random",
    }
    with pytest.raises(ValidationError, match=".*rasterize_method.*"):
        VectorLayerModel(**bad_layer_dict)


def test_models_bad_source_type():
    bad_layer_dict = {**minimal_layer_dict, "source_type": "frog"}
    with pytest.raises(ValidationError, match=r".*source_type.*"):
        RasterLayerModel(**bad_layer_dict)
    with pytest.raises(ValidationError, match=r".*source_type.*"):
        VectorLayerModel(**bad_layer_dict)


def test_models_bad_order():
    bad_layer_dict = {**minimal_layer_dict, "order": "random"}
    with pytest.raises(ValidationError, match=r".*order.*"):
        VectorLayerModel.parse_obj(bad_layer_dict)


def test_models_nan():
    good_layer_dict = copy.deepcopy(minimal_layer_dict)
    good_layer_dict.update(no_data="NaN")
    layer = LayerModel.parse_obj(good_layer_dict)
    assert math.isnan(layer.no_data)

    bad_layer_dict = copy.deepcopy(minimal_layer_dict)
    bad_layer_dict.update(no_data="NaNa")
    with pytest.raises(ValidationError):
        LayerModel.parse_obj(bad_layer_dict)


def test_version_pattern():
    good_versions = ["v2019", "v201911", "v20191122", "v1", "v1.2", "v1.2.3"]
    bad_versions = ["v1.beta", "1.2", "version1.2.3", "v.1.2.3"]

    for v in good_versions:
        good_layer_dict = copy.deepcopy(minimal_layer_dict)
        good_layer_dict.update(version=v)
        layer = LayerModel(**good_layer_dict)
        assert layer.version == v

    for v in bad_versions:
        bad_layer_dict = copy.deepcopy(minimal_layer_dict)
        bad_layer_dict.update(version=v)
        with pytest.raises(ValidationError):
            LayerModel(**bad_layer_dict)


def test_layer_model():
    with pytest.raises(ValidationError):
        RasterLayerModel(
            dataset="test",
            version="v1.1.1",
            source_type="raster",
            pixel_meaning="test",
            data_type=DataTypeEnum.uint8,
            nbits=6,
            no_data=0,
            grid="10/40000",
            resampling="wrong",
            source_uri=["s3://test/tiles.geojson"],
        )

    layer_def = RasterLayerModel(
        dataset="test",
        version="v1.1.1",
        source_type="raster",
        pixel_meaning="test",
        data_type=DataTypeEnum.uint8,
        nbits=6,
        no_data=0,
        grid="10/40000",
        resampling="bilinear",
        source_uri=["s3://test/tiles.geojson"],
    )

    resampling = resampling_factory(layer_def.resampling)

    assert resampling == Resampling.bilinear


def test_layer_model_floats():
    layer_def = RasterLayerModel(
        dataset="test",
        version="v1.1.1",
        source_type="raster",
        pixel_meaning="test",
        data_type=DataTypeEnum.float32,
        nbits=6,
        grid="10/40000",
        resampling="bilinear",
        source_uri=["s3://test/tiles.geojson"],
        no_data="nan",
    )

    assert isinstance(layer_def.no_data, float)
    assert math.isnan(layer_def.no_data)

    layer_def = RasterLayerModel(
        dataset="test",
        version="v1.1.1",
        source_type="raster",
        pixel_meaning="test",
        data_type=DataTypeEnum.float32,
        nbits=6,
        grid="10/40000",
        resampling="bilinear",
        source_uri=["s3://test/tiles.geojson"],
        no_data="2.2",
    )

    assert isinstance(layer_def.no_data, float)
