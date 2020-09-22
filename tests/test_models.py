import copy
import math
import os
import unittest

import pytest
from pydantic import ValidationError

from gfw_pixetl.models import LayerModel
from tests import minimal_layer_dict

os.environ["ENV"] = "test"


class TestValidation(unittest.TestCase):
    def test_models_good_rasterize_method(self):
        good_layer_dict = {**minimal_layer_dict, "rasterize_method": "count"}
        _ = LayerModel.parse_obj(good_layer_dict)

    def test_models_bad_rasterize_method(self):
        bad_layer_dict = {**minimal_layer_dict, "rasterize_method": "random"}
        with self.assertRaises(ValidationError) as e:
            _ = LayerModel.parse_obj(bad_layer_dict)
            assert "rasterize_method" in str(e)

    def test_models_bad_source_type(self):
        bad_layer_dict = {**minimal_layer_dict, "source_type": "frog"}
        with self.assertRaises(ValidationError) as e:
            _ = LayerModel.parse_obj(bad_layer_dict)
            assert "source_type" in str(e)

    def test_models_bad_order(self):
        bad_layer_dict = {**minimal_layer_dict, "order": "random"}
        with self.assertRaises(ValidationError) as e:
            _ = LayerModel.parse_obj(bad_layer_dict)
            assert "order" in str(e)

    def test_models_nan(self):
        good_layer_dict = copy.deepcopy(minimal_layer_dict)
        good_layer_dict.update(no_data="NaN")
        layer = LayerModel.parse_obj(good_layer_dict)
        assert math.isnan(layer.no_data)

        bad_layer_dict = copy.deepcopy(minimal_layer_dict)
        bad_layer_dict.update(no_data="NaNa")
        _ = LayerModel.parse_obj(bad_layer_dict)
        assert pytest.raises(ValidationError)

    def test_version_pattern(self):
        good_versions = ["v2019", "v201911", "v20191122", "v1", "v1.2", "v1.2.3"]
        bad_versions = ["v1.beta", "1.2", "version1.2.3", "v.1.2.3"]

        for v in good_versions:
            good_layer_dict = copy.deepcopy(minimal_layer_dict)
            good_layer_dict.update(version=v)
            _ = LayerModel.parse_obj(good_layer_dict)

        for v in bad_versions:
            bad_layer_dict = copy.deepcopy(minimal_layer_dict)
            bad_layer_dict.update(version=v)
            assert pytest.raises(ValidationError)
