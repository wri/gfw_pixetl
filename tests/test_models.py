import os
import unittest

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
