import logging
import os
from typing import Any, Dict

import yaml

from gfw_tile_prep.data_type_factory import data_type_factory
from gfw_tile_prep.layer import Layer
from gfw_tile_prep.vector_layer import VectorLayer
from gfw_tile_prep.raster_layer import RasterLayer


logger = logging.getLogger(__name__)


def layer_factory(layer_type, **kwargs) -> Layer:

    if layer_type == "vector":
        return _vector_layer_factory(**kwargs)

    elif layer_type == "raster":
        return _raster_layer_factory(**kwargs)
    else:
        raise ValueError("Unknown layer type")


def _vector_layer_factory(**kwargs) -> VectorLayer:

    with open(os.path.join(_cur_dir(), "fixures/vector_sources.yaml"), "r") as stream:
        sources = yaml.load(stream, Loader=yaml.BaseLoader)
    try:
        source = _get_source_by_field(sources[kwargs["name"]], kwargs["field"])
    except KeyError:
        raise ValueError("No such data layer")

    kwargs["field"] = source["field"]
    kwargs["data_type"] = data_type_factory(**source)

    if "order" in source.keys():
        kwargs["order"] = source["order"]
    if "rasterize_method" in source.keys():
        kwargs["rasterize_method"] = source["rasterize_method"]

    return VectorLayer(**kwargs)


def _raster_layer_factory(**kwargs) -> RasterLayer:

    with open(os.path.join(_cur_dir(), "fixures/raster_sources.yaml"), "r") as stream:
        sources = yaml.load(stream, Loader=yaml.BaseLoader)

    try:
        source = _get_source_by_field(sources[kwargs["name"]], kwargs["field"])
    except KeyError:
        raise ValueError("No such data layer")

    kwargs["field"] = source["field"]
    kwargs["data_type"] = data_type_factory(**source)
    kwargs["src_uri"] = source["src_uri"]
    if "single_tile" in source.keys():
        kwargs["single_tile"] = source["single_tile"]
    if "resampling" in source.keys():
        kwargs["resampling"] = source["resampling"]

    return RasterLayer(**kwargs)


def _get_source_by_field(sources, field) -> Dict[str, Any]:

    try:
        if field:
            for source in sources:
                if source["field"] == field:
                    return source
            raise ValueError("No such data field in source definition")
        else:
            return sources[0]
    except KeyError:
        raise ValueError("No such data field in source definition")


def _cur_dir():
    return os.path.dirname(os.path.abspath(__file__))
