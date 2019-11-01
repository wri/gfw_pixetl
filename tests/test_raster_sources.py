import os

import yaml

from gfw_pixetl.data_type import DataType, data_type_factory


FIXURES = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "gfw_pixetl", "fixures"
)

with open(os.path.join(FIXURES, "raster_sources.yaml"), "r") as stream:
    SOURCES = yaml.load(stream, Loader=yaml.BaseLoader)

MANDATORY = ["field", "src_uri", "data_type"]
OPTIONAL = ["no_data", "nbits", "resampling", "single_tile"]


def test_mandatory_fields():

    for key in SOURCES.keys():
        for source in SOURCES[key]:
            for field in MANDATORY:
                assert (
                    field in source.keys()
                ), "Layer {} is missing mandatory field {}".format(key, field)


def test_all_fields():

    for key in SOURCES.keys():
        for source in SOURCES[key]:
            for field in source.keys():
                assert (
                    field in MANDATORY or field in OPTIONAL
                ), "Layer {} has unknown field {}".format(key, field)


def test_data_types():

    for key in SOURCES.keys():
        for source in SOURCES[key]:
            assert isinstance(
                data_type_factory(source["data_type"]), DataType
            ), "Layer {} has unknown datatype {}".format(key, source["data_type"])


def test_field():

    for key in SOURCES.keys():
        for source in SOURCES[key]:
            assert isinstance(
                source["field"], str
            ), "Field 'field' in Layer {} is not a string".format(key)


def test_src_uri():

    for key in SOURCES.keys():
        for source in SOURCES[key]:
            ext = os.path.splitext(source["src_uri"])[1]
            assert ext in [
                ".tif",
                ".vrt",
            ], "'src_uri' in Layer {} has unsupported file extension".format(key)
