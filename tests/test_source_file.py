import os
from typing import List, Set

import yaml

from gfw_pixetl.data_type import DataType, data_type_factory

os.environ["ENV"] = "test"

FIXURES = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "gfw_pixetl", "fixures"
)

with open(os.path.join(FIXURES, "sources.yaml"), "r") as stream:
    SOURCES = yaml.load(stream, Loader=yaml.BaseLoader)

FIELD_MANDATORY: List[str] = ["grids", "data_type"]
FIELD_OPTIONAL: List[str] = ["no_data", "nbits"]
FIELD_POSSIBLE: Set[str] = set(FIELD_MANDATORY) | set(FIELD_OPTIONAL)

GRID_MANDATORY: List[str] = ["type"]
GRID_TYPE_VALUE: List[str] = ["raster", "vector"]

GRID_VECTOR_MANDATORY: List[str] = []
GRID_VECTOR_OPTIONAL: List[str] = ["order", "rasterize_method"]
GRID_VECTOR_POSSIBLE: Set[str] = (
    set(GRID_MANDATORY) | set(GRID_VECTOR_MANDATORY)
) | set(GRID_VECTOR_OPTIONAL)

GRID_RASTER_MUTUALLY_EXCLUSIVE: List[str] = ["uri", "depends_on"]
GRID_RASTER_OPTIONAL: List[str] = ["calc", "resampling"]


def test_layers_have_at_least_one_field():
    for layer in SOURCES:
        assert len(SOURCES[layer]) >= 1, f"{layer} has no fields"


def test_fields_have_mandatory_args():
    for layer in SOURCES:
        for field in SOURCES[layer]:
            args = SOURCES[layer][field].keys()
            assert set(FIELD_MANDATORY) == set(FIELD_MANDATORY) & set(
                args
            ), f"{layer}/{field} misses a mandatory field"


def test_fields_have_only_mandatory_and_optional_args():
    for layer in SOURCES:
        for field in SOURCES[layer]:
            args = SOURCES[layer][field].keys()
            assert (
                set() == (set(FIELD_POSSIBLE) | set(args)) ^ FIELD_POSSIBLE
            ), f"{layer}/{field} has an invalid field"


def test_grids_have_mandatory_args():
    for layer in SOURCES:
        for field in SOURCES[layer]:
            for grid in SOURCES[layer][field]["grids"]:
                args = SOURCES[layer][field]["grids"][grid].keys()
                assert set(GRID_MANDATORY) == set(GRID_MANDATORY) & set(
                    args
                ), f"{layer}/{field}/grids/{grid} misses mandatory field"


def test_grids_types_are_valid():
    for layer in SOURCES:
        for field in SOURCES[layer]:
            for grid in SOURCES[layer][field]["grids"]:
                arg = SOURCES[layer][field]["grids"][grid]["type"]
                assert (
                    arg in GRID_TYPE_VALUE
                ), f"{layer}/{field}/grids/{grid}/type as an invalid value"


def test_grid_vector_have_only_mandatory_and_optional_args():
    for layer in SOURCES:
        for field in SOURCES[layer]:
            for grid in SOURCES[layer][field]["grids"]:
                if SOURCES[layer][field]["grids"][grid]["type"] == "vector":
                    args = SOURCES[layer][field]["grids"][grid].keys()
                    assert (
                        set()
                        == (set(GRID_VECTOR_POSSIBLE) | set(args))
                        ^ GRID_VECTOR_POSSIBLE
                    ), f"{layer}/{field}/grids/{grid} has an invalid field"


def test_grid_raster_have_no_mutually_exclusive_and_optional_args():
    for layer in SOURCES:
        for field in SOURCES[layer]:
            for grid in SOURCES[layer][field]["grids"]:
                if SOURCES[layer][field]["grids"][grid]["type"] == "raster":
                    args = SOURCES[layer][field]["grids"][grid].keys()
                    result = list()
                    for grme in GRID_RASTER_MUTUALLY_EXCLUSIVE:
                        POSSIBLE = ({grme} | set(GRID_MANDATORY)) | set(
                            GRID_RASTER_OPTIONAL
                        )
                        result.append((POSSIBLE | set(args)) ^ POSSIBLE)
                    assert (
                        set() in result
                    ), f"{layer}/{field}/grids/{grid} has an invalid field"


def test_data_types():
    for layer in SOURCES:
        for field in SOURCES[layer]:
            assert isinstance(
                data_type_factory(SOURCES[layer][field]["data_type"]), DataType
            ), f"{layer}/{field} has invalid datatype"
