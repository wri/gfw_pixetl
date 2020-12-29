import math
import os

import pytest

from gfw_pixetl.data_type import DataType, DataTypeEnum, data_type_factory

os.environ["ENV"] = "test"


def test_data_type():
    for dt in DataTypeEnum.__members__.keys():
        data_type: DataType = data_type_factory(dt)
        assert isinstance(data_type, DataType)
        if dt == "boolean":
            assert data_type.data_type == "uint8"
        elif dt == "half":
            assert data_type.data_type == "float16"
        elif dt == "single":
            assert data_type.data_type == "float32"
        elif dt == "double":
            assert data_type.data_type == "float64"
        elif "int" in dt or "float" in dt:
            assert data_type.data_type == DataTypeEnum.__members__[dt]

        # assert to_gdal_dt(data_type.data_type) == dtypes_dict[dt][0]


def test_nbits():
    data_type: DataType = data_type_factory("boolean")
    assert data_type.nbits == 1

    data_type: DataType = data_type_factory("uint8", nbits=5)
    assert data_type.nbits == 5

    data_type: DataType = data_type_factory("half")
    assert data_type.nbits == 16


def test_no_data():
    data_type: DataType = data_type_factory("boolean")
    assert data_type.no_data is None

    data_type: DataType = data_type_factory("boolean", no_data=0)
    assert data_type.no_data == 0

    with pytest.raises(ValueError):
        data_type_factory("boolean", no_data=1)

    data_type = data_type_factory("uint8")
    assert data_type.no_data is None

    data_type = data_type_factory("uint8", no_data=0)
    assert data_type.no_data == 0

    data_type = data_type_factory("int8")
    assert data_type.no_data is None

    data_type = data_type_factory("int8", no_data=1)
    assert data_type.no_data == 1

    data_type = data_type_factory("float32")
    assert data_type.no_data is None

    data_type = data_type_factory("float32", no_data=math.nan)
    assert math.isnan(data_type.no_data)

    data_type = data_type_factory("float32", no_data=0.0)
    assert data_type.no_data == 0.0

    # Note: Surely this test is wrong? Ask Thomas.
    # with pytest.raises(ValueError):
    #     data_type_factory("float32", no_data=1.1)
    # Surely this should be the correct behavior
    data_type = data_type_factory("float32", no_data=1.1)
    assert data_type.no_data == 1.1

    with pytest.raises(ValueError):
        data_type_factory("float32", no_data=1)


def test_signed_int():
    data_type: DataType = data_type_factory("int8")
    assert data_type.signed_byte is True
