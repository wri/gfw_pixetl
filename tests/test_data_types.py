import math
import os

from gfw_pixetl.data_type import DataType, data_type_factory, to_gdal_dt, DataTypeEnum

os.environ["ENV"] = "test"


def test_data_type():
    for dt in DataTypeEnum.__members__.keys():
        data_type: DataType = data_type_factory(dt)
        assert isinstance(data_type, DataType)
        assert data_type.data_type == DataTypeEnum.__members__[dt]
        # assert to_gdal_dt(data_type.data_type) == dtypes_dict[dt][0]


def test_nbits():
    data_type: DataType = data_type_factory("boolean")
    assert data_type.nbits == 1

    data_type: DataType = data_type_factory("uint", nbits=5)
    assert data_type.nbits == 5

    data_type: DataType = data_type_factory("half")
    assert data_type.nbits == 16


def test_no_data():
    data_type: DataType = data_type_factory("boolean")
    assert data_type.no_data == 0

    data_type = data_type_factory("uint")
    assert data_type.no_data == 0

    data_type = data_type_factory("int")
    assert data_type.no_data is None

    data_type = data_type_factory("int", no_data=1)
    assert data_type.no_data == 1

    data_type = data_type_factory("float", no_data=math.nan)
    assert math.isnan(data_type.no_data)


def test_signed_int():
    data_type: DataType = data_type_factory("int")
    assert data_type.signed_byte is True
