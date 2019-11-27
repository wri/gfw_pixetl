from typing import Optional

from gfw_pixetl import get_module_logger

logger = get_module_logger(__name__)


dtypes_dict = {
    "boolean": ("Byte", "bool_"),
    "uint": ("Byte", "uint8"),
    "int": ("Byte", "int8"),
    "uint16": ("UInt16", "uint16"),
    "int16": ("Int16", "int16"),
    "uint32": ("UInt32", "uint32"),
    "int32": ("Int32", "int32"),
    "float16": ("Float32", "float16"),
    "half": ("Float32", "float16"),
    "float32": ("Float32", "float32"),
    "single": ("Float32", "float32"),
    "float64": ("Float64", "float64"),
    "double": ("Float64", "float64"),
}


class DataType(object):
    def __init__(
        self,
        data_type: str,
        no_data: Optional[int],
        nbits: Optional[int] = None,
        compression: str = "DEFLATE",
    ) -> None:
        self.data_type: str = data_type
        self.no_data: Optional[int] = no_data
        self.nbits: Optional[int] = nbits
        self.compression: str = compression

        if data_type == "int8":
            self.signed_byte = True
        else:
            self.signed_byte = False

    def has_no_data(self):
        return self.no_data == 0 or self.no_data


def data_type_factory(
    data_type: str, nbits: Optional[int] = None, no_data: Optional[int] = None
) -> DataType:

    _8bits: Optional[int] = None if not nbits and nbits not in range(1, 8) else nbits
    _16bits: Optional[int] = None if not nbits and nbits not in range(9, 16) else nbits
    _32bits: Optional[int] = None if not nbits and nbits not in range(17, 32) else nbits
    no_data_0: int = 0 if not no_data else no_data
    no_data_none: Optional[int] = None if not no_data else no_data

    dtype = data_type.lower()
    try:
        dtype_numpy: str = dtypes_dict[dtype][1]
    except KeyError:
        message = "Unknown data type {}".format(data_type)
        logger.exception(message)
        raise ValueError(message)

    if dtype == "boolean":
        dt = DataType(
            data_type=dtype_numpy, no_data=0, nbits=1, compression="CCITTFAX4"
        )

    elif dtype == "uint":
        return DataType(data_type=dtype_numpy, no_data=no_data_0, nbits=_8bits)

    elif dtype == "int":
        dt = DataType(data_type=dtype_numpy, no_data=no_data_none, nbits=_8bits)

    elif dtype == "uint16":
        dt = DataType(data_type=dtype_numpy, no_data=no_data_0, nbits=_16bits)

    elif dtype == "int16":
        dt = DataType(data_type=dtype_numpy, no_data=no_data_none, nbits=_16bits)

    elif dtype == "uint32":
        dt = DataType(data_type=dtype_numpy, no_data=no_data_0, nbits=_32bits)

    elif dtype == "int32":
        dt = DataType(data_type=dtype_numpy, no_data=no_data_none, nbits=_32bits)

    elif dtype == "float16" or dtype == "half":
        dt = DataType(data_type=dtype_numpy, no_data=no_data_none, nbits=16)

    elif dtype == "float32" or dtype == "single":
        dt = DataType(data_type=dtype_numpy, no_data=no_data_none)

    elif dtype == "float64" or dtype == "double":
        dt = DataType(data_type=dtype_numpy, no_data=no_data_none)

    else:
        message = "Unknown data type {}".format(data_type)
        logger.exception(message)
        raise ValueError(message)

    return dt


def to_gdal_dt(data_type):
    if data_type == "bool_" or data_type == "uint8" or data_type == "int8":
        return "Byte"
    elif data_type == "float16":
        return "Float32"
    elif data_type[0] == "u":
        return "U" + data_type[1:].capitalize()
    else:
        return data_type.capitalize()
