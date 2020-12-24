import math
from enum import Enum
from typing import Callable, Dict, Optional, Union

from pydantic.types import StrictFloat, StrictInt

from gfw_pixetl import get_module_logger

LOGGER = get_module_logger(__name__)


class DataTypeEnum(str, Enum):
    boolean = "boolean"
    uint8 = "uint8"
    int8 = "int8"
    uint16 = "uint16"
    int16 = "int16"
    uint32 = "uint32"
    int32 = "int32"
    float16 = "float16"
    half = "half"
    float32 = "float32"
    single = "single"
    float64 = "float64"
    double = "double"


class DataType(object):
    def __init__(
        self,
        data_type: str,
        no_data: Optional[Union[StrictInt, StrictFloat]],
        nbits: Optional[StrictInt] = None,
        compression: str = "DEFLATE",
    ) -> None:
        self._validate_no_data(data_type, no_data, nbits)
        self.data_type: str = data_type
        self.no_data: Optional[Union[StrictInt, StrictFloat]] = no_data
        self.nbits: Optional[StrictInt] = nbits
        self.compression: str = compression

        if data_type == "int8":
            self.signed_byte = True
        else:
            self.signed_byte = False

    def has_no_data(self):
        return self.no_data == 0 or self.no_data or math.isnan(self.no_data)

    @staticmethod
    def _validate_no_data(
        data_type: str,
        no_data: Optional[Union[StrictInt, StrictFloat]],
        nbits: Optional[StrictInt],
    ):
        dtype = data_type.lower()

        if "int" in dtype and (no_data is not None and not isinstance(no_data, int)):
            message = f"No data value {no_data} must be of type `int` or None for data type {data_type}"
            raise ValueError(message)
        elif (
            ("float" in dtype or dtype in ["half", "single", "double"])
            and (no_data is not None)
            and (not isinstance(no_data, float))
        ):
            message = f"No data value {no_data} must be of type `float` or None for data type {data_type}"
            raise ValueError(message)
        elif nbits == 1 and (no_data != 0 and no_data is not None):
            message = f"No data value {no_data} must be 0 or None for data type Boolean"
            raise ValueError(message)


def data_type_constructor(
    data_type: str,
    nbits: Optional[int] = None,
    compression: str = "DEFLATE",
):
    """Using closure design for a data type constructor."""

    def get_data_type(no_data):
        return DataType(
            data_type=data_type, no_data=no_data, nbits=nbits, compression=compression
        )

    return get_data_type


def data_type_factory(
    data_type: str,
    nbits: Optional[int] = None,
    no_data: Optional[Union[StrictInt, StrictFloat]] = None,
) -> DataType:
    _8bits: Optional[int] = None if not nbits or nbits not in range(1, 8) else nbits
    _16bits: Optional[int] = None if not nbits or nbits not in range(9, 16) else nbits
    _32bits: Optional[int] = None if not nbits or nbits not in range(17, 32) else nbits

    dtypes_constructor: Dict[str, Callable] = {
        DataTypeEnum.boolean: data_type_constructor(
            "uint8", nbits=1, compression="CCITTFAX4"
        ),
        DataTypeEnum.uint8: data_type_constructor("uint8", _8bits),
        DataTypeEnum.int8: data_type_constructor("int8", _8bits),
        DataTypeEnum.uint16: data_type_constructor("uint16", _16bits),
        DataTypeEnum.int16: data_type_constructor("int16", _16bits),
        DataTypeEnum.uint32: data_type_constructor("uint32", _32bits),
        DataTypeEnum.int32: data_type_constructor("int32", _32bits),
        DataTypeEnum.float16: data_type_constructor("float16", 16),
        DataTypeEnum.half: data_type_constructor("float16", 16),
        DataTypeEnum.float32: data_type_constructor("float32"),
        DataTypeEnum.single: data_type_constructor("float32"),
        DataTypeEnum.float64: data_type_constructor("float64"),
        DataTypeEnum.double: data_type_constructor("float64"),
    }

    dtype = data_type.lower()

    try:
        dt = dtypes_constructor[dtype](no_data)

    except KeyError:
        message = "Unknown data type {}".format(data_type)
        LOGGER.exception(message)
        raise ValueError(message)

    return dt


def to_gdal_data_type(data_type: str) -> str:
    if data_type == "bool_" or data_type == "uint8" or data_type == "int8":
        return "Byte"
    elif data_type == "float16":
        return "Float32"
    elif data_type[0] == "u":
        return "U" + data_type[1:].capitalize()
    else:
        return data_type.capitalize()


def from_gdal_data_type(data_type: str) -> str:
    if data_type == "Byte":
        return "uint8"
    else:
        return data_type.lower()
