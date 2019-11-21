from typing import Optional

from gfw_pixetl import get_module_logger

logger = get_module_logger(__name__)


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

    def to_numpy_dt(self):
        if self.data_type == "Byte" and self.nbits == 1:
            return "bool_"
        elif self.data_type == "Byte":
            return "uint8"
        elif self.data_type == "Float32" and self.nbits == 16:
            return "float16"
        else:
            return self.data_type.lower()


def data_type_factory(
    data_type: str, nbits: Optional[int] = None, no_data: Optional[int] = None
) -> DataType:

    _8bits: Optional[int] = None if not nbits and nbits not in range(1, 8) else nbits
    _16bits: Optional[int] = None if not nbits and nbits not in range(9, 16) else nbits
    _32bits: Optional[int] = None if not nbits and nbits not in range(17, 32) else nbits
    no_data_0: int = 0 if not no_data else no_data
    no_data_none: Optional[int] = None if not no_data else no_data

    dtype: str = data_type.lower()

    if dtype == "boolean":
        dt = DataType(data_type="Byte", no_data=0, nbits=1, compression="CCITTFAX4")

    elif dtype == "uint":
        return DataType(data_type="Byte", no_data=no_data_0, nbits=_8bits)

    elif dtype == "int":
        dt = DataType(data_type="Byte", no_data=no_data_none, nbits=_8bits)

    elif dtype == "uint16":
        dt = DataType(data_type="UInt16", no_data=no_data_0, nbits=_16bits)

    elif dtype == "int16":
        dt = DataType(data_type="Int16", no_data=no_data_none, nbits=_16bits)

    elif dtype == "uint32":
        dt = DataType(data_type="UInt32", no_data=no_data_0, nbits=_32bits)

    elif dtype == "int32":
        dt = DataType(data_type="Int32", no_data=no_data_none, nbits=_32bits)

    elif dtype == "float16" or dtype == "half":
        dt = DataType(data_type="Float32", no_data=no_data_none, nbits=16)

    elif dtype == "float32" or dtype == "single":
        dt = DataType(data_type="Float32", no_data=no_data_none)

    elif dtype == "float64" or dtype == "double":
        dt = DataType(data_type="Float64", no_data=no_data_none)

    else:
        message = "Unknown data type {}".format(data_type)
        logger.exception(message)
        raise ValueError(message)

    return dt
