from typing import Optional, Union

from gfw_pixetl import get_module_logger

logger = get_module_logger(__name__)


class DataType(object):
    def __init__(
        self,
        data_type: str,
        no_data: Optional[int],
        nbits: Optional[int],
        compression: str,
    ) -> None:
        self.data_type: str = data_type
        self.no_data: Union[Optional[int], Optional[float]] = no_data
        self.nbits: Optional[int] = nbits
        self.compression: str = compression

    def to_numpy_dt(self):
        if self.data_type == "Byte":
            if self.nbits == 1:
                return "bool_"
            else:
                return "uint8"
        elif self.data_type == "Float32" and self.nbits == 16:
            return "float16"
        else:
            return self.data_type.lower()


int_dt = ["uint", "uint8", "uint16", "uint32", "int16", "int32"]
float_dt = ["float16", "float32", "float64", "half", "single", "double"]


def data_type_factory(
    data_type: str, nbits: Optional[int] = None, no_data: Optional[int] = None, **kwargs
) -> DataType:

    dt: DataType

    if data_type.lower() == "boolean":
        dt = DataType(data_type="Byte", no_data=0, nbits=1, compression="CCITTFAX4")

    elif data_type.lower() in int_dt:
        dt = _int_data_type(data_type, no_data, nbits)

    elif data_type.lower() in float_dt:
        dt = _float_data_type(data_type, no_data)

    else:
        message = "Unknown data type {}".format(data_type)
        logger.exception(message)
        raise ValueError(message)

    return dt


def _int_data_type(dtype, ndata, n):
    if dtype.lower() == "uint" or dtype.lower() == "uint8":
        bits = 8
    elif dtype.lower() == "uint16" or dtype.lower() == "int16":
        bits = 16
    elif dtype.lower() == "uint32" or dtype.lower() == "int32":
        bits = 32
    else:
        raise ValueError("Not an known integer type")

    if dtype.lower()[:1] == "u":
        data_type = "UInt{}".format(bits)
        no_data = 0 if not ndata else int(ndata)
    else:
        data_type = "Int{}".format(bits)
        no_data = int(ndata) if ndata == 0 or ndata else None

    if bits == 8:
        data_type = "Byte"

    nbits = None if not n or int(n) not in range(bits - 7, bits) else int(n)
    compression = "DEFLATE"

    return DataType(data_type, no_data, nbits, compression)


def _float_data_type(dtype, ndata):

    no_data = float(ndata) if ndata == 0 or ndata else None
    compression = "DEFLATE"
    nbits = None
    data_type = "Float32"

    if dtype.lower() == "float16" or dtype.lower() == "half":
        nbits = 16

    elif dtype.lower() == "float64" or dtype.lower() == "double":
        data_type = "Float64"

    return DataType(data_type, no_data, nbits, compression)
