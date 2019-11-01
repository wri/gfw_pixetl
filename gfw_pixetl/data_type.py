from typing import Optional

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
        self.no_data: Optional[int] = no_data
        self.nbits: Optional[int] = nbits
        self.compression: str = compression


def data_type_factory(
    data_type: str, nbits: Optional[int] = None, no_data: Optional[int] = None, **kwargs
) -> DataType:
    if data_type.lower() == "boolean":
        return DataType(data_type="Byte", no_data=0, nbits=1, compression="CCITTFAX4")

    elif data_type.lower() == "uint":
        return DataType(
            data_type="Byte",
            no_data=0 if not no_data else no_data,
            nbits=None if not nbits and nbits not in range(1, 8) else nbits,
            compression="DEFLATE",
        )

    elif data_type.lower() == "int":
        return DataType(
            data_type="Byte",
            no_data=None if not no_data else no_data,
            nbits=None if not nbits and nbits not in range(1, 8) else nbits,
            compression="DEFLATE",
        )

    elif data_type.lower() == "uint16":
        return DataType(
            data_type="UInt16",
            no_data=0 if not no_data else no_data,
            nbits=None if not nbits and nbits not in range(9, 16) else nbits,
            compression="DEFLATE",
        )

    elif data_type.lower() == "int16":
        return DataType(
            data_type="Int16",
            no_data=None if not no_data else no_data,
            nbits=None if not nbits and nbits not in range(9, 16) else nbits,
            compression="DEFLATE",
        )

    elif data_type.lower() == "uint32":
        return DataType(
            data_type="UInt32",
            no_data=0 if not no_data else no_data,
            nbits=None if not nbits and nbits not in range(17, 32) else nbits,
            compression="DEFLATE",
        )

    elif data_type.lower() == "int32":
        return DataType(
            data_type="Int32",
            no_data=None if not no_data else no_data,
            nbits=None if not nbits and nbits not in range(17, 32) else nbits,
            compression="DEFLATE",
        )

    elif data_type.lower() == "float16" or data_type.lower() == "half":
        return DataType(
            data_type="Float32",
            no_data=None if not no_data else no_data,
            nbits=16,
            compression="DEFLATE",
        )

    elif data_type.lower() == "float32" or data_type.lower() == "single":
        return DataType(
            data_type="Float32",
            no_data=None if not no_data else no_data,
            nbits=None,
            compression="DEFLATE",
        )

    elif data_type.lower() == "float64" or data_type.lower() == "double":
        return DataType(
            data_type="Float64",
            no_data=None if not no_data else no_data,
            nbits=None,
            compression="DEFLATE",
        )

    else:
        raise Exception("Unknown data type {}".format(data_type))
