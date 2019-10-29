from typing import Optional


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
