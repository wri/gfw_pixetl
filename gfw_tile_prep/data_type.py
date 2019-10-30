from typing import Optional

from gfw_tile_prep import get_module_logger

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
