from pathlib import Path
from typing import NamedTuple, Optional, Union

from shapely.geometry import Polygon


class InputBandElement(NamedTuple):

    geometry: Optional[Polygon]
    uri: Union[Path, str]
    band: int

    def __str__(self):
        return (
            f"InputElement(uri={self.uri}, band={self.band}, geometry={self.geometry}"
        )
