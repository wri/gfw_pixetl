from typing import NamedTuple, Optional

from shapely.geometry import Polygon


class InputBandElement(NamedTuple):

    geometry: Optional[Polygon]
    uri: str
    band: int

    def __str__(self):
        return (
            f"InputElement(uri={self.uri}, band={self.band}, geometry={self.geometry}"
        )
