from typing import NamedTuple, Optional

from shapely.geometry import Polygon


class AreaOfUse(NamedTuple):
    """Area Of Use for projections.

    Copied from pyproj.aoi.AreaOfUse version 3.0 PyProj Version 2.6 does
    not expose this class.
    """

    #: West bound of area of use.
    west: float
    #: South bound of area of use.
    south: float
    #: East bound of area of use.
    east: float
    #: North bound of area of use.
    north: float
    #: Name of area of use.
    name: Optional[str] = None

    @property
    def bounds(self):
        return self.west, self.south, self.east, self.north

    def __str__(self):
        return f"- name: {self.name}\n" f"- bounds: {self.bounds}"


class InputBandElement(NamedTuple):

    geometry: Optional[Polygon]
    uri: str
    band: int

    def __str__(self):
        return (
            f"InputElement(uri={self.uri}, band={self.band}, geometry={self.geometry}"
        )
