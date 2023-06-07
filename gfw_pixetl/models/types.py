from pathlib import Path
from typing import Tuple, Union

from pydantic import StrictInt
from shapely.geometry import MultiPolygon, Polygon

Bounds = Tuple[float, float, float, float]
ShapePathPair = Tuple[Union[Polygon, MultiPolygon], Path]
NoData = Union[StrictInt, float]
