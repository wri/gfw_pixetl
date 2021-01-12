from typing import Any, Dict, Optional, Sequence, Tuple, Union

from shapely.geometry import MultiPolygon, Polygon

Bounds = Tuple[float, float, float, float]
OrderedColorMap = Dict[Union[int, float], Tuple[int, int, int, int]]
FeatureTuple = Sequence[Tuple[Union[Polygon, MultiPolygon], Optional[Dict[str, Any]]]]
