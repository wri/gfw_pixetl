from enum import Enum
from typing import Optional

from pydantic import BaseModel

# from .resampling import ResamplingMethod


class Order(str, Enum):
    asc = "asc"
    desc = "desc"


class RasterizeMethod(str, Enum):
    count = "count"
    value = "value"


class SourceType(str, Enum):
    raster = "raster"
    vector = "vector"


class LayerModel(BaseModel):
    dataset: str
    version: str
    source_type: SourceType
    pixel_meaning: str
    data_type: str  # Make an enum from dict in data_type.py
    nbits: Optional[int]
    no_data: Optional[int]
    grid: str  # Make an enum?
    rasterize_method: Optional[RasterizeMethod]
    # resampling: Optional[ResamplingMethod] = "nearest"
    resampling: str = "nearest"
    uri: Optional[str]
    calc: Optional[str]
    order: Optional[Order]
