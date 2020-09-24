from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, Field

from gfw_pixetl.data_type import DataTypeEnum
from gfw_pixetl.resampling import ResamplingMethodEnum

VERSION_REGEX = r"^v\d{1,8}\.?\d{,3}\.?\d{,3}$"


class Order(str, Enum):
    asc = "asc"
    desc = "desc"


class RasterizeMethod(str, Enum):
    _count = "count"
    value = "value"


class SourceType(str, Enum):
    raster = "raster"
    vector = "vector"


class LayerModel(BaseModel):
    dataset: str
    version: str = Field(..., regex=VERSION_REGEX)
    source_type: SourceType
    pixel_meaning: str
    data_type: DataTypeEnum
    nbits: Optional[int]
    no_data: Optional[Union[int, float]]
    grid: str  # Make an enum?
    rasterize_method: Optional[RasterizeMethod]
    resampling: ResamplingMethodEnum = ResamplingMethodEnum.nearest
    source_uri: Optional[str]
    calc: Optional[str]
    order: Optional[Order]
