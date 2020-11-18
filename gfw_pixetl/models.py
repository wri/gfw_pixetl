from enum import Enum
from typing import List, Optional, Tuple, Union

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


class Histogram(BaseModel):
    count: int
    min: float
    max: float
    buckets: List[int]


class BandStats(BaseModel):
    data_type: DataTypeEnum
    min: float
    max: float
    mean: float
    std_dev: float
    no_data: float
    histogram: Histogram
    nbits: Optional[int]
    blockxsize: int
    blockysize: int


class Stats(BaseModel):
    extent: Tuple[float, float, float, float]
    width: int
    height: int
    pixelxsize: float
    pixelysize: float
    crs: str
    driver: str
    compression: str
    bands: List[BandStats] = list()
