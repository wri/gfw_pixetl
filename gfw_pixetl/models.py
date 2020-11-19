from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field, conint

from gfw_pixetl.data_type import DataTypeEnum
from gfw_pixetl.resampling import ResamplingMethodEnum

VERSION_REGEX = r"^v\d{1,8}\.?\d{,3}\.?\d{,3}$"

Bounds = Tuple[float, float, float, float]


class Order(str, Enum):
    asc = "asc"
    desc = "desc"


class RasterizeMethod(str, Enum):
    _count = "count"
    value = "value"


class SourceType(str, Enum):
    raster = "raster"
    vector = "vector"


class ColorMapType(str, Enum):
    discrete = "discrete"
    gradient = "gradient"


class RGBA(BaseModel):
    red: int = Field(..., ge=0, le=255)
    green: int = Field(..., ge=0, le=255)
    blue: int = Field(..., ge=0, le=255)
    alpha: int = Field(255, ge=0, le=255)


class ColorMap(BaseModel):
    type: ColorMapType
    colormap: Dict[Union[int, float], RGBA]


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
    symbology: Optional[ColorMap]


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
    no_data: Optional[Union[int, float]]
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
