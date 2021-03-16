from typing import List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, Field, StrictInt, validator

from gfw_pixetl.data_type import DataTypeEnum
from gfw_pixetl.grids.grid_factory import GridEnum
from gfw_pixetl.models.enums import Order, RasterizeMethod
from gfw_pixetl.resampling import ResamplingMethodEnum

VERSION_REGEX = r"^v\d{1,8}(\.\d{1,3}){0,2}?$|^latest$"


class VectorCalc(BaseModel):
    field: Optional[str]
    where: Optional[str]
    group_by: Optional[str]


class LayerModel(BaseModel):
    dataset: str
    version: str = Field(..., regex=VERSION_REGEX)
    pixel_meaning: str
    data_type: DataTypeEnum
    nbits: Optional[int]
    no_data: Optional[Union[StrictInt, float]]
    grid: GridEnum
    compute_stats: bool = False
    compute_histogram: bool = False
    process_locally: bool = False


class RasterLayerModel(LayerModel):
    source_type: Literal["raster"]
    resampling: ResamplingMethodEnum = ResamplingMethodEnum.nearest
    calc: Optional[str]
    source_uri: List[str]

    @validator("source_uri")
    def validate_source_uri(cls, v, values, **kwargs):
        if len(v) > 1:
            assert values.get("calc"), "More than one source_uri require calc"
        return v


class VectorLayerModel(LayerModel):
    source_type: Literal["vector"]
    rasterize_method: RasterizeMethod
    calc: Optional[VectorCalc]
    order: Order = Order.desc


class Histogram(BaseModel):
    count: int
    min: float
    max: float
    buckets: List[int]


class BandStats(BaseModel):
    min: float
    max: float
    mean: float
    std_dev: float


class Band(BaseModel):
    data_type: DataTypeEnum
    no_data: Optional[Union[StrictInt, float]]
    nbits: Optional[int]
    blockxsize: int
    blockysize: int
    stats: Optional[BandStats] = None
    histogram: Optional[Histogram] = None


class Metadata(BaseModel):
    extent: Tuple[float, float, float, float]
    width: int
    height: int
    pixelxsize: float
    pixelysize: float
    crs: str
    driver: str
    compression: Optional[str]
    bands: List[Band] = list()
