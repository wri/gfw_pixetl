from typing import Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field, StrictInt, validator

from gfw_pixetl.data_type import DataTypeEnum
from gfw_pixetl.grids.grid_factory import GridEnum
from gfw_pixetl.models.enums import ColorMapType, Order, RasterizeMethod, SourceType
from gfw_pixetl.resampling import ResamplingMethodEnum

VERSION_REGEX = r"^v\d{1,8}(\.\d{1,3}){0,2}?$|^latest$"


class RGBA(BaseModel):
    red: int = Field(..., ge=0, le=255)
    green: int = Field(..., ge=0, le=255)
    blue: int = Field(..., ge=0, le=255)
    alpha: int = Field(255, ge=0, le=255)

    def tuple(self) -> Tuple[int, int, int, int]:
        return self.red, self.green, self.blue, self.alpha


class Symbology(BaseModel):
    type: ColorMapType
    colormap: Dict[Union[StrictInt, float], RGBA]


class LayerModel(BaseModel):
    dataset: str
    version: str = Field(..., regex=VERSION_REGEX)
    source_type: SourceType
    pixel_meaning: str
    data_type: DataTypeEnum
    nbits: Optional[int]
    no_data: Optional[Union[StrictInt, float]]
    grid: GridEnum
    rasterize_method: Optional[RasterizeMethod]
    resampling: ResamplingMethodEnum = ResamplingMethodEnum.nearest
    calc: Optional[str]
    source_uri: Optional[List[str]]
    order: Optional[Order]
    symbology: Optional[Symbology]
    compute_stats: bool = False
    compute_histogram: bool = False
    process_locally: bool = False

    @validator("source_uri")
    def validate_source_uri(cls, v, values, **kwargs):
        if values["source_type"] == SourceType.raster:
            assert v, "Raster source types require source_uri"
            if len(v) > 1:
                assert values.get("calc"), "More than one source_uri require calc"
        else:
            assert not v, "Only raster source type require source_uri"
        return v


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
