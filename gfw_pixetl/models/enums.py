from enum import Enum


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


class DstFormat(str, Enum):
    geotiff = "geotiff"
    gdal_geotiff = "gdal-geotiff"
