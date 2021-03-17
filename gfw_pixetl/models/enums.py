from enum import Enum


class Order(str, Enum):
    asc = "asc"
    desc = "desc"


class RasterizeMethod(str, Enum):
    _count = "count"
    value = "value"


class DstFormat(str, Enum):
    geotiff = "geotiff"
    gdal_geotiff = "gdal-geotiff"
