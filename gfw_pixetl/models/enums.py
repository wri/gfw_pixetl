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
    discrete_intensity = "discrete_intensity"
    gradient = "gradient"
    gradient_intensity = "gradient_intensity"


class DstFormat(str, Enum):
    geotiff = "geotiff"
    gdal_geotiff = "gdal-geotiff"


class PhotometricType(str, Enum):
    minisblack = "MINISBLACK"
    miniswhite = "MINISWHITE"
    rgb = "RGB"
    cmyk = "CMYK"
    ycbcr = "YCBCR"
    cielab = "CIELAB"
    icclab = "ICCLAB"
    itulab = "ITULAB"
