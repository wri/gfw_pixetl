from enum import StrEnum


class Order(StrEnum):
    asc = "asc"
    desc = "desc"


class RasterizeMethod(StrEnum):
    _count = "count"
    value = "value"


class SourceType(StrEnum):
    raster = "raster"
    vector = "vector"


class ColorMapType(StrEnum):
    discrete = "discrete"
    discrete_intensity = "discrete_intensity"
    gradient = "gradient"
    gradient_intensity = "gradient_intensity"


class DstFormat(StrEnum):
    geotiff = "geotiff"
    gdal_geotiff = "gdal-geotiff"


class PhotometricType(StrEnum):
    minisblack = "MINISBLACK"
    miniswhite = "MINISWHITE"
    rgb = "RGB"
    cmyk = "CMYK"
    ycbcr = "YCBCR"
    cielab = "CIELAB"
    icclab = "ICCLAB"
    itulab = "ITULAB"
