from rasterio import RasterioIOError

from gfw_pixetl import get_module_logger

LOGGER = get_module_logger(__name__)


class GDALError(Exception):
    pass


class GDALNoneTypeError(GDALError):
    pass


class GDALAccessDeniedError(GDALError):
    pass


class GDALAWSConfigError(GDALError):
    pass


class VolumeNotReadyError(Exception):
    pass


class ValueConversionError(Exception):
    pass


def retry_if_none_type_error(exception) -> bool:
    """Return True if we should retry (in this case when it's an IOError),
    False otherwise."""
    is_none_type_error: bool = isinstance(exception, GDALNoneTypeError)
    if is_none_type_error:
        LOGGER.warning("GDALNoneType exception - RETRY")
    return is_none_type_error


def retry_if_volume_not_ready(exception) -> bool:
    """Return True if we should retry (in this case when Volume not yet ready),
    False otherwise."""
    is_not_ready: bool = isinstance(exception, VolumeNotReadyError)
    if is_not_ready:
        LOGGER.warning("Volume not ready - RETRY")
    return is_not_ready


def retry_if_rasterio_error(exception) -> bool:
    is_rasterio_error: bool = isinstance(exception, RasterioIOError)
    if is_rasterio_error:
        LOGGER.warning("RasterioIO Error - RETRY")
    return is_rasterio_error


def retry_if_rasterio_io_error(exception) -> bool:
    is_rasterio_io_error: bool = isinstance(
        exception, RasterioIOError
    ) and "IReadBlock failed" in str(exception)
    if is_rasterio_io_error:
        LOGGER.warning("RasterioIO Error - RETRY")
    return is_rasterio_io_error


#
# def retry_if_not_recognized(exception) -> bool:
#     if_not_recognized: bool = isinstance(
#         exception, RasterioIOError
#     ) and ("not recognized as a supported file format" in str(exception) or "Please try again" in str(exception))
#     if if_not_recognized:
#         LOGGER.warning("RasterioIO Error - RETRY")
#     return if_not_recognized
