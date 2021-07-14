from rasterio import RasterioIOError

from gfw_pixetl import get_module_logger
from gfw_pixetl.settings.gdal import GDAL_ENV
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.utils.secrets import set_google_application_credentials

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


class MissingGCSKeyError(Exception):
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


def retry_if_missing_gcs_key_error(exception) -> bool:
    is_missing_gcs_key_error: bool = isinstance(exception, MissingGCSKeyError)
    if (
        is_missing_gcs_key_error
        and GDAL_ENV.get("GOOGLE_APPLICATION_CREDENTIALS")
        and GLOBALS.aws_gcs_key_secret_arn
    ):
        set_google_application_credentials(GDAL_ENV["GOOGLE_APPLICATION_CREDENTIALS"])

    elif is_missing_gcs_key_error and (
        not GDAL_ENV["GOOGLE_APPLICATION_CREDENTIALS"]
        or not GLOBALS.aws_gcs_key_secret_arn
    ):
        raise RuntimeError(
            "Both GOOGLE_APPLICATION_CREDENTIALS and GCS_KEY_SECRET_ARN variables must be set"
        )

    return is_missing_gcs_key_error


def _file_does_not_exist(e: Exception) -> bool:
    """Check if RasterIO can access file.

    If file is inaccessible or does not exist, rasterio will always
    raise RasterioIOError. Error messages will differ, depending on the
    access method, if file exists or is inaccessible. However, end
    result should always be the same.
    """

    errors = [
        "does not exist in the file system, and is not recognized as a supported dataset name",
        "The specified key does not exist",
        "No such file or directory",
        "not recognized as a supported file format",
        "Access Denied",
    ]

    return isinstance(e, RasterioIOError) and any(error in str(e) for error in errors)
