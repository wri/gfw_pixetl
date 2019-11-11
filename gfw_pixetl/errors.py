from gfw_pixetl import get_module_logger

logger = get_module_logger(__name__)


class GDALError(Exception):
    pass


class GDALNoneTypeError(GDALError):
    pass


class GDALAccessDeniedError(GDALError):
    pass


def retry_if_none_type_error(exception):
    """Return True if we should retry (in this case when it's an IOError), False otherwise"""
    logger.warning("GDALNoneType exception - RETRY")
    return isinstance(exception, GDALNoneTypeError)
