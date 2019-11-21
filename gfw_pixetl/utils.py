import rasterio
from rasterio.errors import RasterioIOError

from gfw_pixetl import get_module_logger
from gfw_pixetl.sources import RasterSource


logger = get_module_logger(__name__)


def get_src(uri: str) -> RasterSource:
    logger.debug("Check if tile {} exists".format(uri))

    try:
        with rasterio.open(uri) as src:
            logger.info(f"File {uri} exists")
            return RasterSource(uri=uri, profile=src.profile, bounds=src.bounds)

    except Exception as e:

        if _file_does_not_exist(e, uri):
            logger.info(f"File does not exist {uri}")
            raise FileExistsError
        else:
            logger.exception(f"Cannot open {uri}")
            raise


def _file_does_not_exist(e: Exception, uri: str) -> bool:
    return isinstance(e, RasterioIOError) and (
        str(e)
        == f"'{uri}' does not exist in the file system, and is not recognized as a supported dataset name."
        or str(e) == "The specified key does not exist."
    )
