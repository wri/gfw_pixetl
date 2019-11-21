import rasterio
from rasterio.errors import RasterioIOError

from gfw_pixetl import get_module_logger
from gfw_pixetl.source import RasterSource

logger = get_module_logger(__name__)


def get_src(uri: str) -> RasterSource:
    logger.debug("Check if tile {} exists".format(uri))

    try:
        with rasterio.open(uri) as src:
            source: RasterSource = RasterSource(
                uri=uri, profile=src.profile, bounds=src.bounds
            )
    except Exception as e:

        if isinstance(e, RasterioIOError) and (
            str(e)
            == f"'{uri}' does not exist in the file system, and is not recognized as a supported dataset name."
            or str(e) == "The specified key does not exist."
        ):
            logger.info(f"File does not exist {uri}")
            raise FileExistsError
        else:
            logger.exception(f"Cannot open {uri}")
            raise
    else:
        logger.info(f"File {uri} exists")
        return source
