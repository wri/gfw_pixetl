from typing import Any, Dict

import rasterio
from rasterio.coords import BoundingBox
from rasterio.errors import RasterioIOError

from gfw_pixetl import get_module_logger
from gfw_pixetl.utils import get_bucket
from gfw_pixetl.connection import PgConn

LOGGER = get_module_logger(__name__)


class Source(object):
    pass


class VectorSource(Source):
    def __init__(self, table_name):
        self.conn: PgConn = PgConn()
        self.table_name = table_name


class RasterSource(Source):
    def __init__(self, profile: Dict[str, Any], bounds: BoundingBox, uri: str):

        self.profile = profile
        self.bounds = bounds
        self.uri = uri


class Destination(RasterSource):

    bucket = get_bucket()

    def get_prefix(self):
        return "/".join(self.uri.split("/")[:-1])

    def get_filename(self):
        return self.uri.split("/")[-1:]


def get_src(uri: str) -> RasterSource:
    LOGGER.debug("Check if tile {} exists".format(uri))

    try:
        with rasterio.open(uri) as src:
            LOGGER.info(f"File {uri} exists")
            return RasterSource(uri=uri, profile=src.profile, bounds=src.bounds)

    except Exception as e:

        if _file_does_not_exist(e, uri):
            LOGGER.info(f"File does not exist {uri}")
            raise FileNotFoundError(f"File does not exist: {uri}")
        else:
            LOGGER.exception(f"Cannot open {uri}")
            raise


def _file_does_not_exist(e: Exception, uri: str) -> bool:
    return isinstance(e, RasterioIOError) and (
        str(e)
        == f"'{uri}' does not exist in the file system, and is not recognized as a supported dataset name."
        or str(e) == "The specified key does not exist."
        or str(e) == f"{uri}: No such file or directory"
    )
