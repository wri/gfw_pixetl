import os
from typing import Any, Dict

from rasterio.coords import BoundingBox

from gfw_pixetl import get_module_logger
from gfw_pixetl import utils
from gfw_pixetl.connection import PgConn

logger = get_module_logger(__name__)


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

    bucket = utils.get_bucket()

    def get_prefix(self):
        return "/".join(self.uri.split("/")[:-1])

    def get_filename(self):
        return self.uri.split("/")[-1:]
