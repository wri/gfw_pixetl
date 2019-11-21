import os
from typing import Optional

import rasterio
from rasterio.errors import RasterioIOError

from gfw_pixetl import get_module_logger
from gfw_pixetl.sources import RasterSource


logger = get_module_logger(__name__)

if "ENV" in os.environ:
    ENV: str = os.environ["ENV"]
else:
    ENV = "dev"


def get_bucket(env: Optional[str] = ENV) -> str:
    if not env:
        env = "dev"
    bucket = "gfw-data-lake"
    if env != "production":
        bucket += f"-{env}"
    return bucket


def get_src(uri: str) -> RasterSource:
    logger.debug("Check if tile {} exists".format(uri))

    try:
        with rasterio.open(uri) as src:
            logger.info(f"File {uri} exists")
            return RasterSource(uri=uri, profile=src.profile, bounds=src.bounds)

    except Exception as e:

        if _file_does_not_exist(e, uri):
            logger.info(f"File does not exist {uri}")
            raise FileNotFoundError
        else:
            logger.exception(f"Cannot open {uri}")
            raise


def _file_does_not_exist(e: Exception, uri: str) -> bool:
    return isinstance(e, RasterioIOError) and (
        str(e)
        == f"'{uri}' does not exist in the file system, and is not recognized as a supported dataset name."
        or str(e) == "The specified key does not exist."
        or str(e) == f"{uri}: No such file or directory"
    )
