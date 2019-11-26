import os
import re
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


def verify_version_pattern(version: str) -> None:
    """
    Verify if version matches general pattern
    - Must start with a v
    - Followed by up to three groups of digits seperated with a .
    - First group can have up to 8 digits
    - Second and third group up to 3 digits

    Examples:
    - v20191001
    - v1.1.2
    """

    if not version:
        message = "No version number provided"
        logger.error(message)
        raise ValueError(message)

    p = re.compile(r"^v\d{,8}\.?\d{,3}\.?\d{,3}$")
    m = p.match(version)
    if not m:
        message = "Version number does not match pattern"
        logger.error(message)
        raise ValueError(message)


def _file_does_not_exist(e: Exception, uri: str) -> bool:
    return isinstance(e, RasterioIOError) and (
        str(e)
        == f"'{uri}' does not exist in the file system, and is not recognized as a supported dataset name."
        or str(e) == "The specified key does not exist."
        or str(e) == f"{uri}: No such file or directory"
    )
