import datetime
import multiprocessing
import os
import subprocess as sp
from typing import Any, List

from retrying import retry

from gfw_pixetl import get_module_logger
from gfw_pixetl.errors import (
    GDALError,
    MissingGCSKeyError,
    retry_if_missing_gcs_key_error,
)

LOGGER = get_module_logger(__name__)


@retry(
    retry_on_exception=retry_if_missing_gcs_key_error,
    stop_max_attempt_number=2,
)
def create_vrt(uris: List[str], vrt="all.vrt", tile_list="tiles.txt") -> str:
    """
    ! Important this is not a parallelpipe Stage and must be run with only one worker per vrt file
    Create VRT file from input URI.
    """

    _write_tile_list(tile_list, uris)

    cmd = ["gdalbuildvrt", "-input_file_list", tile_list, vrt]
    # env = set_aws_credentials()
    env = os.environ.copy()

    LOGGER.info(f"Create VRT file {vrt}")
    p: sp.Popen = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, env=env)

    e: Any
    _, e = p.communicate()

    os.remove(tile_list)

    if p.returncode != 0 and "ERROR 3: Load json file" in str(e):
        raise MissingGCSKeyError()
    if p.returncode != 0:
        LOGGER.error("Could not create VRT file")
        LOGGER.exception(e)
        raise GDALError(e)
    else:
        return vrt


def _write_tile_list(tile_list: str, uris: List[str]) -> None:
    with open(tile_list, "w") as input_tiles:
        for uri in uris:
            LOGGER.debug(f"Add {uri} to tile list")
            input_tiles.write(f"{uri}\n")
