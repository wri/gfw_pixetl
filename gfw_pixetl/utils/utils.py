import datetime
import multiprocessing
import os
import re
import shutil
import subprocess as sp
import uuid

from typing import Any, List, Optional, Tuple
from urllib.parse import urlparse

import psutil
from pyproj import CRS, Transformer
from rasterio.windows import Window
from retrying import retry

from gfw_pixetl import get_module_logger
from gfw_pixetl.errors import (
    VolumeNotReadyError,
    retry_if_volume_not_ready,
    GDALError,
    ValueConversionError,
)

LOGGER = get_module_logger(__name__)

TOKEN_EXPIRATION: Optional[datetime.datetime] = None
AWS_ACCESS_KEY_ID: Optional[str] = None
AWS_SECRET_ACCESS_KEY: Optional[str] = None
AWS_SESSION_TOKEN: Optional[str] = None
AVAILABLE_MEMORY: Optional[int] = None
WORKERS: int = 1

Bounds = Tuple[float, float, float, float]


class Secret:
    """
    Holds a string value that should not be revealed in tracebacks etc.
    You should cast the value to `str` at the point it is required.
    """

    def __init__(self, value: str):
        self._value = value

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}('**********')"

    def __str__(self) -> str:
        return self._value


def get_bucket(env: Optional[str] = None) -> str:
    """
    compose bucket name based on environment
    """

    if not env and "ENV" in os.environ:
        env = os.environ["ENV"]
    else:
        env = "dev"

    bucket = "gfw-data-lake"
    if env != "production":
        bucket += f"-{env}"
    return bucket


def verify_version_pattern(version: str) -> bool:
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
        LOGGER.error(message)
        raise ValueError(message)

    p = re.compile(r"^v\d{,8}\.?\d{,3}\.?\d{,3}$")
    m = p.match(version)

    if not m:
        return False
    else:
        return True


# def set_aws_credentials():
#     """
#     GDALwrap doesn't seem to be able to handle role permissions.
#     Instead it requires presents of credentials in ENV variables or .aws/credential file.
#     When run in batch environment, we alter ENV variables for sub process and add AWS credentials.
#     AWS_S3_ENDPOINT can be set to route requests to a different server, for example for tests
#     """

#
# # only need to set credentials in AWS Batch environment
# if "AWS_BATCH_JOB_ID" in os.environ.keys():
#
#     global TOKEN_EXPIRATION
#     global AWS_ACCESS_KEY_ID
#     global AWS_SECRET_ACCESS_KEY
#     global AWS_SESSION_TOKEN
#
#     env: Dict[str, Any] = os.environ.copy()
#     sts_client = get_sts_client()
#
#     if not TOKEN_EXPIRATION or TOKEN_EXPIRATION <= datetime.datetime.now(
#         tz=tzutc()
#     ):
#         LOGGER.debug("Update session token")
#
#         credentials: Dict[str, Any] = sts_client.assume_role(
#             RoleArn=JOB_ROLE_ARN, RoleSessionName="pixETL"
#         )
#
#         TOKEN_EXPIRATION = credentials["Credentials"]["Expiration"]
#         AWS_ACCESS_KEY_ID = credentials["Credentials"]["AccessKeyId"]
#         AWS_SECRET_ACCESS_KEY = credentials["Credentials"]["SecretAccessKey"]
#         AWS_SESSION_TOKEN = credentials["Credentials"]["SessionToken"]
#
#     LOGGER.debug("Set AWS credentials")
#     env["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
#     env["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
#     env["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN
#     env["AWS_S3_ENDPOINT"] = AWS_S3_ENDPOINT
#
#     LOGGER.debug(f"ENV: {env}")
#     return env
#
# else:
#     return os.environ.copy()


def get_aws_s3_endpoint(endpoint: Optional[str]) -> Optional[str]:
    """check if AWS_S3_ENDPOINT or ENDPOINT_URL is set and remove protocol from endpoint if present"""

    if endpoint:
        o = urlparse(endpoint, allow_fragments=False)
        if o.scheme and o.netloc:
            result: Optional[str] = o.netloc
        else:
            result = o.path
    else:
        result = None

    return result


def to_bool(value: Optional[str]) -> Optional[bool]:
    boolean = {
        "false": False,
        "true": True,
        "no": False,
        "yes": True,
        "0": False,
        "1": True,
    }
    if value is None:
        response = None
    else:
        try:
            response = boolean[value.lower()]
        except KeyError:
            raise ValueConversionError(f"Cannot convert value {value} to boolean")

    return response


def set_cwd() -> str:
    if "AWS_BATCH_JOB_ID" in os.environ.keys():
        check_volume_ready()
        cwd: str = os.environ["AWS_BATCH_JOB_ID"]
    else:
        cwd = str(uuid.uuid4())

    if os.path.exists(cwd):
        shutil.rmtree(cwd)
    os.mkdir(cwd)
    os.chdir(cwd)
    LOGGER.info(f"Current Work Directory set to {os.getcwd()}")
    return cwd


def remove_work_directory(old_cwd, cwd) -> None:
    os.chdir(old_cwd)
    if os.path.exists(cwd):
        LOGGER.info("Delete temporary work directory")
        shutil.rmtree(cwd)


@retry(
    retry_on_exception=retry_if_volume_not_ready,
    stop_max_attempt_number=7,
    wait_fixed=2000,
)
def check_volume_ready() -> bool:
    """
    This check assures we make use of the ephemeral volume of the AWS compute environment.
    We only perform this check if we use this module in AWS Batch compute environment (AWS_BATCH_JOB_ID is present)
    The READY file is created during bootstrap process after formatting and mounting ephemeral volume
    """
    if not os.path.exists("READY") and "AWS_BATCH_JOB_ID" in os.environ.keys():
        raise VolumeNotReadyError("Mounted Volume not ready")
    return True


def set_workers(workers: int) -> int:
    """
    Set environment variable with number of workers
    Cannot exceed number of cores and must be at least one
    """
    global WORKERS
    WORKERS = max(min(multiprocessing.cpu_count(), workers), 1)
    LOGGER.info(f"Set workers to {WORKERS}")
    return WORKERS


def get_workers() -> int:
    """
    Return number of workers for parallel jobs
    """
    return WORKERS


def set_available_memory() -> int:
    global AVAILABLE_MEMORY
    if not AVAILABLE_MEMORY:
        AVAILABLE_MEMORY = psutil.virtual_memory()[1]
        LOGGER.info(f"Total available memory set to {AVAILABLE_MEMORY}")
    return AVAILABLE_MEMORY  # type: ignore


def available_memory_per_process() -> float:
    """
    Snapshot of currently available memory per core or process
    """
    return set_available_memory() / get_workers()


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


def replace_inf_nan(number: float, replacement: float) -> float:
    if number == float("inf") or number == float("nan"):
        LOGGER.debug("Replace number")
        return replacement
    else:
        return number


def snapped_window(window):
    """
        Make sure window is snapped to grid and contains full pixels to avoid missing rows and columns
        """
    col_off, row_off, width, height = window.flatten()

    return Window(
        col_off=round(col_off),
        row_off=round(row_off),
        width=round(width),
        height=round(height),
    )


def world_bounds(crs: CRS) -> Bounds:
    """
        Get world bounds got given CRT
        """

    from_crs = CRS(4326)

    proj = Transformer.from_crs(from_crs, crs, always_xy=True)

    _left, _bottom, _right, _top = crs.area_of_use.bounds

    # Get World Extent in Source Projection
    # Important: We have to get each top, left, right, bottom separately.
    # We cannot get them using the corner coordinates.
    # For some projections such as Goode (epsg:54052) this would cause strange behavior
    top = proj.transform(0, _top)[1]
    left = proj.transform(_left, 0)[0]
    bottom = proj.transform(0, _bottom)[1]
    right = proj.transform(_right, 0)[0]

    LOGGER.debug(f"World Extent of CRS {crs}: {left}, {bottom}, {right}, {top}")

    return left, bottom, right, top
