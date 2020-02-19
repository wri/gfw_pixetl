import datetime
import multiprocessing
import os
import re
import shutil
import subprocess as sp
import uuid
from dateutil.tz import tzutc
from typing import Any, Dict, List, Optional

import boto3
import psutil
from retrying import retry

from gfw_pixetl import get_module_logger
from gfw_pixetl.errors import VolumeNotReadyError, retry_if_volume_not_ready, GDALError

LOGGER = get_module_logger(__name__)

TOKEN_EXPIRATION: Optional[datetime.datetime] = None
AWS_ACCESS_KEY_ID: Optional[str] = None
AWS_SECRET_ACCESS_KEY: Optional[str] = None
AWS_SESSION_TOKEN: Optional[str] = None
AVAILABLE_MEMORY: Optional[int] = None
WORKERS: int = 1


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


def set_aws_credentials():
    """
    GDALwrap doesn't seem to be able to handle role permissions.
    Instead it requires presents of credentials in ENV variables or .aws/credential file.
    When run in batch environment, we alter ENV variables for sub process and add AWS credentials.
    """

    # only need to set credentials in AWS Batch environment
    if "AWS_BATCH_JOB_ID" in os.environ.keys():

        global TOKEN_EXPIRATION
        global AWS_ACCESS_KEY_ID
        global AWS_SECRET_ACCESS_KEY
        global AWS_SESSION_TOKEN

        env: Dict[str, Any] = os.environ.copy()
        client = boto3.client("sts")

        if not TOKEN_EXPIRATION or TOKEN_EXPIRATION <= datetime.datetime.now(
            tz=tzutc()
        ):
            LOGGER.debug("Update session token")

            credentials: Dict[str, Any] = client.assume_role(
                RoleArn=os.environ["JOB_ROLE_ARN"], RoleSessionName="pixETL"
            )

            TOKEN_EXPIRATION = credentials["Credentials"]["Expiration"]
            AWS_ACCESS_KEY_ID = credentials["Credentials"]["AccessKeyId"]
            AWS_SECRET_ACCESS_KEY = credentials["Credentials"]["SecretAccessKey"]
            AWS_SESSION_TOKEN = credentials["Credentials"]["SessionToken"]

        LOGGER.debug("Set AWS credentials")
        env["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
        env["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
        env["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN

        LOGGER.debug(f"ENV: {env}")
        return env

    else:
        return os.environ.copy()


def set_cwd() -> str:
    if "AWS_BATCH_JOB_ID" in os.environ.keys():
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
    ! Important this is not a parallelpipe Stage and must be run with only one worker
    Create VRT file from input URI.
    """

    _write_tile_list(tile_list, uris)

    cmd = ["gdalbuildvrt", "-input_file_list", tile_list, vrt]
    env = set_aws_credentials()

    LOGGER.info(f"Create VRT file {vrt}")
    p: sp.Popen = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, env=env)

    o: Any
    e: Any
    o, e = p.communicate()

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
            input_tiles.write(f"{uri}\n")
