import datetime
import multiprocessing
import os
import re
import shutil
import uuid
from typing import Optional, Tuple
from urllib.parse import urlparse

import psutil
from pyproj import CRS, Transformer
from rasterio.windows import Window
from retrying import retry

from gfw_pixetl import get_module_logger
from gfw_pixetl.errors import VolumeNotReadyError, retry_if_volume_not_ready

LOGGER = get_module_logger(__name__)

TOKEN_EXPIRATION: Optional[datetime.datetime] = None
AWS_ACCESS_KEY_ID: Optional[str] = None
AWS_SECRET_ACCESS_KEY: Optional[str] = None
AWS_SESSION_TOKEN: Optional[str] = None
AVAILABLE_MEMORY: Optional[int] = None
WORKERS: int = 1

Bounds = Tuple[float, float, float, float]


def get_bucket(env: Optional[str] = None) -> str:
    """compose bucket name based on environment."""

    if not env and "ENV" in os.environ:
        env = os.environ["ENV"]
    else:
        env = "dev"

    bucket = "gfw-data-lake"
    if env != "production":
        bucket += f"-{env}"
    return bucket


def verify_version_pattern(version: str) -> bool:
    """Verify if version matches general pattern.

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
    """This check assures we make use of the ephemeral volume of the AWS
    compute environment.

    We only perform this check if we use this module in AWS Batch
    compute environment (AWS_BATCH_JOB_ID is present) The READY file is
    created during bootstrap process after formatting and mounting
    ephemeral volume
    """
    if not os.path.exists("READY") and "AWS_BATCH_JOB_ID" in os.environ.keys():
        raise VolumeNotReadyError("Mounted Volume not ready")
    return True


def set_workers(workers: int) -> int:
    """Set environment variable with number of workers Cannot exceed number of
    cores and must be at least one."""
    global WORKERS
    WORKERS = max(min(multiprocessing.cpu_count(), workers), 1)
    LOGGER.info(f"Set workers to {WORKERS}")
    return WORKERS


def get_workers() -> int:
    """Return number of workers for parallel jobs."""
    return WORKERS


def set_available_memory() -> int:
    global AVAILABLE_MEMORY
    if not AVAILABLE_MEMORY:
        AVAILABLE_MEMORY = psutil.virtual_memory()[1]
        LOGGER.info(f"Total available memory set to {AVAILABLE_MEMORY}")
    return AVAILABLE_MEMORY  # type: ignore


def available_memory_per_process() -> float:
    """Snapshot of currently available memory per core or process."""
    return set_available_memory() / get_workers()


def replace_inf_nan(number: float, replacement: float) -> float:
    if number == float("inf") or number == float("nan"):
        LOGGER.debug("Replace number")
        return replacement
    else:
        return number


def snapped_window(window):
    """Make sure window is snapped to grid and contains full pixels to avoid
    missing rows and columns."""
    col_off, row_off, width, height = window.flatten()

    return Window(
        col_off=round(col_off),
        row_off=round(row_off),
        width=round(width),
        height=round(height),
    )


def world_bounds(crs: CRS) -> Bounds:
    """Get world bounds got given CRT."""

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
