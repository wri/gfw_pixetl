import os
import shutil
import uuid

from retrying import retry

from gfw_pixetl import get_module_logger
from gfw_pixetl.errors import VolumeNotReadyError, retry_if_volume_not_ready
from gfw_pixetl.settings.globals import GLOBALS

LOGGER = get_module_logger(__name__)


def set_cwd() -> str:
    if GLOBALS.aws_batch_job_id:
        check_volume_ready()
        cwd: str = GLOBALS.aws_batch_job_id
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
