import multiprocessing
from typing import Optional

import psutil
import pydantic
from pydantic import Field, PositiveInt

from gfw_pixetl import get_module_logger
from gfw_pixetl.models import DstFormat
from gfw_pixetl.settings.models import EnvSettings

LOGGER = get_module_logger(__name__)


class Secret:
    """Holds a string value that should not be revealed in tracebacks etc.

    You should cast the value to `str` at the point it is required.
    """

    def __init__(self, value: str):
        self._value = value

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}('**********')"

    def __str__(self) -> str:
        return self._value


class Globals(EnvSettings):

    #####################
    # General
    #####################

    default_dst_format = DstFormat.geotiff

    #####################
    # Resource management
    ######################
    cores: PositiveInt = Field(
        multiprocessing.cpu_count(), description="Max number of cores to use"
    )
    max_mem: PositiveInt = Field(
        psutil.virtual_memory()[1] / 1000000,
        description="Max memory available to pixETL",
    )
    divisor: PositiveInt = Field(
        4,
        description="Fraction of memory per worker to use to compute maximum block size."
        "(ie 4 => size =  25% of available memory)",
    )
    workers: PositiveInt = Field(
        1, description="Number of workers to use to execute job."
    )

    ########################
    # PostgreSQL authentication
    ########################
    db_username: Optional[str] = Field(
        None, env="PGUSER", description="PostgreSQL user name"
    )
    db_password: Optional[Secret] = Field(
        None, env="PGPASSWORD", description="PostgreSQL password"
    )
    db_host: Optional[str] = Field(None, env="PGHOST", description="PostgreSQL host")
    db_port: Optional[int] = Field(None, env="PGPORT", description="PostgreSQL port")
    db_name: Optional[str] = Field(
        None, env="PGDATABASE", description="PostgreSQL database name"
    )

    #######################
    # Google authentication
    #######################
    google_application_credentials: str = Field(
        "/root/.gcs/private_key.json",
        description="Path to Google application credential file",
    )

    ######################
    # AWS configuration
    ######################
    aws_region: str = Field("us-east-1", description="AWS region")
    aws_batch_job_id: Optional[str] = Field(None, description="AWS Batch job ID")
    aws_job_role_arn: Optional[str] = Field(
        None,
        description="ARN of the AWS IAM role which runs the batch job on docker host",
    )
    aws_gcs_key_secret_arn: Optional[str] = Field(
        None, description="ARN of AWS Secret which holds GCS key"
    )

    aws_endpoint_url: Optional[str] = Field(
        None, description="Endpoint URL for AWS S3 Server (required for Moto)"
    )

    @pydantic.validator("db_password", pre=True, always=True)
    def hide_password(cls, v):
        return Secret(v) or None

    @pydantic.validator("workers", pre=True, always=True)
    def set_workers(cls, v, *, values, **kwargs):
        workers = max(min(values["cores"], v), 1)
        LOGGER.info(f"Set workers to {workers}")
        return workers


GLOBALS = Globals()
