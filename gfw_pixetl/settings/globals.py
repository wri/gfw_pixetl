from os import cpu_count
from typing import Optional

import psutil
import pydantic
from pydantic import Field, PositiveInt

from gfw_pixetl import get_module_logger
from gfw_pixetl.models.enums import DstFormat
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
    num_processes: PositiveInt = Field(
        1, description="Max number of parallel processes to use"
    )
    max_mem: PositiveInt = Field(
        psutil.virtual_memory()[1] / 1000000,
        description="Max memory available to pixETL",
    )
    divisor: PositiveInt = Field(
        16,
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

    aws_secretsmanager_url: Optional[str] = Field(
        None,
        description="Endpoint URL for AWS Secretsmanager Server (required for Moto)",
    )

    @pydantic.validator("db_password", pre=True, always=True)
    def hide_password(cls, v):
        return Secret(v) or None

    @pydantic.root_validator()
    def set_processes_workers(cls, values):
        cores = values.get("cores", cpu_count())

        num_processes = max(min(cores, values.get("num_processes", cores)), 1)
        workers = max(min(num_processes, values.get("workers", num_processes)), 1)

        values["num_processes"] = num_processes
        values["workers"] = workers

        LOGGER.info(f"Set num_processes to {num_processes}")
        LOGGER.info(f"Set workers to {workers}")

        return values

    @pydantic.validator("max_mem", pre=True, always=True)
    def set_max_mem(cls, v, *, values, **kwargs):
        max_mem = max(min(psutil.virtual_memory()[1] / 1000000, float(v)), 1)
        LOGGER.info(f"Set maximum memory to {max_mem} MB")
        return max_mem


GLOBALS = Globals()
