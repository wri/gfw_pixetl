import multiprocessing
import os
from typing import Optional
from urllib.parse import urlparse

import psutil
import pydantic
from pydantic import BaseSettings, Field, PositiveInt


def set_aws_s3_endpoint():
    endpoint = os.environ.get("AWS_ENDPOINT_URL", None)
    if endpoint:
        o = urlparse(endpoint, allow_fragments=False)
        if o.scheme and o.netloc:
            result: Optional[str] = o.netloc
        else:
            result = o.path
        os.environ["AWS_S3_ENDPOINT"] = result
    else:
        result = None

    return result


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


class EnvSettings(BaseSettings):
    def env_dict(self):
        env = self.dict(exclude_none=True, exclude_unset=True)
        return {key.upper(): value for key, value in env.items()}

    class Config:
        case_sensitive = False


class Settings(EnvSettings):
    #####################
    # Resource management
    ######################
    cores: PositiveInt = Field(
        multiprocessing.cpu_count(), description="Max number of cores to use"
    )
    max_mem: PositiveInt = Field(
        psutil.virtual_memory()[1] / 1000, description="Max memory available to pixETL"
    )
    divisor: PositiveInt = Field(
        4,
        description="Fraction of memory per worker to use to compute maximum block size."
        "(ie 4 => size =  25% of available memory)",
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
    google_application_credentials: Optional[str] = Field(
        None, description="Path to Google application credential file"
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
    aws_https: Optional[str] = Field(
        None, description="Use HTTPS to connect to AWS (required for Moto)"
    )
    aws_virtual_hosting: Optional[bool] = Field(
        None, description="Use AWS Virtutal hosting (required for Moto)"
    )
    aws_endpoint_url: Optional[str] = Field(
        None, description="Endpoint URL for AWS S3 Server (required for Moto)"
    )

    #######################
    # GDAL configuration
    #######################
    gdal_disable_readdir_on_open: Optional[str] = Field(
        None, description="Disable read dir on open for GDAL (required for Moto)"
    )

    @pydantic.validator("db_password", pre=True, always=True)
    def hide_password(cls, v):
        return Secret(v) or None


class GdalEnv(EnvSettings):
    aws_https: Optional[str] = None
    aws_virtual_hosting: Optional[str] = None
    gdal_disable_readdir_on_open: Optional[str] = None
    aws_s3_endpoint: Optional[str] = set_aws_s3_endpoint()


SETTINGS = Settings()
GDAL_ENV = GdalEnv().env_dict()
