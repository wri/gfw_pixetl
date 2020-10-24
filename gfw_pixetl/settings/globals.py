import multiprocessing
import os
from typing import Optional
from urllib.parse import urlparse

import psutil
import pydantic
from pydantic import BaseSettings, Field


def set_aws_s3_endpoint():
    endpoint = os.environ.get("ENDPOINT_URL", None)
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
    cores: int = multiprocessing.cpu_count()
    max_mem: int = psutil.virtual_memory()[1] / 1000
    db_username: Optional[str] = Field(None, env="PGUSER")
    db_password: Optional[Secret] = Field(None, env="PGPASSWORD")
    db_host: Optional[str] = Field(None, env="PGHOST")
    db_port: Optional[int] = Field(None, env="PGPORT")
    db_name: Optional[str] = Field(None, env="PGDATABASE")
    aws_region: str = "us-east-1"
    job_role_arn: Optional[str] = None
    aws_https: Optional[str] = None
    aws_virtual_hosting: Optional[bool] = None
    gdal_disable_readdir_on_open: Optional[str] = None
    endpoint_url: Optional[str] = None
    aws_batch_job_id: Optional[str] = None
    google_application_credentials: Optional[str] = None
    gcs_key_secret_arn: Optional[str] = None

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
