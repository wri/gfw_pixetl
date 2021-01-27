import os
from typing import Optional
from urllib.parse import urlparse

import pydantic
from pydantic import Field

from gfw_pixetl import get_module_logger
from gfw_pixetl.settings import GLOBALS
from gfw_pixetl.settings.models import EnvSettings
from gfw_pixetl.utils.aws import get_secret_client

LOGGER = get_module_logger(__name__)


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


class GdalEnv(EnvSettings):
    gdal_tiff_internal_mask = True
    gdal_disable_readdir_on_open: Optional[str] = None
    gdal_http_max_retry: int = 4
    gdal_http_retry_delay: int = 10
    vsi_cache: str = "YES"  # file can be cached in RAM.  Content in that cache is discarded when the file handle is closed.
    aws_https: Optional[str] = None
    aws_virtual_hosting: Optional[str] = None
    aws_s3_endpoint: Optional[str] = set_aws_s3_endpoint()
    aws_request_payer: str = "requester"
    google_application_credentials: str = Field(
        "/root/.gcs/private_key.json",
        description="Path to Google application credential file",
    )

    @pydantic.validator("google_application_credentials", pre=True, always=True)
    def set_google_application_credentials(cls, v):
        if not os.path.isfile(v):
            LOGGER.info("GCS key is missing. Try to fetch key from secret manager")

            client = get_secret_client()
            response = client.get_secret_value(SecretId=GLOBALS.aws_gcs_key_secret_arn)
            os.makedirs(
                os.path.dirname(GDAL_ENV["GOOGLE_APPLICATION_CREDENTIALS"]),
                exist_ok=True,
            )

            LOGGER.info("Write GCS key to file")
            with open(GDAL_ENV["GOOGLE_APPLICATION_CREDENTIALS"], "w") as f:
                f.write(response["SecretString"])

            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GDAL_ENV[
                "GOOGLE_APPLICATION_CREDENTIALS"
            ]
        return v


GDAL_ENV = GdalEnv().env_dict()
