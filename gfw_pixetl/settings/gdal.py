import os
from typing import Optional
from urllib.parse import urlparse

from gfw_pixetl.settings.models import EnvSettings


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
    aws_https: Optional[str] = None
    aws_virtual_hosting: Optional[str] = None
    gdal_disable_readdir_on_open: Optional[str] = None
    aws_s3_endpoint: Optional[str] = set_aws_s3_endpoint()
    vsi_cache: str = "YES"  # file can be cached in RAM.  Content in that cache is discarded when the file handle is closed.
    gdal_http_max_retry: int = 4
    gdal_http_retry_delay: int = 10
    aws_request_payer: str = "requester"


GDAL_ENV = GdalEnv().env_dict()
