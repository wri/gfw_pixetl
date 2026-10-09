import os
from typing import Optional
from urllib.parse import urlparse

from pydantic import Field, validator

from gfw_pixetl import get_module_logger
from gfw_pixetl.settings.models import EnvSettings
from gfw_pixetl.utils.secrets import set_google_application_credentials

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
    vsi_cache: str = (
        "YES"  # file can be cached in RAM.  Content in that cache is discarded when the file handle is closed.
    )
    aws_https: Optional[str] = None
    aws_virtual_hosting: Optional[str] = None
    aws_s3_endpoint: Optional[str] = None  # Populated at call time via get_gdal_env()
    aws_request_payer: str = "requester"
    google_application_credentials: str = Field(
        "/root/.gcs/private_key.json",
        description="Path to Google application credential file",
    )
    cpl_debug: Optional[int] = None
    cpl_curl_verbose: Optional[str] = None

    @validator(
        "google_application_credentials", pre=True, always=True, allow_reuse=True
    )
    def validate_google_application_credentials(cls, v):
        set_google_application_credentials(v)
        return v


# Do not set GDAL_CACHEMAX process-wide. Raster transforms benefit from GDAL's
# normal cache behavior, while vector rasterization and GeoTIFF copy operations
# need bounded caches to prevent concurrent subprocesses from consuming a large
# fraction of host memory. Tile.gdal_cachemax_mb provides per-operation sizing.


def get_gdal_env() -> dict:
    """Return a fresh GDAL environment dict, re-reading AWS_ENDPOINT_URL each
    time.

    Must be a function rather than a module-level constant so that the
    moto test endpoint URL (injected via AWS_ENDPOINT_URL after module
    import) is always picked up.  In production the value is stable so
    calling this on each rasterio.Env / subprocess invocation is cheap.
    """
    return GdalEnv(aws_s3_endpoint=set_aws_s3_endpoint()).env_dict()


# Static configuration may use this snapshot. S3-sensitive paths should call
# get_gdal_env() so late-bound endpoint configuration is respected.
GDAL_ENV = get_gdal_env()
