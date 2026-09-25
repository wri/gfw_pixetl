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
    vsi_cache: str = "YES"  # file can be cached in RAM.  Content in that cache is discarded when the file handle is closed.
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


# GDAL_CACHEMAX deliberately does NOT live on GdalEnv above, even though it's
# conceptually the same kind of setting. GdalEnv.env_dict() stringifies every
# field so the result can be merged into a subprocess's OS environment (which
# requires str values) -- but rasterio.Env(**kwargs) special-cases
# GDAL_CACHEMAX internally and requires a real Python int there, not a
# string. Passing the stringified "512" through rasterio.Env(GDAL_CACHEMAX=
# "512") raises "TypeError: an integer is required" in rasterio's Cython
# _env.pyx, before a single raster file is touched -- exactly what broke
# fetch_metadata()'s rasterio.Env(**get_gdal_env()) call on the very first
# tile of the very first stage.
#
# Setting it as a real OS environment variable instead sidesteps the type
# mismatch entirely: GDAL's C layer reads GDAL_CACHEMAX from the process
# environment as a fallback whenever it isn't explicitly passed as a config
# option, so this still reaches both run_gdal_subcommand()'s gdal_rasterize
# subprocess (which inherits it via os.environ.copy()) and in-process
# rasterio/GDAL calls (just_copy_geotiff, fetch_metadata) -- without ever
# handing the value to rasterio.Env()'s kwargs, where the crash happened.
# setdefault() so a value the deployment's own environment already sets
# takes precedence over this default.
os.environ.setdefault("GDAL_CACHEMAX", "512")


def get_gdal_env() -> dict:
    """Return a fresh GDAL environment dict, re-reading AWS_ENDPOINT_URL each
    time.

    Must be a function rather than a module-level constant so that the
    moto test endpoint URL (injected via AWS_ENDPOINT_URL after module
    import) is always picked up.  In production the value is stable so
    calling this on each rasterio.Env / subprocess invocation is cheap.
    """
    return GdalEnv(aws_s3_endpoint=set_aws_s3_endpoint()).env_dict()


# Backwards-compatible module-level constant for code paths that import GDAL_ENV
# directly and are not sensitive to late-bound environment changes (e.g. static
# config reads at startup).  S3-sensitive paths (fetch_metadata, run_gdal_subcommand)
# should call get_gdal_env() instead.
GDAL_ENV = get_gdal_env()
