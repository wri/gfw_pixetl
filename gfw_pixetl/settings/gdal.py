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


# GDAL_CACHEMAX deliberately does NOT live here, or anywhere else applied
# process-wide. It was, briefly (os.environ.setdefault("GDAL_CACHEMAX",
# "512")) -- fixed one real problem (vector rasterize's write-once workload
# had no business letting every concurrent gdal_rasterize/geotiff-copy
# process independently claim GDAL's default 5%-of-host-RAM cache, which is
# what caused the OOM this was built to prevent) but broke another: raster
# transform's windowed, often-overlapping reads are a genuinely
# cache-friendly access pattern, and capping every GDAL operation in the
# whole process to 512MB regardless of which pipeline was running slowed
# that down instead.
#
# Per-tile-type sizing lives on Tile.gdal_cachemax_mb instead (None by
# default = GDAL's own behavior, which is what raster transform always
# had and is fine for it; VectorSrcTile overrides it to a small fixed
# value), applied at the specific call sites that need it --
# just_copy_geotiff() (the shared geotiff-copy step both pipelines use)
# and VectorSrcTile.rasterize()'s own gdal_rasterize subprocess call --
# rather than as a single global default for every GDAL operation in the
# process.


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
