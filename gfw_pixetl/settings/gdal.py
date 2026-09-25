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
    gdal_cachemax: str = Field(
        "512",
        description="GDAL's per-process raster block cache limit, in MB "
        "(GDAL also accepts a percentage, e.g. '5%'). Explicitly fixed here "
        "because GDAL's own default, when this is left unset, is 5% of the "
        "*host's* physical RAM -- not the cgroup memory limit -- recomputed "
        "independently by every new process that touches GDAL: every "
        "gdal_rasterize subprocess call, and every spawned geotiff-copy "
        "process (see tile.py's _copy_geotiff_spawned). On a 371GiB host "
        "that default is ~18.5GiB of *permitted* cache per process, and it "
        "grows every time the instance is resized up -- the opposite of "
        "what VectorPipe._rasterize_reservation_bytes() assumes when it "
        "reserves a roughly fixed amount per tile from the layer's grid and "
        "dtype. With many tiles concurrently in flight, several processes "
        "independently approaching a many-GiB cache ceiling at once is a "
        "very plausible source of the ~2.1x real-vs-modeled overhead seen "
        "on the 10/100000 grid, and would also mean part of a bigger "
        "instance's extra headroom goes into bigger per-process caches "
        "instead of more concurrent tiles. A small, fixed value keeps GDAL's "
        "own memory use predictable and decoupled from host size.",
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
