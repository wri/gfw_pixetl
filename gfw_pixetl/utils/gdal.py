import json
import os
import subprocess as sp
from typing import Any, Dict, List, Optional, Tuple

from retrying import retry

from gfw_pixetl import get_module_logger
from gfw_pixetl.data_type import from_gdal_data_type
from gfw_pixetl.errors import (
    GDALAWSConfigError,
    GDALError,
    GDALNoneTypeError,
    MissingGCSKeyError,
    retry_if_missing_gcs_key_error,
    retry_if_none_type_error,
)
from gfw_pixetl.models.pydantic import Band, BandStats, Histogram, Metadata
from gfw_pixetl.models.types import Bounds
from gfw_pixetl.settings import GDAL_ENV

LOGGER = get_module_logger(__name__)


@retry(
    retry_on_exception=retry_if_missing_gcs_key_error,
    stop_max_attempt_number=2,
)
def create_vrt(
    uris: List[str], extent: Optional[Bounds] = None, vrt: str = "all.vrt"
) -> str:
    """
    ! Important this is not a parallelpipe Stage and must be run with only one worker per vrt file
    Create VRT file from input URI.
    """

    cmd = ["gdalbuildvrt"]
    if extent:
        cmd += ["-te"] + [str(v) for v in extent]
    cmd += [vrt, *uris]

    try:
        run_gdal_subcommand(cmd)
    except GDALError:
        LOGGER.error("Could not create VRT file")
        raise

    return vrt


@retry(
    retry_on_exception=retry_if_none_type_error,
    stop_max_attempt_number=7,
    wait_fixed=2000,
)
def run_gdal_subcommand(
    cmd: List[str], env: Optional[Dict] = GDAL_ENV
) -> Tuple[str, str]:
    """Run GDAL as sub command and catch common errors."""

    gdal_env = os.environ.copy()
    if env:
        gdal_env.update(**env)

    LOGGER.debug(f"RUN subcommand {cmd}, using env {gdal_env}")
    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, env=gdal_env)

    o_byte, e_byte = p.communicate()

    # somehow return type when running `gdalbuildvrt` is str but otherwise bytes
    try:
        o = o_byte.decode("utf-8")
        e = e_byte.decode("utf-8")
    except AttributeError:
        o = str(o_byte)
        e = str(e_byte)

    if p.returncode != 0:
        if not e:
            raise GDALNoneTypeError(e)
        elif (
            e
            == b"ERROR 15: AWS_SECRET_ACCESS_KEY and AWS_NO_SIGN_REQUEST configuration options not defined, and /root/.aws/credentials not filled\n"
        ):
            raise GDALAWSConfigError(e)
        elif "ERROR 3: Load json file" in e or "GOOGLE_APPLICATION_CREDENTIALS" in e:
            raise MissingGCSKeyError(e)
        else:
            raise GDALError(e)

    return o, e


def get_metadata(
    uri: str, compute_stats: bool = False, compute_histogram: bool = False
) -> Metadata:
    """Compute statistics and histogram using gdalinfo.

    Parse statistics as Stats object
    """

    cmd: List[str] = ["gdalinfo", "-json"]

    if compute_stats:
        cmd += ["-stats", "-mm"]

    if compute_histogram:
        cmd += ["-hist"]

    cmd += [uri]

    o, e = run_gdal_subcommand(cmd)

    meta: Dict[str, Any] = json.loads(o)
    metadata = Metadata(
        extent=(
            meta["cornerCoordinates"]["lowerLeft"][0],
            meta["cornerCoordinates"]["lowerLeft"][1],
            meta["cornerCoordinates"]["upperRight"][0],
            meta["cornerCoordinates"]["upperRight"][1],
        ),
        width=meta["size"][0],
        height=meta["size"][1],
        pixelxsize=abs(meta["geoTransform"][0]),
        pixelysize=abs(meta["geoTransform"][5]),
        crs=meta["coordinateSystem"]["wkt"],
        driver=meta["driverShortName"],
        compression=meta["metadata"]["IMAGE_STRUCTURE"].get("COMPRESSION", None),
    )

    for band in meta["bands"]:

        band_metadata = Band(
            no_data=band.get("noDataValue", None),
            data_type=from_gdal_data_type(band["type"]),
            nbits=band["metadata"].get("IMAGE_STRUCTURE", dict()).get("NBITS", None),
            blockxsize=band["block"][0],
            blockysize=band["block"][1],
        )

        try:
            if compute_stats:
                band_metadata.stats = BandStats(
                    min=band["minimum"],
                    max=band["maximum"],
                    mean=band["mean"],
                    std_dev=band["stdDev"],
                )
            if compute_histogram:
                band_metadata.histogram = Histogram(**band["histogram"])

            metadata.bands.append(band_metadata)
        except Exception as ex:
            # Don't bother logging because this happens in a sub-process and
            # won't be seen. But the msg of an exception DOES make it back,
            # so put something informative in there.
            msg = (
                f"Caught exception running {cmd} stdout: {o} stderr: {e} exception:{ex}"
            )
            raise Exception(msg)

    return metadata
