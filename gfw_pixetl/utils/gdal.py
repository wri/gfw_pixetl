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
from gfw_pixetl.models import BandStats, Stats

LOGGER = get_module_logger(__name__)


@retry(
    retry_on_exception=retry_if_missing_gcs_key_error,
    stop_max_attempt_number=2,
)
def create_vrt(uris: List[str], vrt="all.vrt", tile_list="tiles.txt") -> str:
    """
    ! Important this is not a parallelpipe Stage and must be run with only one worker per vrt file
    Create VRT file from input URI.
    """

    _write_tile_list(tile_list, uris)

    cmd = ["gdalbuildvrt", "-input_file_list", tile_list, vrt]

    try:
        run_gdal_subcommand(cmd)
    except GDALError:
        LOGGER.error("Could not create VRT file")
        raise
    finally:
        os.remove(tile_list)

    return vrt


@retry(
    retry_on_exception=retry_if_none_type_error,
    stop_max_attempt_number=7,
    wait_fixed=2000,
)
def run_gdal_subcommand(cmd: List[str], env: Optional[Dict] = None) -> Tuple[str, str]:
    """Run GDAL as sub command and catch common errors."""

    gdal_env = os.environ.copy()  # utils.set_aws_credentials()
    if env:
        gdal_env.update(**env)

    LOGGER.debug(f"RUN subcommand, using env {gdal_env}")
    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, env=gdal_env)
    o, e = p.communicate()

    if p.returncode != 0:
        if not e:
            raise GDALNoneTypeError(e.decode("utf-8"))
        elif (
            e
            == b"ERROR 15: AWS_SECRET_ACCESS_KEY and AWS_NO_SIGN_REQUEST configuration options not defined, and /root/.aws/credentials not filled\n"
        ):
            raise GDALAWSConfigError(e.decode("utf-8"))
        elif "ERROR 3: Load json file" in str(e):
            raise MissingGCSKeyError()
        else:
            raise GDALError(e.decode("utf-8"))

    return o.decode("utf-8"), e.decode("utf-8")


def compute_stats(uri: str) -> Stats:
    """Compute statistics and histogram using gdalinfo.

    Parse statistics as Stats object
    """

    cmd: List[str] = ["gdalinfo", "-stats", "-mm", "-hist", "-json", uri]

    o, _ = run_gdal_subcommand(cmd)

    meta: Dict[str, Any] = json.loads(o)
    stats = Stats(
        extent=(
            meta["cornerCoordinates"]["lowerLeft"][0],
            meta["cornerCoordinates"]["lowerLeft"][1],
            meta["cornerCoordinates"]["upperRight"][0],
            meta["cornerCoordinates"]["upperright"][1],
        ),
        width=meta["size"][0],
        height=meta["size"][1],
        pixelxsize=abs(meta["geoTransform"][0]),
        pixelysize=abs(meta["geoTransform"][5]),
        crs=meta["coordinateSystem"]["wkt"],
        driver=meta["driverShortName"],
        compression=meta["metadata"]["IMAGE_STRUCTURE"]["COMPRESSION"],
    )

    for band in meta["bands"]:
        band_stats = BandStats(
            min=band["minimum"],
            max=band["maximum"],
            mean=band["mean"],
            std_dev=band["stdDev"],
            histogram=band["histogram"],
            no_data=band["noDataValue"],
            data_type=from_gdal_data_type(band["type"]),
            nbits=band["metadata"]["IMAGE_STRUCTURE"].get("NBITS", None),
            blockxsize=band["block"][0],
            blockysize=band["block"][1],
        )

        stats.bands.append(band_stats)

    return stats


def _write_tile_list(tile_list: str, uris: List[str]) -> None:
    with open(tile_list, "w") as input_tiles:
        for uri in uris:
            LOGGER.debug(f"Add {uri} to tile list")
            input_tiles.write(f"{uri}\n")
