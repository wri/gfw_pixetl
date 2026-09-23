import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlparse

from botocore.exceptions import BotoCoreError, ClientError
from geojson import FeatureCollection
from shapely.geometry import shape

from gfw_pixetl import get_module_logger
from gfw_pixetl.models.enums import DstFormat
from gfw_pixetl.models.types import ShapePathPair
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.sources import RasterSource
from gfw_pixetl.utils.aws import download_s3, get_aws_files, get_s3_client
from gfw_pixetl.utils.geometry import generate_feature_collection
from gfw_pixetl.utils.google import download_gcs, get_gs_files
from gfw_pixetl.utils.path import create_dir, from_vsi
from gfw_pixetl.utils.utils import DummyTile

LOGGER = get_module_logger(__name__)

DOWNLOAD_MAX_ATTEMPTS = 4
DOWNLOAD_INITIAL_BACKOFF_SECONDS = 1.0
DOWNLOAD_MAX_BACKOFF_SECONDS = 8.0


def _http_status_code(exception: Exception) -> Optional[int]:
    """Best-effort extraction of an HTTP status from cloud SDK exceptions."""
    if isinstance(exception, ClientError):
        return exception.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    status = getattr(exception, "code", None)
    if callable(status):
        status = status()
    status = getattr(status, "value", status)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _is_transient_download_error(exception: Exception) -> bool:
    """Return True for cloud/network failures which are safe to retry."""
    if isinstance(exception, BotoCoreError):
        return True

    if isinstance(exception, (TimeoutError, ConnectionError)):
        return True

    status = _http_status_code(exception)
    if status == 429 or (status is not None and 500 <= status < 600):
        return True

    if isinstance(exception, ClientError):
        code = str(exception.response.get("Error", {}).get("Code", ""))
        return code in {
            "SlowDown",
            "RequestTimeout",
            "RequestTimeoutException",
            "Throttling",
            "ThrottlingException",
            "TooManyRequestsException",
        }

    return False


def get_file_list_from_tiles_geojson(bucket: str, prefix: str) -> List[str]:
    """Fetches a geojson file from AWS and returns the filenames contained
    therein (whatever their format, but note that we generally store GDAL-
    style /vsi URLs)"""
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=bucket, Key=prefix)
    body = response["Body"].read()

    features = json.loads(body.decode("utf-8"))["features"]

    return [feature["properties"]["name"] for feature in features]


def get_file_list_from_cloud_folder(
    provider: str, bucket: str, prefix: str
) -> List[str]:
    # Allow pseudo-globbing: If the prefix doesn't end in *, assume the user
    # meant for the prefix to specify a "folder" and add a "/" to enforce
    # that behavior.
    new_prefix: str = prefix
    if new_prefix.endswith("*"):
        new_prefix = new_prefix[:-1]
    elif not new_prefix.endswith("/"):
        new_prefix += "/"

    get_files_constructor = {"s3": get_aws_files, "gs": get_gs_files}

    return get_files_constructor[provider](bucket, new_prefix)


def get_shape_path_pairs_under_directory(dir_path: str) -> List[ShapePathPair]:
    path_obj = Path(dir_path)
    path_list = list(path_obj.rglob("*.tif"))

    tiles: List[DummyTile] = list()

    for path in path_list:
        src = RasterSource(str(path))
        tiles.append(DummyTile({"geotiff": src}))

    fc: FeatureCollection = generate_feature_collection(
        tiles, DstFormat(GLOBALS.default_dst_format)
    )

    return [
        (shape(feature["geometry"]), feature["properties"]["name"])
        for feature in fc["features"]
    ]


def download_source_file(args: Tuple[str, str]) -> Path:
    """Download remote AWS or GCS files."""
    remote_file, basedir = args

    download_constructor = {"gs": download_gcs, "s3": download_s3}

    parts = urlparse(remote_file)

    local_file = Path(os.path.join(basedir, str(parts.netloc), str(parts.path[1:])))
    os.makedirs(os.path.dirname(local_file), exist_ok=True)

    LOGGER.debug(f"Downloading remote file {remote_file} to {local_file}")

    delay = DOWNLOAD_INITIAL_BACKOFF_SECONDS
    for attempt in range(1, DOWNLOAD_MAX_ATTEMPTS + 1):
        try:
            download_constructor[parts.scheme](
                bucket=str(parts.netloc), key=str(parts.path[1:]), dst=str(local_file)
            )
            return local_file
        except Exception as exc:
            if attempt == DOWNLOAD_MAX_ATTEMPTS or not _is_transient_download_error(
                exc
            ):
                raise

            LOGGER.warning(
                f"Transient error downloading {remote_file} "
                f"(attempt {attempt}/{DOWNLOAD_MAX_ATTEMPTS}): {exc}. "
                f"Retrying in {delay:.1f}s."
            )
            time.sleep(delay)
            delay = min(delay * 2, DOWNLOAD_MAX_BACKOFF_SECONDS)

    raise AssertionError("download retry loop exited unexpectedly")


def download_sources(source_uris: List[str], work_dir: str) -> List[str]:
    """Given a list of source URIs (pointing to any combination of
    tiles.geojsons and cloud storage folders), download all indicated files to
    the local filesystem and return a new list of source_uris pointing to those
    local directories."""
    assert isinstance(source_uris, list)

    file_uris: List[Tuple[str, str]] = list()
    local_source_uris: List[str] = list()

    for i, source_uri in enumerate(source_uris):
        o = urlparse(source_uri, allow_fragments=False)

        bucket: str = str(o.netloc)
        prefix: str = (str(o.path)).lstrip("/")

        local_source_dir = f"{work_dir}/input/source{i}"

        if prefix.endswith(".geojson"):
            file_uris += [
                (from_vsi(file_uri), local_source_dir)
                for file_uri in get_file_list_from_tiles_geojson(bucket, prefix)
            ]
        else:
            file_uris += [
                (from_vsi(str(file_uri)), local_source_dir)
                for file_uri in get_file_list_from_cloud_folder(
                    str(o.scheme), bucket, prefix
                )
            ]
        create_dir(local_source_dir)
        local_source_uris.append(local_source_dir)

    LOGGER.info(f"Complete list of file_uris to download: {file_uris}")

    if file_uris:
        download_workers = min(GLOBALS.download_workers, len(file_uris))
        started = time.monotonic()
        LOGGER.info(
            f"Downloading {len(file_uris)} source files with "
            f"{download_workers} concurrent workers"
        )

        with ThreadPoolExecutor(
            max_workers=download_workers, thread_name_prefix="source-download"
        ) as executor:
            futures = [
                executor.submit(download_source_file, file_uri)
                for file_uri in file_uris
            ]
            for future in as_completed(futures):
                file_path = future.result()
                assert os.path.exists(
                    file_path
                ), f"In download_sources. {file_path} does not exist!"

        LOGGER.info(
            f"Downloaded {len(file_uris)} source files in "
            f"{time.monotonic() - started:.1f}s"
        )

    return local_source_uris
