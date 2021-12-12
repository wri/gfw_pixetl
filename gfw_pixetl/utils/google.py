import os
from typing import List, Sequence

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage
from retrying import retry

from gfw_pixetl import get_module_logger
from gfw_pixetl.errors import MissingGCSKeyError, retry_if_missing_gcs_key_error

LOGGER = get_module_logger(__name__)


@retry(
    retry_on_exception=retry_if_missing_gcs_key_error,
    stop_max_attempt_number=2,
)
def download_gcs(bucket: str, key: str, dst: str) -> None:

    try:
        storage_client = storage.Client()
    except DefaultCredentialsError:
        raise MissingGCSKeyError()

    gs_bucket = storage_client.bucket(bucket)
    blob = gs_bucket.blob(key)
    blob.download_to_filename(dst)
    if os.stat(dst).st_size == 0:
        LOGGER.error(
            "Call to download_to_filename succeeded, but result is an empty file!"
        )
    else:
        LOGGER.info(f"Downloaded file {dst} is of size {os.stat(dst).st_size}")
    if os.stat(dst).st_size <= 1000:
        with open(dst, "r") as file_obj:
            LOGGER.debug(f"Contents of small file {dst}: {file_obj.read()}")


@retry(
    retry_on_exception=retry_if_missing_gcs_key_error,
    stop_max_attempt_number=2,
)
def get_gs_files(
    bucket: str, prefix: str, extensions: Sequence[str] = (".tif",)
) -> List[str]:
    """Get all matching files in GCS."""

    try:
        storage_client = storage.Client()
    except DefaultCredentialsError:
        raise MissingGCSKeyError()

    blobs = storage_client.list_blobs(bucket, prefix=prefix)
    files = [
        f"/vsigs/{bucket}/{blob.name}"
        for blob in blobs
        if any(blob.name.endswith(ext) for ext in extensions)
    ]
    return files
