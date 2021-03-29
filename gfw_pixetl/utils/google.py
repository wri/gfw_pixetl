from collections import Sequence
from typing import List

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage
from retrying import retry

from gfw_pixetl.errors import MissingGCSKeyError, retry_if_missing_gcs_key_error


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


@retry(
    retry_on_exception=retry_if_missing_gcs_key_error,
    stop_max_attempt_number=2,
)
def get_gs_files(
    bucket: str, prefix: str, extensions: Sequence[str] = (".tif",)
) -> List[str]:
    """Get all geotiffs in GCS."""

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
