from google.cloud import storage


def download_gcs(bucket: str, key: str, dst: str) -> None:
    storage_client = storage.Client()

    gs_bucket = storage_client.bucket(bucket)
    blob = gs_bucket.blob(key)
    blob.download_to_filename(dst)
