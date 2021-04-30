from typing import List, Optional, Sequence

import boto3
from boto3.s3.transfer import TransferConfig

from gfw_pixetl.settings.globals import GLOBALS


def client_constructor(service: str, endpoint_url: Optional[str] = None):
    """Using closure design for a client constructor This way we only need to
    create the client once in central location and it will be easier to
    mock."""
    service_client = None

    def client():
        nonlocal service_client
        if service_client is None:
            service_client = boto3.client(
                service, region_name=GLOBALS.aws_region, endpoint_url=endpoint_url
            )
        return service_client

    return client


get_s3_client = client_constructor("s3", endpoint_url=GLOBALS.aws_endpoint_url)
get_batch_client = client_constructor("batch")
get_sts_client = client_constructor("sts")
get_secret_client = client_constructor(
    "secretsmanager", endpoint_url=GLOBALS.aws_secretsmanager_url
)


def download_s3(bucket: str, key: str, dst: str) -> None:
    s3_client = get_s3_client()
    config = TransferConfig(use_threads=False)
    s3_client.download_file(bucket, key, dst, Config=config)


def upload_s3(path: str, bucket: str, dst: str) -> None:
    s3_client = get_s3_client()
    config = TransferConfig(use_threads=False)
    s3_client.upload_file(path, bucket, dst, Config=config)


def get_aws_files(
    bucket: str, prefix: str, extensions: Sequence[str] = (".tif",)
) -> List[str]:
    """Get all geotiffs in S3."""
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    objs = response.get("Contents", [])
    files = [
        f"/vsis3/{bucket}/{obj['Key']}"
        for obj in objs
        if any(obj["Key"].endswith(ext) for ext in extensions)
    ]

    return files
