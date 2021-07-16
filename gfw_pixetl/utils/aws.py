from typing import Any, Dict, List, Optional, Sequence

import boto3
from boto3.s3.transfer import TransferConfig

from gfw_pixetl.decorators import processify
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


@processify
def download_s3(bucket: str, key: str, dst: str) -> Dict[str, Any]:
    s3_client = get_s3_client()
    config = TransferConfig(use_threads=False)
    return s3_client.download_file(bucket, key, dst, Config=config)


@processify
def upload_s3(path: str, bucket: str, dst: str) -> Dict[str, Any]:
    s3_client = get_s3_client()
    config = TransferConfig(use_threads=False)
    return s3_client.upload_file(path, bucket, dst, Config=config)


@processify
def get_aws_files(
    bucket: str, prefix: str, extensions: Sequence[str] = (".tif",)
) -> List[str]:
    """Get all matching files in S3."""
    files: List[str] = list()

    s3_client = get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        try:
            contents = page["Contents"]
        except KeyError:
            break

        for obj in contents:
            key = str(obj["Key"])
            if any(key.endswith(ext) for ext in extensions):
                files.append(f"/vsis3/{bucket}/{key}")

    return files
