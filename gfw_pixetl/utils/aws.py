from typing import Any, Dict, List, Optional, Sequence

import boto3

from gfw_pixetl.decorators import processify
from gfw_pixetl.settings.globals import GLOBALS


def client_constructor(service: str, endpoint_url: Optional[str] = None):
    """Get a client of the desired service.

    Unlike in the data API, DON'T reuse clients because it leads to
    processes sharing connections, which causes weird errors. See
    https://github.com/dask/dask/issues/1292
    """
    return lambda: boto3.client(
        service, region_name=GLOBALS.aws_region, endpoint_url=endpoint_url
    )


get_s3_client = client_constructor("s3", endpoint_url=GLOBALS.aws_endpoint_url)
get_batch_client = client_constructor("batch")
get_sts_client = client_constructor("sts")
get_secret_client = client_constructor(
    "secretsmanager", endpoint_url=GLOBALS.aws_secretsmanager_url
)


@processify
def download_s3(bucket: str, key: str, dst: str) -> Dict[str, Any]:
    s3_client = get_s3_client()
    return s3_client.download_file(bucket, key, dst)


@processify
def upload_s3(path: str, bucket: str, dst: str) -> Dict[str, Any]:
    s3_client = get_s3_client()
    return s3_client.upload_file(path, bucket, dst)


@processify
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
