from typing import Optional
from urllib.parse import urlparse


def get_aws_s3_endpoint(endpoint: Optional[str]) -> Optional[str]:
    """check if AWS_S3_ENDPOINT or ENDPOINT_URL is set and remove protocol from
    endpoint if present."""

    if endpoint:
        o = urlparse(endpoint, allow_fragments=False)
        if o.scheme and o.netloc:
            result: Optional[str] = o.netloc
        else:
            result = o.path
    else:
        result = None

    return result
