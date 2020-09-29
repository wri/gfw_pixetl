import multiprocessing
import os
from typing import Any, Dict, Optional

from gfw_pixetl.utils.path import get_aws_s3_endpoint
from gfw_pixetl.utils.secret import Secret
from gfw_pixetl.utils.type_casting import to_bool

CORES = multiprocessing.cpu_count()

DB_USERNAME: Optional[str] = os.environ.get("PGUSER", None)
_password: Optional[str] = os.environ.get("PGPASSWORD", None)
DB_PASSWORD: Optional[Secret] = Secret(_password) if _password else None
DB_HOST: Optional[str] = os.environ.get("PGHOST", None)
_port: Optional[str] = os.environ.get("PGPORT", None)
DB_PORT: Optional[int] = int(_port) if _port else None
DB_NAME: Optional[str] = os.environ.get("PGDATABASE", None)

AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")
JOB_ROLE_ARN: Optional[str] = os.environ.get("JOB_ROLE_ARN", None)

AWS_HTTPS: Optional[str] = os.environ.get("AWS_HTTPS", None)
AWS_VIRTUAL_HOSTING: Optional[bool] = to_bool(
    os.environ.get("AWS_VIRTUAL_HOSTING", None)
)
GDAL_DISABLE_READDIR_ON_OPEN: Optional[str] = os.environ.get(
    "GDAL_DISABLE_READDIR_ON_OPEN", None
)

ENDPOINT_URL: Optional[str] = os.environ.get("ENDPOINT_URL", None)
AWS_S3_ENDPOINT: Optional[str] = get_aws_s3_endpoint(ENDPOINT_URL)
AWS_BATCH_JOB_ID: Optional[str] = os.environ.get("AWS_BATCH_JOB_ID", None)
GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS", None
)
GCS_KEY_SECRET_ARN: Optional[str] = os.environ.get("GCS_KEY_SECRET_ARN", None)

GDAL_ENV: Dict[str, Any] = dict()
if AWS_HTTPS:
    GDAL_ENV["AWS_HTTPS"] = AWS_HTTPS
if AWS_VIRTUAL_HOSTING:
    GDAL_ENV["AWS_VIRTUAL_HOSTING"] = AWS_VIRTUAL_HOSTING
if GDAL_DISABLE_READDIR_ON_OPEN:
    GDAL_ENV["GDAL_DISABLE_READDIR_ON_OPEN"] = GDAL_DISABLE_READDIR_ON_OPEN
if AWS_S3_ENDPOINT:
    GDAL_ENV["AWS_S3_ENDPOINT"] = AWS_S3_ENDPOINT
    os.environ["AWS_S3_ENDPOINT"] = AWS_S3_ENDPOINT
