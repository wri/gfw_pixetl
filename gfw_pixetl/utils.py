import datetime
import os
import re
import uuid

from typing import Any, Dict, Optional

import boto3

from gfw_pixetl import get_module_logger

LOGGER = get_module_logger(__name__)

TOKEN_EXPIRATION: Optional[datetime.datetime] = None
AWS_ACCESS_KEY_ID: Optional[str] = None
AWS_SECRET_ACCESS_KEY: Optional[str] = None
AWS_SESSION_TOKEN: Optional[str] = None


def get_bucket(env: Optional[str] = None) -> str:
    """
    compose bucket name based on environment
    """

    if not env and "ENV" in os.environ:
        env = os.environ["ENV"]
    else:
        env = "dev"

    bucket = "gfw-data-lake"
    if env != "production":
        bucket += f"-{env}"
    return bucket


def verify_version_pattern(version: str) -> bool:
    """
    Verify if version matches general pattern
    - Must start with a v
    - Followed by up to three groups of digits seperated with a .
    - First group can have up to 8 digits
    - Second and third group up to 3 digits

    Examples:
    - v20191001
    - v1.1.2
    """

    if not version:
        message = "No version number provided"
        LOGGER.error(message)
        raise ValueError(message)

    p = re.compile(r"^v\d{,8}\.?\d{,3}\.?\d{,3}$")
    m = p.match(version)

    if not m:
        return False
    else:
        return True


def set_aws_credentials():
    """
    GDALwrap doesn't seem to be able to handle role permissions.
    Instead it requires presents of credentials in ENV variables or .aws/credential file.
    When run in batch environment, we alter ENV variables for sub process and add AWS credentials.
    """

    # only need to set credentials in AWS Batch environment
    if "AWS_BATCH_JOB_ID" in os.environ.keys():

        global TOKEN_EXPIRATION
        global AWS_ACCESS_KEY_ID
        global AWS_SECRET_ACCESS_KEY
        global AWS_SESSION_TOKEN

        env: Dict[str, Any] = os.environ.copy()
        client = boto3.client("sts")

        if not TOKEN_EXPIRATION or TOKEN_EXPIRATION <= datetime.datetime.now():
            LOGGER.debug("Update session token")

            credentials: Dict[str, Any] = client.assume_role(
                RoleArn=os.environ["JOB_ROLE_ARN"], RoleSessionName="pixETL"
            )

            TOKEN_EXPIRATION = credentials["Credentials"]["Expiration"]
            AWS_ACCESS_KEY_ID = credentials["Credentials"]["AccessKeyId"]
            AWS_SECRET_ACCESS_KEY = credentials["Credentials"]["SecretAccessKey"]
            AWS_SESSION_TOKEN = credentials["Credentials"]["SessionToken"]

        LOGGER.debug("Set AWS credentials")
        env["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
        env["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
        env["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN

        LOGGER.debug(f"ENV: {env}")
        return env

    else:
        return os.environ.copy()


def set_cwd():
    if "AWS_BATCH_JOB_ID" in os.environ.keys():
        cwd: str = os.environ["AWS_BATCH_JOB_ID"]
    else:
        cwd = str(uuid.uuid4())

    os.mkdir(cwd)
    os.chdir(cwd)
    LOGGER.info(f"Current Work Directory set to {os.path.curdir}")
