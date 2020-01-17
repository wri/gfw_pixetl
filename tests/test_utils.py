import os
from datetime import datetime
from dateutil.tz import tzutc
from unittest import mock

from gfw_pixetl.utils import (
    get_bucket,
    verify_version_pattern,
    set_aws_credentials,
    set_cwd,
)

os.environ["ENV"] = "test"


class Client(object):
    def __init__(self, service):
        pass

    @staticmethod
    def assume_role(RoleArn, RoleSessionName):
        return {
            "Credentials": {
                "Expiration": datetime.now(tz=tzutc()),
                "AccessKeyId": "test",
                "SecretAccessKey": "test",
                "SessionToken": "test",
            }
        }


def test_get_bucket():
    os.environ["ENV"] = "production"
    bucket: str = get_bucket()
    assert bucket == "gfw-data-lake"

    os.environ["ENV"] = "staging"
    bucket = get_bucket()
    assert bucket == "gfw-data-lake-staging"

    os.environ["ENV"] = "dev"
    bucket = get_bucket()
    assert bucket == "gfw-data-lake-dev"

    os.environ["ENV"] = "test"
    bucket = get_bucket()
    assert bucket == "gfw-data-lake-test"


def test_verify_version_pattern():
    assert verify_version_pattern("v2019") is True
    assert verify_version_pattern("v201911") is True
    assert verify_version_pattern("v20191122") is True
    assert verify_version_pattern("v1") is True
    assert verify_version_pattern("v1.2") is True
    assert verify_version_pattern("v1.2.3") is True
    assert verify_version_pattern("v1.beta") is False
    assert verify_version_pattern("1.2") is False
    assert verify_version_pattern("version1.2.3") is False
    assert verify_version_pattern("v.1.2.3") is False


def test_set_aws_credentials():
    env = os.environ.copy()
    result = set_aws_credentials()

    assert env == result
    assert "AWS_ACCESS_KEY_ID" not in list(result.keys())
    assert "AWS_SECRET_ACCESS_KEY" not in list(result.keys())
    assert "AWS_SESSION_TOKEN" not in list(result.keys())

    os.environ["AWS_BATCH_JOB_ID"] = "test"
    os.environ["JOB_ROLE_ARN"] = "test"
    env = os.environ.copy()

    with mock.patch("boto3.client", return_value=Client):
        result = set_aws_credentials()

    assert env != result
    assert "AWS_ACCESS_KEY_ID" in list(result.keys())
    assert "AWS_SECRET_ACCESS_KEY" in list(result.keys())
    assert "AWS_SESSION_TOKEN" in list(result.keys())

    del os.environ["AWS_BATCH_JOB_ID"]
    del os.environ["JOB_ROLE_ARN"]


def test_set_cwd():
    cwd = os.getcwd()
    new_dir = set_cwd()
    assert cwd != os.getcwd()
    assert os.path.join(cwd, new_dir) == os.getcwd()
    os.chdir(cwd)
    os.rmdir(new_dir)
