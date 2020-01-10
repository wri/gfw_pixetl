import os

from gfw_pixetl.utils import get_bucket, verify_version_pattern

os.environ["ENV"] = "test"


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
