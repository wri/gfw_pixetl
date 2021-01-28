import os

from moto import mock_secretsmanager

from gfw_pixetl.settings.gdal import GDAL_ENV
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.utils.aws import get_secret_client
from gfw_pixetl.utils.secrets import set_google_application_credentials


@mock_secretsmanager
def test_gcs_secret():
    secret = "foosecret"  # pragma: allowlist secret
    secret_client = get_secret_client()
    secret_client.create_secret(
        Name=GLOBALS.aws_gcs_key_secret_arn, SecretString=secret
    )
    credential_file = GDAL_ENV["GOOGLE_APPLICATION_CREDENTIALS"]

    if os.path.isfile(credential_file):
        os.remove(credential_file)
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS")

    assert not os.path.isfile(credential_file)

    set_google_application_credentials()
    assert os.path.isfile(credential_file)
    with open(credential_file) as src:
        data = src.read()
    assert data == secret
    assert os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") == credential_file
