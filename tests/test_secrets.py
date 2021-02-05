import os

from moto import mock_secretsmanager

from gfw_pixetl.settings.gdal import GDAL_ENV
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.utils.secrets import set_google_application_credentials


@mock_secretsmanager
def test_gcs_secret():
    secret = "foosecret"  # pragma: allowlist secret
    credential_file = GDAL_ENV["GOOGLE_APPLICATION_CREDENTIALS"]

    # Secret should already be set during initialization
    _secret_set(credential_file, secret)

    # lets remove everything and try to set it again
    if os.path.isfile(credential_file):
        os.remove(credential_file)
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS")

    assert not os.path.isfile(credential_file)

    set_google_application_credentials(credential_file)
    _secret_set(credential_file, secret)


def _secret_set(credential_file, secret):

    set_google_application_credentials(credential_file)

    # we skip this part so that we don't have to reach out to mocked secrets manager
    # This caused issues when running tests in Github Actions.
    # Once bug in Localstack is fixed me might be able to test this again.
    # https://github.com/localstack/serverless-localstack/issues/83
    if GLOBALS.aws_gcs_key_secret_arn:
        assert os.path.isfile(credential_file)
        with open(credential_file) as src:
            data = src.read()
        assert data == secret
    assert os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") == credential_file
