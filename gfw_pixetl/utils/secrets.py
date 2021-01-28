import os

from gfw_pixetl import get_module_logger
from gfw_pixetl.settings.gdal import GDAL_ENV
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.utils.aws import get_secret_client

LOGGER = get_module_logger(__name__)


def set_google_application_credentials():

    credential_file = GDAL_ENV["GOOGLE_APPLICATION_CREDENTIALS"]

    if not os.path.isfile(credential_file):
        LOGGER.info("GCS key is missing. Try to fetch key from secret manager")

        client = get_secret_client()
        response = client.get_secret_value(SecretId=GLOBALS.aws_gcs_key_secret_arn)
        LOGGER.debug(response)

        os.makedirs(
            os.path.dirname(credential_file),
            exist_ok=True,
        )

        LOGGER.info("Write GCS key to file")
        with open(credential_file, "w") as f:
            f.write(response["SecretString"])

    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") != credential_file:
        LOGGER.info("Update ENV GOOGLE_APPLICATION_CREDENTIALS")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_file
