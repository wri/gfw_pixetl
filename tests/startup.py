from botocore.exceptions import EndpointConnectionError
from retrying import retry

from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.utils.aws import get_secret_client


def retry_if_endpoint_error(exception) -> bool:
    """Return True if we should retry, False otherwise."""
    is_endpoint_error: bool = isinstance(exception, EndpointConnectionError)
    return is_endpoint_error


@retry(
    retry_on_exception=retry_if_endpoint_error,
    stop_max_attempt_number=5,
    wait_fixed=2000,
)
def add_secrets():
    secret_client = get_secret_client()
    secret_client.create_secret(
        Name=GLOBALS.aws_gcs_key_secret_arn, SecretString="foosecret"
    )


if __name__ == "__main__":
    add_secrets()
