import os
import re
from typing import Any, Dict, Optional

from gfw_pixetl import get_module_logger


logger = get_module_logger(__name__)


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
        logger.error(message)
        raise ValueError(message)

    p = re.compile(r"^v\d{,8}\.?\d{,3}\.?\d{,3}$")
    m = p.match(version)

    if not m:
        return False
    else:
        return True
