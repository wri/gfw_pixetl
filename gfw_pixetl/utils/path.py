import errno
import os
from urllib.parse import urlparse


def from_vsi(file_name: str) -> str:
    """Convert /vsi path to s3 or gs path."""

    protocols = {"vsis3": "s3", "vsigs": "gs"}

    parts = file_name.split("/")
    try:
        vsi = f"{protocols[parts[1]]}://{'/'.join(parts[2:])}"
    except KeyError:
        raise ValueError(f"Unknown protocol: {parts[1]}")
    return vsi


def to_vsi(file_name: str) -> str:
    prefix = {"s3": "vsis3", "gs": "vsigs"}

    parts = urlparse(file_name)
    try:
        path = f"/{prefix[parts.scheme]}/{parts.netloc}{parts.path}"
    except KeyError:
        raise ValueError(f"Unknown protocol: {parts.scheme}")

    return path


def create_dir(dir_name):
    try:
        os.makedirs(dir_name)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    return dir_name
