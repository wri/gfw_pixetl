import os
import shutil
import uuid

import pytest

from gfw_pixetl.utils.path import create_dir, from_vsi, to_vsi


def test_from_vsi():
    vsis3 = "/vsis3/bucket/some/prefix/file.ext"
    s3_path = from_vsi(vsis3)

    assert s3_path == "s3://bucket/some/prefix/file.ext"

    vsigs = "/vsigs/bucket/some/prefix/file.ext"
    gs_path = from_vsi(vsigs)

    assert gs_path == "gs://bucket/some/prefix/file.ext"

    with pytest.raises(ValueError):
        vsizip = "/vsizip/bucket/some/prefix/file.ext"
        _ = from_vsi(vsizip)


def test_to_vsi():
    s3_path = "s3://bucket/some/prefix/file.ext"
    vsis3 = to_vsi(s3_path)

    assert vsis3 == "/vsis3/bucket/some/prefix/file.ext"

    gs_path = "gs://bucket/some/prefix/file.ext"
    vsigs = to_vsi(gs_path)

    assert vsigs == "/vsigs/bucket/some/prefix/file.ext"

    with pytest.raises(ValueError):
        zip_path = "zip://bucket/some/prefix/file.ext"
        _ = to_vsi(zip_path)


def test_create_dir():
    dir_name = str(uuid.uuid4())
    create_dir(dir_name)
    assert os.path.isdir(dir_name)

    # Creating the dir again should not create an error
    create_dir(dir_name)

    shutil.rmtree(dir_name)
