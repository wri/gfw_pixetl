from unittest.mock import patch

import pytest

from gfw_pixetl.utils.sources import download_sources, get_file_list_from_cloud_folder


@pytest.mark.parametrize(
    "provider,mocked_func", [("gs", "get_gs_files"), ("s3", "get_aws_files")]
)
def test_download_sources_preserves_wildcard_filename_prefix(
    tmp_path, provider, mocked_func
):
    bucket = "some-bucket"
    key_prefix = "GLADalert/C2/2021/final/alert21"
    source_uri = f"{provider}://{bucket}/{key_prefix}*"

    with patch(f"gfw_pixetl.utils.sources.{mocked_func}", return_value=[]) as mock_list:
        download_sources([source_uri], str(tmp_path))

    called_bucket, called_prefix = mock_list.call_args.args
    assert called_bucket == bucket
    assert called_prefix == key_prefix
    assert not called_prefix.endswith("/")


@pytest.mark.parametrize(
    "input_prefix,expected_lookup_prefix",
    [
        ("GLADalert/C2/2021/final/alert21*", "GLADalert/C2/2021/final/alert21"),
        ("GLADalert/C2/2021/final/", "GLADalert/C2/2021/final/"),
        ("GLADalert/C2/2021/final", "GLADalert/C2/2021/final/"),
    ],
)
def test_cloud_folder_prefix_semantics(input_prefix, expected_lookup_prefix):
    with patch("gfw_pixetl.utils.sources.get_gs_files", return_value=[]) as mock_list:
        get_file_list_from_cloud_folder("gs", "some-bucket", input_prefix)

    assert mock_list.call_args.args[1] == expected_lookup_prefix
