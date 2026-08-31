"""Regression tests for the wildcard source_uri prefix bug.

Bug summary
-----------
``download_sources()`` in ``gfw_pixetl/utils/sources.py`` strips a trailing
``*`` off the prefix *before* handing it to
``get_file_list_from_cloud_folder()``:

    prefix: str = (str(o.path)).lstrip("/").rstrip("*")

``get_file_list_from_cloud_folder()`` then does its own pseudo-globbing:

    if new_prefix.endswith("*"):
        new_prefix = new_prefix[:-1]
    elif not new_prefix.endswith("/"):
        new_prefix += "/"

Because the "*" has already been removed by ``download_sources()``, the
``endswith("*")`` check never fires, so the function falls into the
``elif`` branch and appends a trailing "/". A source_uri like
``gs://bucket/GLADalert/C2/2021/final/alert21*`` (intended to match files
named like ``alert21_020W_50N.tif``) ends up being looked up with prefix
``GLADalert/C2/2021/final/alert21/`` -- which matches nothing, since the
real objects are not stored under an "alert21/" subfolder.

The net effect in production: every source_uri silently resolves to zero
files, ``input_bands`` ends up empty, and pixetl later fails deep inside
``RasterSrcLayer.geom`` with ``RuntimeError: Input bands do not overlap``,
which is a confusing error far removed from the actual cause.

These tests assert the prefix that actually reaches the storage-provider
file-listing calls, so they fail on the buggy code and pass once the
double-stripping is fixed (e.g. by not stripping "*" in
``download_sources()`` and leaving that job entirely to
``get_file_list_from_cloud_folder()``).
"""

import os
from unittest.mock import patch

import pytest

os.environ["ENV"] = "test"

from gfw_pixetl.utils.sources import (  # noqa: E402
    download_sources,
    get_file_list_from_cloud_folder,
)


@pytest.mark.parametrize(
    "provider,mocked_func", [("gs", "get_gs_files"), ("s3", "get_aws_files")]
)
def test_download_sources_preserves_wildcard_filename_prefix(
    tmp_path, provider, mocked_func
):
    """A source_uri ending in '*' should be treated as a filename prefix (no
    trailing slash), not as a subfolder, no matter which provider is used."""
    bucket = "some-bucket"
    key_prefix = "GLADalert/C2/2021/final/alert21"
    source_uri = f"{provider}://{bucket}/{key_prefix}*"

    with patch(f"gfw_pixetl.utils.sources.{mocked_func}", return_value=[]) as mock_list:
        download_sources([source_uri], str(tmp_path))

    assert mock_list.called, f"{mocked_func} was never called"
    called_bucket, called_prefix = mock_list.call_args.args

    assert called_bucket == bucket
    assert called_prefix == key_prefix, (
        "Expected the wildcard '*' to be stripped exactly once, leaving a "
        f"bare filename prefix ({key_prefix!r}). Got {called_prefix!r} "
        "instead -- this looks like the double-stripping bug where a "
        "trailing '/' gets appended after the '*' was already removed."
    )
    assert not called_prefix.endswith("/")


def test_download_sources_finds_files_with_shared_filename_prefix(tmp_path):
    """End-to-end check: files that share a filename prefix (rather than
    living in a same-named subfolder) must be discovered and queued for
    download when the source_uri uses a trailing '*'."""
    bucket = "some-bucket"
    key_prefix = "GLADalert/C2/2021/final/alert21"
    source_uri = f"gs://{bucket}/{key_prefix}*"

    matching_files = [
        f"/vsigs/{bucket}/{key_prefix}_020W_50N.tif",
        f"/vsigs/{bucket}/{key_prefix}_030W_50N.tif",
    ]

    def fake_get_gs_files(bucket_arg, prefix_arg, *args, **kwargs):
        # Mimic real GCS prefix-matching semantics: only return files whose
        # name actually starts with the given prefix.
        return [
            f
            for f in matching_files
            if f == f"/vsigs/{bucket_arg}/{prefix_arg}"[: len(f)]
            or f.startswith(f"/vsigs/{bucket_arg}/{prefix_arg}")
        ]

    def fake_download_source_file(args):
        # download_sources() asserts the returned path exists, so actually
        # create a placeholder file rather than mocking os.path.exists
        # globally (which would also mask real directory-creation bugs).
        _, basedir = args
        downloaded = tmp_path / "downloaded.tif"
        downloaded.touch()
        return downloaded

    with (
        patch("gfw_pixetl.utils.sources.get_gs_files", side_effect=fake_get_gs_files),
        patch(
            "gfw_pixetl.utils.sources.download_source_file",
            side_effect=fake_download_source_file,
        ) as mock_download,
    ):
        download_sources([source_uri], str(tmp_path))

    downloaded_uris = [call.args[0][0] for call in mock_download.call_args_list]
    assert downloaded_uris, (
        "No files were queued for download. With the double-stripping bug, "
        "the prefix gets an erroneous trailing '/' appended and the real "
        "files (which share a filename prefix, not a subfolder) are never "
        "found."
    )
    # download_sources() converts /vsigs/... paths to gs:// URIs (via
    # from_vsi()) before queueing them for download.
    expected_uris = {f.replace("/vsigs/", "gs://") for f in matching_files}
    assert set(downloaded_uris) == expected_uris


@pytest.mark.parametrize(
    "input_prefix,expected_lookup_prefix",
    [
        ("GLADalert/C2/2021/final/alert21*", "GLADalert/C2/2021/final/alert21"),
        ("GLADalert/C2/2021/final/", "GLADalert/C2/2021/final/"),
        ("GLADalert/C2/2021/final", "GLADalert/C2/2021/final/"),
    ],
)
def test_get_file_list_from_cloud_folder_prefix_semantics(
    input_prefix, expected_lookup_prefix
):
    """Sanity-check get_file_list_from_cloud_folder()'s own pseudo-globbing
    in isolation: a trailing '*' means 'filename prefix' (no slash added),
    while no trailing '*' means 'folder' (slash added if missing)."""
    with patch("gfw_pixetl.utils.sources.get_gs_files", return_value=[]) as mock_list:
        get_file_list_from_cloud_folder("gs", "some-bucket", input_prefix)

    called_prefix = mock_list.call_args.args[1]
    assert called_prefix == expected_lookup_prefix
