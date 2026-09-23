import os
import shutil
import tempfile
import threading
from pathlib import Path
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

os.environ["ENV"] = "test"

from gfw_pixetl.settings.globals import GLOBALS  # noqa: E402
from gfw_pixetl.utils.sources import (  # noqa: E402
    _is_transient_download_error,
    download_source_file,
    download_sources,
)


@pytest.fixture
def work_dir():
    d = tempfile.mkdtemp(prefix="test_parallel_downloads_")
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_download_sources_uses_bounded_thread_parallelism(work_dir):
    files = [f"/vsigs/bucket/file-{i}.tif" for i in range(4)]
    active = 0
    max_active = 0
    lock = threading.Lock()
    release = threading.Event()
    downloaded = Path(work_dir) / "downloaded.tif"
    downloaded.touch()

    def fake_download(args):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
            if active >= 2:
                release.set()
        release.wait(timeout=1)
        with lock:
            active -= 1
        return downloaded

    with (
        patch("gfw_pixetl.utils.sources.get_gs_files", return_value=files),
        patch(
            "gfw_pixetl.utils.sources.download_source_file", side_effect=fake_download
        ),
        patch.object(GLOBALS, "download_workers", 4),
    ):
        download_sources(["gs://bucket/prefix/"], work_dir)

    assert max_active >= 2


def test_download_source_file_retries_transient_errors(work_dir):
    attempts = 0

    def flaky_download(**kwargs):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise TimeoutError("temporary timeout")

    with (
        patch("gfw_pixetl.utils.sources.download_gcs", side_effect=flaky_download),
        patch("gfw_pixetl.utils.sources.time.sleep") as sleep,
    ):
        path = download_source_file(("gs://bucket/path/file.tif", work_dir))

    assert attempts == 3
    assert path == Path(work_dir) / "bucket/path/file.tif"
    assert [call.args[0] for call in sleep.call_args_list] == [1.0, 2.0]


def test_download_source_file_does_not_retry_non_transient_errors(work_dir):
    with (
        patch(
            "gfw_pixetl.utils.sources.download_gcs",
            side_effect=FileNotFoundError("missing"),
        ) as download,
        patch("gfw_pixetl.utils.sources.time.sleep") as sleep,
    ):
        with pytest.raises(FileNotFoundError):
            download_source_file(("gs://bucket/path/missing.tif", work_dir))

    assert download.call_count == 1
    sleep.assert_not_called()


def test_transient_download_error_classification():
    throttled = ClientError(
        {
            "Error": {"Code": "SlowDown", "Message": "slow down"},
            "ResponseMetadata": {"HTTPStatusCode": 400},
        },
        "GetObject",
    )
    missing = ClientError(
        {
            "Error": {"Code": "NoSuchKey", "Message": "missing"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        },
        "GetObject",
    )

    assert _is_transient_download_error(throttled)
    assert _is_transient_download_error(
        EndpointConnectionError(endpoint_url="https://example.invalid")
    )
    assert not _is_transient_download_error(missing)
    assert not _is_transient_download_error(FileNotFoundError("local path"))
