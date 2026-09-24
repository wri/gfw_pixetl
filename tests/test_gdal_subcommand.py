from unittest.mock import Mock, patch

import pytest

from gfw_pixetl.errors import GDALAWSConfigError
from gfw_pixetl.utils.gdal import run_gdal_subcommand


def test_run_gdal_subcommand_recognizes_aws_config_error_after_decode():
    process = Mock()
    process.communicate.return_value = (
        b"",
        b"ERROR 15: AWS_SECRET_ACCESS_KEY and AWS_NO_SIGN_REQUEST configuration options not defined, and /root/.aws/credentials not filled\n",
    )
    process.returncode = 1

    with patch("gfw_pixetl.utils.gdal.sp.Popen", return_value=process):
        with pytest.raises(GDALAWSConfigError):
            run_gdal_subcommand(["gdalinfo", "dummy.tif"], env={})
