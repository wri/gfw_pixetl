import pytest

from gfw_pixetl.data_type import DataTypeEnum
from gfw_pixetl.utils.gdal import get_metadata
from tests.conftest import TILE_4_PATH


def test_get_metadata():
    metadata = get_metadata(TILE_4_PATH)

    assert metadata.height == 4000
    assert metadata.width == 4000
    assert metadata.compression is None
    assert metadata.driver == "GTiff"
    assert metadata.extent == (1, 0, 2, 1)

    assert len(metadata.bands) == 1
    assert metadata.bands[0].data_type == DataTypeEnum.uint8
    assert metadata.bands[0].nbits is None
    assert metadata.bands[0].no_data == 0
    assert metadata.bands[0].blockxsize == 4000
    assert metadata.bands[0].blockysize == 2
    assert metadata.bands[0].stats is None
    assert metadata.bands[0].histogram is None

    metadata = get_metadata(TILE_4_PATH, True)
    assert metadata.bands[0].stats.min == 1.0
    assert metadata.bands[0].stats.max == 4.0
    assert metadata.bands[0].stats.mean == pytest.approx(2.5, 0.001)
    assert metadata.bands[0].stats.std_dev == pytest.approx(1.118, 0.001)

    metadata = get_metadata(TILE_4_PATH, False, True)
    assert metadata.bands[0].histogram.count == 256
    assert metadata.bands[0].histogram.min == -0.5
    assert metadata.bands[0].histogram.max == 255.5
    assert len(metadata.bands[0].histogram.buckets) == 256
    assert metadata.bands[0].histogram.buckets[0] == 0
    assert (
        sum(metadata.bands[0].histogram.buckets) > 12000000
    )  # no data values are not included in histogram and we hence don't know the exact number of data pixels
