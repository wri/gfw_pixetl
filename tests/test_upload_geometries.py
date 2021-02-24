from unittest import mock

from geojson import FeatureCollection
from shapely.geometry import shape

from gfw_pixetl.utils.upload_geometries import (
    _extract_geoms,
    _to_feature_collection,
    _union_tile_geoms,
    generate_feature_collection,
    upload_geojsons,
)
from tests.test_pipe import _get_subset_tiles


def test_upload_geojsons(PIPE):
    all_tiles = list(_get_subset_tiles(PIPE))
    processed_tiles, existing_tiles = all_tiles[:2], all_tiles[2:]
    assert len(processed_tiles) == 2
    assert len(existing_tiles) == 2

    with mock.patch(
        "gfw_pixetl.utils.upload_geometries._upload_geojson", return_value={}
    ) as mock_upload_geojson, mock.patch(
        "gfw_pixetl.utils.upload_geometries._upload_extent", return_value={}
    ):
        # There should be 4 responses: a tiles.geojson and extent.geojson for 2 dst formats
        resps = upload_geojsons(
            processed_tiles, existing_tiles, "some_prefix", ignore_existing_tiles=False
        )
        assert resps == [dict(), dict(), dict(), dict()]

        # Can't compare to the FCs as a whole because the feature order is non-deterministic
        for mock_call in mock_upload_geojson.call_args_list:
            fc = mock_call[0][0]
            assert len(fc["features"]) == 4

    # Get new mocks, and this time ignore existing tiles
    with mock.patch(
        "gfw_pixetl.utils.upload_geometries._upload_geojson", return_value={}
    ) as mock_upload_geojson, mock.patch(
        "gfw_pixetl.utils.upload_geometries._upload_extent", return_value={}
    ):
        _ = upload_geojsons(
            processed_tiles, existing_tiles, "some_prefix", ignore_existing_tiles=True
        )

        for mock_call in mock_upload_geojson.call_args_list:
            fc = mock_call[0][0]
            assert len(fc["features"]) == 2


def test_union_tile_geoms(PIPE):
    tiles = list(_get_subset_tiles(PIPE))

    for dst_format in tiles[0].dst.keys():
        geoms = _extract_geoms(tiles, dst_format)
        assert len(geoms) == 4
        fc = _to_feature_collection(geoms)
        assert len(fc["features"]) == 4

        fc = _union_tile_geoms(fc)
        assert len(fc["features"]) == 1
        geom = shape(fc["features"][0]["geometry"])
        assert geom.bounds == (10, 9, 12, 11)


def test_generate_feature_collection(PIPE):
    tiles = list(_get_subset_tiles(PIPE))

    dst_format = "geotiff"
    fc: FeatureCollection = generate_feature_collection(tiles, dst_format)
    assert len(fc["features"]) == 4

    dst_format = "nonexisting"
    fc: FeatureCollection = generate_feature_collection(tiles, dst_format)
    assert len(fc["features"]) == 0
