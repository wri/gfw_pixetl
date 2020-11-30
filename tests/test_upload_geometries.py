from shapely.geometry import shape

from gfw_pixetl.utils.upload_geometries import (
    _geoms_uris_per_dst_format,
    _merge_feature_collections,
    _to_feature_collection,
    _union_tile_geoms,
)
from tests.conftest import BUCKET, GEOJSON_NAME
from tests.test_pipe import _get_subset_tiles


def test__merge_feature_collection():
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-20.0, 10.0],
                            [-10.0, 10.0],
                            [-10.0, 0.0],
                            [-20.0, 0.0],
                            [-20.0, 10.0],
                        ]
                    ],
                },
                "properties": {"name": "/vsis3/gfw-data-lake-test/10N_020W.tif"},
            }
        ],
    }

    merged_fc = _merge_feature_collections(fc, BUCKET, GEOJSON_NAME)
    assert fc != merged_fc
    assert len(merged_fc["features"]) == 3


def test__union_tile_geoms():
    tiles = list(_get_subset_tiles())

    features = _geoms_uris_per_dst_format(tiles)
    for dst_format in features.keys():
        assert len(features[dst_format]) == 4
        fc = _to_feature_collection(features[dst_format])
        assert len(fc["features"]) == 4

        fc = _union_tile_geoms(fc)
        assert len(fc["features"]) == 1
        geom = shape(fc["features"][0]["geometry"])
        assert geom.bounds == (10, 9, 12, 11)
