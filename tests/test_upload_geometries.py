from geojson import FeatureCollection

from gfw_pixetl.utils.upload_geometries import _merge_feature_collections
from tests.conftest import BUCKET, GEOJSON_NAME


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
