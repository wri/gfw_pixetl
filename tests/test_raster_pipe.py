from typing import Set
from unittest import mock

import pytest

from gfw_pixetl import layers
from gfw_pixetl.decorators import SubprocessKilledError
from gfw_pixetl.grids import LatLngGrid, grid_factory
from gfw_pixetl.models.pydantic import LayerModel, Metadata
from gfw_pixetl.pipes import RasterPipe
from gfw_pixetl.sources import Destination
from gfw_pixetl.tiles import RasterSrcTile
from tests.conftest import minimal_layer_dict

GRID_10 = grid_factory("10/40000")
GRID_1 = grid_factory("1/4000")
EMPTY_METADATA = Metadata(
    extent=(0.0, 0.0, 0.0, 0.0),
    width=0,
    height=0,
    pixelxsize=0.0,
    pixelysize=0.0,
    crs="",
    driver="",
    compression=None,
    bands=list(),
)


def test_create_tiles_subset(PIPE_10x10):
    with mock.patch.object(
        RasterPipe, "get_grid_tiles", return_value=_get_subset_tiles()
    ):
        with mock.patch.object(
            RasterSrcTile, "within", return_value=True
        ), mock.patch.object(
            Destination, "exists", return_value=False
        ), mock.patch.object(
            RasterSrcTile, "transform", return_value=True
        ), mock.patch.object(
            RasterSrcTile, "create_gdal_geotiff", return_value=None
        ), mock.patch.object(
            RasterSrcTile, "upload", return_value=None
        ), mock.patch.object(
            RasterSrcTile, "rm_local_src", return_value=None
        ), mock.patch(
            "gfw_pixetl.utils.upload_geometries.upload_geojsons", return_value=None
        ):
            (
                tiles,
                skipped_tiles,
                failed_tiles,
                existing_tiles,
            ) = PIPE_10x10.create_tiles(overwrite=True)
            assert len(tiles) == 1
            assert len(skipped_tiles) == 3
            assert len(failed_tiles) == 0
            assert len(existing_tiles) == 0


def test_create_tiles_all(LAYER):
    with mock.patch.object(
        RasterPipe, "get_grid_tiles", return_value=_get_subset_tiles()
    ), mock.patch(
        "gfw_pixetl.pipes.pipe.get_metadata", return_value=EMPTY_METADATA
    ), mock.patch.object(
        RasterSrcTile, "within", return_value=True
    ), mock.patch.object(
        Destination, "exists", return_value=False
    ), mock.patch.object(
        RasterSrcTile, "transform", return_value=True
    ), mock.patch.object(
        RasterSrcTile, "create_gdal_geotiff", return_value=None
    ), mock.patch.object(
        RasterSrcTile, "upload", return_value=None
    ), mock.patch.object(
        RasterSrcTile, "rm_local_src", return_value=None
    ), mock.patch(
        "gfw_pixetl.utils.upload_geometries.upload_geojsons", return_value=None
    ):
        pipe = RasterPipe(LAYER)
        (tiles, skipped_tiles, failed_tiles, existing_tiles) = pipe.create_tiles(
            overwrite=False
        )
        assert len(tiles) == 4
        assert len(skipped_tiles) == 0
        assert len(failed_tiles) == 0
        assert len(existing_tiles) == 0


def test_create_tiles_existing(LAYER):
    with mock.patch.object(
        RasterPipe, "get_grid_tiles", return_value=_get_subset_tiles()
    ), mock.patch(
        "gfw_pixetl.pipes.pipe.get_metadata", return_value=EMPTY_METADATA
    ), mock.patch.object(
        RasterSrcTile, "within", return_value=True
    ), mock.patch.object(
        Destination, "exists", return_value=True
    ), mock.patch.object(
        RasterSrcTile, "transform", return_value=True
    ), mock.patch.object(
        RasterSrcTile, "create_gdal_geotiff", return_value=None
    ), mock.patch.object(
        RasterSrcTile, "upload", return_value=None
    ), mock.patch.object(
        RasterSrcTile, "rm_local_src", return_value=None
    ), mock.patch(
        "gfw_pixetl.utils.upload_geometries.upload_geojsons", return_value=None
    ):
        # All exist
        pipe = RasterPipe(LAYER)
        (tiles, skipped_tiles, failed_tiles, existing_tiles) = pipe.create_tiles(
            overwrite=False
        )
        assert len(tiles) == 0
        assert len(skipped_tiles) == 0
        assert len(failed_tiles) == 0
        assert len(existing_tiles) == 4

        # Now do it with overwrite set to True
        pipe = RasterPipe(LAYER)
        (tiles, skipped_tiles, failed_tiles, existing_tiles) = pipe.create_tiles(
            overwrite=True
        )
        assert len(tiles) == 4
        assert len(skipped_tiles) == 0
        assert len(failed_tiles) == 0
        assert len(existing_tiles) == 0


@pytest.mark.skip("Causes test_raster_src_tile.py::test__calc_multi_in to fail")
def test_create_tiles_fail(LAYER):
    with mock.patch.object(
        RasterPipe,
        "get_grid_tiles",
        return_value=[RasterSrcTile(tile_id="10N_011E", grid=GRID_1, layer=LAYER)],
    ), mock.patch(
        "gfw_pixetl.pipes.pipe.get_metadata", return_value=EMPTY_METADATA
    ), mock.patch.object(
        RasterSrcTile, "within", return_value=True
    ), mock.patch.object(
        Destination, "exists", return_value=False
    ), mock.patch.object(
        RasterSrcTile, "create_gdal_geotiff", return_value=None
    ), mock.patch.object(
        RasterSrcTile, "_process_windows", side_effect=Exception
    ), mock.patch.object(
        RasterSrcTile, "rm_local_src", return_value=None
    ), mock.patch(
        "gfw_pixetl.utils.upload_geometries.upload_geojsons", return_value=None
    ):
        pipe = RasterPipe(LAYER)
        (tiles, skipped_tiles, failed_tiles, existing_tiles) = pipe.create_tiles(
            overwrite=False
        )
        assert len(tiles) == 0
        assert len(skipped_tiles) == 0
        assert len(failed_tiles) == 1
        assert len(existing_tiles) == 0
        assert all(tile.status == "failed" for tile in failed_tiles)


@pytest.mark.skip("Causes test_raster_src_tile.py::test__calc_multi_in to fail")
def test_create_tiles_subprocess_oom(LAYER):
    with mock.patch.object(
        RasterPipe,
        "get_grid_tiles",
        return_value=[RasterSrcTile(tile_id="10N_011E", grid=GRID_1, layer=LAYER)],
    ), mock.patch(
        "gfw_pixetl.pipes.pipe.get_metadata", return_value=EMPTY_METADATA
    ), mock.patch.object(
        RasterSrcTile, "within", return_value=True
    ), mock.patch.object(
        Destination, "exists", return_value=False
    ), mock.patch.object(
        RasterSrcTile, "_processified_transform", side_effect=SubprocessKilledError
    ), mock.patch.object(
        RasterSrcTile, "create_gdal_geotiff", return_value=None
    ), mock.patch.object(
        RasterSrcTile, "rm_local_src", return_value=None
    ), mock.patch(
        "gfw_pixetl.utils.upload_geometries.upload_geojsons", return_value=None
    ):
        # First test the sequential code path
        with mock.patch("gfw_pixetl.utils.get_co_workers", return_value=1):
            pipe = RasterPipe(LAYER)
            (tiles, skipped_tiles, failed_tiles, existing_tiles) = pipe.create_tiles(
                overwrite=False
            )
            assert len(tiles) == 0
            assert len(skipped_tiles) == 0
            assert len(failed_tiles) == 1
            assert len(existing_tiles) == 0
            assert all(
                tile.status == "failed - subprocess was killed" for tile in failed_tiles
            )

        # Then the parallel one
        with mock.patch("gfw_pixetl.utils.get_co_workers", return_value=2):
            pipe = RasterPipe(LAYER)
            (tiles, skipped_tiles, failed_tiles, existing_tiles) = pipe.create_tiles(
                overwrite=False
            )
            assert len(tiles) == 0
            assert len(skipped_tiles) == 0
            assert len(failed_tiles) == 1
            assert len(existing_tiles) == 0
            assert all(
                tile.status == "failed - subprocess was killed" for tile in failed_tiles
            )


def test_filter_src_tiles(PIPE_10x10):
    tiles = _get_subset_tiles()

    with mock.patch.object(RasterSrcTile, "within", return_value=False):
        pipe = tiles | PIPE_10x10.filter_src_tiles
        i = 0
        for tile in pipe.results():
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, RasterSrcTile)
        assert i == 0

    with mock.patch.object(RasterSrcTile, "within", return_value=True):
        pipe = tiles | PIPE_10x10.filter_src_tiles
        i = 0
        for tile in pipe.results():
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, RasterSrcTile)
        assert i == 4


def test_transform(PIPE_10x10):
    with mock.patch.object(RasterSrcTile, "transform", return_value=True):
        tiles = PIPE_10x10.transform(_get_subset_tiles())
        i = 0
        for tile in tiles:
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, RasterSrcTile)
        assert i == 4


def _get_subset_tiles() -> Set[RasterSrcTile]:
    layer_dict = {
        **minimal_layer_dict,
        "dataset": "aqueduct_erosion_risk",
        "version": "v201911",
        "pixel_meaning": "level",
        "no_data": 0,
        "grid": "1/4000",
    }
    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))

    assert isinstance(layer, layers.RasterSrcLayer)

    pipe = RasterPipe(layer)

    tiles = set()
    for i in range(10, 12):
        for j in range(10, 12):
            assert isinstance(pipe.grid, LatLngGrid)
            tile_id = pipe.grid.xy_to_tile_id(j, i)
            tiles.add(RasterSrcTile(tile_id=tile_id, grid=pipe.grid, layer=layer))

    return tiles
