import os
from typing import Any, Dict, Set
from unittest import mock

from gfw_pixetl import layers
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.pipes import RasterPipe, Pipe
from gfw_pixetl.tiles import RasterSrcTile
from gfw_pixetl.sources import Destination

os.environ["ENV"] = "test"

GRID_10 = grid_factory("10/40000")
GRID_1 = grid_factory("1/4000")

RASTER_LAYER: Dict[str, Any] = {
    "name": "aqueduct_erosion_risk",
    "version": "v201911",
    "field": "level",
    "grid": GRID_10,
}

LAYER_TYPE = layers._get_source_type(
    RASTER_LAYER["name"], RASTER_LAYER["field"], RASTER_LAYER["grid"].name
)

LAYER = layers.layer_factory(**RASTER_LAYER)
SUBSET = ["10N_010E", "20N_010E", "30N_010E"]
PIPE = RasterPipe(LAYER, SUBSET)


def test_create_tiles_subset():
    """
    THIS TEST NEEDS TO BE REFACTORED. SHAME ON ME!
    """
    with mock.patch.object(
        RasterPipe, "get_grid_tiles", return_value=_get_subset_tiles()
    ):
        with mock.patch.object(RasterSrcTile, "within", return_value=True):
            with mock.patch.object(Destination, "exists", return_value=False):
                with mock.patch.object(RasterSrcTile, "transform", return_value=True):
                    with mock.patch.object(
                        RasterSrcTile, "create_gdal_geotiff", return_value=None
                    ):
                        with mock.patch.object(
                            RasterSrcTile, "upload", return_value=None
                        ):
                            with mock.patch.object(
                                RasterSrcTile, "rm_local_src", return_value=None
                            ):
                                with mock.patch(
                                    "gfw_pixetl.utils.upload_geometries.upload_vrt",
                                    return_value=None,
                                ):
                                    with mock.patch(
                                        "gfw_pixetl.utils.upload_geometries.upload_geom",
                                        return_value=None,
                                    ):
                                        with mock.patch(
                                            "gfw_pixetl.utils.upload_geometries.upload_tile_geoms",
                                            return_value=None,
                                        ):
                                            (
                                                tiles,
                                                skipped_tiles,
                                                failed_tiles,
                                            ) = PIPE.create_tiles(overwrite=True)
                                            assert len(tiles) == 1
                                            assert len(skipped_tiles) == 3
                                            assert len(failed_tiles) == 0


def test_create_tiles_all():
    """
    THIS TEST NEEDS TO BE REFACTORED. SHAME ON ME!
    """
    pipe = RasterPipe(LAYER)
    with mock.patch.object(
        RasterPipe, "get_grid_tiles", return_value=_get_subset_tiles()
    ):
        with mock.patch.object(RasterSrcTile, "within", return_value=True):
            with mock.patch.object(Destination, "exists", return_value=False):
                with mock.patch.object(RasterSrcTile, "transform", return_value=True):
                    with mock.patch.object(
                        RasterSrcTile, "create_gdal_geotiff", return_value=None
                    ):
                        with mock.patch.object(
                            RasterSrcTile, "upload", return_value=None
                        ):
                            with mock.patch.object(
                                RasterSrcTile, "rm_local_src", return_value=None
                            ):
                                with mock.patch(
                                    "gfw_pixetl.utils.upload_geometries.upload_vrt",
                                    return_value=None,
                                ):
                                    with mock.patch(
                                        "gfw_pixetl.utils.upload_geometries.upload_geom",
                                        return_value=None,
                                    ):
                                        with mock.patch(
                                            "gfw_pixetl.utils.upload_geometries.upload_tile_geoms",
                                            return_value=None,
                                        ):

                                            (
                                                tiles,
                                                skipped_tiles,
                                                failed_tiles,
                                            ) = pipe.create_tiles(overwrite=True)
                                            assert len(tiles) == 4
                                            assert len(skipped_tiles) == 0
                                            assert len(failed_tiles) == 0


def test_filter_src_tiles():
    tiles = _get_subset_tiles()

    with mock.patch.object(RasterSrcTile, "within", return_value=False):
        pipe = tiles | PIPE.filter_src_tiles
        i = 0
        for tile in pipe.results():
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, RasterSrcTile)
        assert i == 0

    with mock.patch.object(RasterSrcTile, "within", return_value=True):
        pipe = tiles | PIPE.filter_src_tiles
        i = 0
        for tile in pipe.results():
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, RasterSrcTile)
        assert i == 4


def test_transform():
    with mock.patch.object(RasterSrcTile, "transform", return_value=True):
        tiles = PIPE.transform(_get_subset_tiles())
        i = 0
        for tile in tiles:
            if tile.status == "pending":
                i += 1
                assert isinstance(tile, RasterSrcTile)
        assert i == 4


def _get_subset_tiles() -> Set[RasterSrcTile]:
    raster_layer: Dict[str, Any] = {
        "name": "aqueduct_erosion_risk",
        "version": "v201911",
        "field": "level",
        "grid": GRID_1,
    }

    layer = layers.layer_factory(**raster_layer)

    assert isinstance(layer, layers.RasterSrcLayer)

    pipe = RasterPipe(layer)

    tiles = set()
    for i in range(10, 12):
        for j in range(10, 12):
            origin = pipe.grid.xy_grid_origin(j, i)
            tiles.add(RasterSrcTile(origin=origin, grid=pipe.grid, layer=layer))

    return tiles
