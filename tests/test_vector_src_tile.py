import json
import os
import subprocess

from sqlalchemy.ext.declarative import declarative_base

from gfw_pixetl.grids import LatLngGrid, grid_factory
from gfw_pixetl.layers import VectorSrcLayer, layer_factory
from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.tiles import VectorSrcTile

Base = declarative_base()

base_vector_layer_dict = {
    "grid": "10/40000",
    "pixel_meaning": "is",
    "source_type": "vector",
    "no_data": None,
    "data_type": "uint8",
    "calc": "1"
}


def test_vector_src_tile_intersects_data(sample_vector_data):
    dataset, version = sample_vector_data
    layer: VectorSrcLayer = layer_factory(
        LayerModel.parse_obj(
            base_vector_layer_dict | {"dataset": dataset, "version": version}
        )
    )

    assert isinstance(layer, VectorSrcLayer)

    tile: VectorSrcTile = VectorSrcTile("60N_010E", layer.grid, layer)
    assert tile.src_vector_intersects()


def test_vector_src_tile_intersects_surrounding_tiles(sample_vector_data):
    dataset, version = sample_vector_data
    layer: VectorSrcLayer = layer_factory(
        LayerModel.parse_obj(
            base_vector_layer_dict | {"dataset": dataset, "version": version}
        )
    )

    for tile_id in [
        "70N_000E",
        "70N_010E",
        "70N_020E",  # NOQA
        "60N_000E",
        "60N_020E",  # NOQA
        "50N_000E",
        "50N_010E",
        "50N_020E",  # NOQA
    ]:
        tile: VectorSrcTile = VectorSrcTile(tile_id, layer.grid, layer)
        assert not tile.src_vector_intersects()


def test_vector_src_tile_fetch_data_creates_parquet(sample_vector_data):
    dataset, version = sample_vector_data
    layer: VectorSrcLayer = layer_factory(
        LayerModel.parse_obj(
            base_vector_layer_dict | {"dataset": dataset, "version": version}
        )
    )

    tile: VectorSrcTile = VectorSrcTile("60N_010E", layer.grid, layer)

    parquet_path = os.path.join(tile.work_dir, f"{tile.tile_id}.parquet")
    tile.remove_work_dir()
    assert not os.path.isfile(parquet_path)

    tile.fetch_data()

    assert os.path.isfile(parquet_path)


def test_vector_src_tile_rasterize_creates_tiff(sample_vector_data):
    grid_name: str = "1/4000"
    some_grid: LatLngGrid = grid_factory(grid_name)

    dataset, version = sample_vector_data
    layer: VectorSrcLayer = layer_factory(
        LayerModel.parse_obj(
            base_vector_layer_dict
            | {"dataset": dataset, "version": version, "grid": grid_name}
        )
    )

    tile = VectorSrcTile("54N_010E", some_grid, layer)
    assert tile.src_vector_intersects()

    tiff_path = tile.get_local_dst_uri(tile.default_format)
    tile.remove_work_dir()
    assert not os.path.isfile(tiff_path)

    tile.fetch_data()
    tile.rasterize()

    assert os.path.isfile(tiff_path)


def test_vector_src_tile_rasterize_tiff_has_data(sample_vector_data):
    grid_name: str = "1/4000"
    some_grid: LatLngGrid = grid_factory(grid_name)

    dataset, version = sample_vector_data
    layer: VectorSrcLayer = layer_factory(
        LayerModel.parse_obj(
            base_vector_layer_dict
            | {"dataset": dataset, "version": version, "grid": grid_name}
        )
    )

    tile = VectorSrcTile("54N_010E", some_grid, layer)
    assert tile.src_vector_intersects()

    tiff_path = tile.get_local_dst_uri(tile.default_format)
    tile.remove_work_dir()
    assert not os.path.isfile(tiff_path)

    tile.fetch_data()
    tile.rasterize()

    assert os.path.isfile(tiff_path)

    proc_args = [
        "gdalinfo",
        "-stats",
        "-json",
        tiff_path
    ]
    p = subprocess.run(proc_args, capture_output=True, check=True)
    output = p.stdout.decode("utf-8")
    info = json.loads(output)
    assert info["bands"][0]["metadata"][""]["STATISTICS_MINIMUM"] == "0"
    assert info["bands"][0]["metadata"][""]["STATISTICS_MAXIMUM"] == "1"
