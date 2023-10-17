import os
import subprocess

import pytest
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import text

from gfw_pixetl import layers
from gfw_pixetl.layers import VectorSrcLayer
from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.settings.globals import GLOBALS
from gfw_pixetl.tiles import VectorSrcTile

Base = declarative_base()

base_vector_layer_dict = {
    "dataset": "public",
    "version": "v4",
    "grid": "10/40000",
    "pixel_meaning": "gfw_fid",
    "source_type": "vector",
    "no_data": 0,
    "data_type": "uint32",
}


@pytest.fixture(scope="module")
def rw_db():
    proc_args = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        f"PG:password={GLOBALS.db_password} host={GLOBALS.db_host} port={GLOBALS.db_port} dbname={GLOBALS.db_name} user={GLOBALS.db_username}",
        os.path.join(os.path.dirname(__file__), "fixtures", "sample_data.csv"),
        "-nln",
        "public.v4",
        "-t_srs",
        "EPSG:4326",
        "-s_srs",
        "EPSG:4326",
    ]
    p = subprocess.run(proc_args, capture_output=True, check=True)
    assert p.stderr == b""

    yield

    db_url = URL(
        "postgresql+psycopg2",
        host=GLOBALS.db_host,
        port=GLOBALS.db_port,
        username=GLOBALS.db_username,
        password=GLOBALS.db_password,
        database=GLOBALS.db_name,
    )

    sql = text("DROP TABLE IF EXISTS public.v4;")

    with create_engine(db_url).begin() as conn:
        conn.execute(sql)


def test_vector_src_tile_intersects_data(rw_db):
    layer_dict = {**base_vector_layer_dict}

    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))
    assert isinstance(layer, layers.VectorSrcLayer)

    tile: VectorSrcTile = VectorSrcTile("60N_010E", layer.grid, layer)
    assert tile.src_vector_intersects()


def test_vector_src_tile_intersects_surrounding_tiles(rw_db):
    layer_dict = {**base_vector_layer_dict}

    layer: VectorSrcLayer = layers.layer_factory(LayerModel.parse_obj(layer_dict))

    for tile_id in [
        "70N_000E", "70N_010E", "70N_020E",
        "60N_000E",             "60N_020E",
        "50N_000E", "50N_010E", "50N_020E"
    ]:
        tile: VectorSrcTile = VectorSrcTile(tile_id, layer.grid, layer)
        assert not tile.src_vector_intersects()


def test_vector_src_tile_fetch_data_creates_csv(rw_db):
    layer_dict = {**base_vector_layer_dict}

    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))
    tile: VectorSrcTile = VectorSrcTile("60N_010E", layer.grid, layer)

    csv_path = os.path.join(tile.work_dir, f"{tile.tile_id}.csv")
    tile.remove_work_dir()
    assert not os.path.isfile(csv_path)

    tile.fetch_data()

    assert os.path.isfile(csv_path)


def test_vector_src_tile_rasterize_creates_tiff(rw_db):
    layer_dict = {**base_vector_layer_dict}

    layer = layers.layer_factory(LayerModel.parse_obj(layer_dict))
    tile: VectorSrcTile = VectorSrcTile("60N_010E", layer.grid, layer)

    tiff_path = tile.get_local_dst_uri(tile.default_format)
    tile.remove_work_dir()
    assert not os.path.isfile(tiff_path)

    tile.fetch_data()
    tile.rasterize()

    assert os.path.isfile(tiff_path)
