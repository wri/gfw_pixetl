import numpy as np
from rasterio.windows import Window

from gfw_pixetl import layers
from gfw_pixetl.tiles import RasterSrcTile
from gfw_pixetl.tiles.utils.array_utils import calc, set_datatype


def test_set_dtype(LAYER):
    window = Window(0, 0, 10, 10)
    data = np.random.randint(4, size=(10, 10))
    masked_data = np.ma.masked_values(data, 0)
    masked_sum = masked_data.sum()
    masked_pixel_count = masked_data.mask.sum()

    assert masked_sum == masked_data.data.sum()
    assert isinstance(LAYER, layers.RasterSrcLayer)

    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)
    tile.dst[tile.default_format].nodata = 5
    result = set_datatype(
        masked_data,
        window,
        tile.dst[tile.default_format].nodata,
        tile.dst[tile.default_format].dtype,
        tile.tile_id,
    )

    assert masked_sum != result.sum()

    masked_result = np.ma.masked_values(result, 5)
    assert masked_pixel_count == masked_result.mask.sum()
    assert masked_sum == masked_result.sum()


def test_set_dtype_multi(LAYER):
    window = Window(0, 0, 10, 10)

    band1 = np.random.randint(2, size=(10, 10)) + 1
    masked_band1 = np.ma.masked_values(band1, 2)
    masked_sum1 = masked_band1.sum()

    band2 = np.random.randint(2, size=(10, 10)) + 1
    masked_band2 = np.ma.masked_values(band2, 2)
    masked_sum2 = masked_band2.sum()

    band3 = np.random.randint(2, size=(10, 10)) + 1
    masked_band3 = np.ma.masked_values(band3, 2)
    masked_sum3 = masked_band3.sum()

    masked_data = np.ma.array([masked_band1, masked_band2, masked_band3])

    assert isinstance(LAYER, layers.RasterSrcLayer)

    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)
    tile.dst[tile.default_format].nodata = [1, 2, 3]
    result = set_datatype(
        masked_data,
        window,
        tile.dst[tile.default_format].nodata,
        tile.dst[tile.default_format].dtype,
        tile.tile_id,
    )

    print(result)
    assert result[0].sum() == masked_sum1 + (100 - masked_sum1)
    assert result[1].sum() == masked_sum2 + (100 - masked_sum2) * 2
    assert result[2].sum() == masked_sum3 + (100 - masked_sum3) * 3


def test_calc_single(LAYER):
    window = Window(0, 0, 1, 3)
    assert isinstance(LAYER, layers.RasterSrcLayer)
    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)

    tile.layer.calc = "A+1"
    data = np.zeros((1, 1, 3))
    result = calc(
        data,
        window,
        tile.layer.calc,
        tile.dst[tile.default_format].profile["count"],
        tile.tile_id,
    )
    assert result.sum() == 3

    tile.layer.calc = "A+1*5"
    data = np.zeros((1, 1, 3))
    result = calc(
        data,
        window,
        tile.layer.calc,
        tile.dst[tile.default_format].profile["count"],
        tile.tile_id,
    )
    assert result.sum() == 15

    tile.layer.calc = "A*5+1"
    data = np.zeros((1, 1, 3))
    result = calc(
        data,
        window,
        tile.layer.calc,
        tile.dst[tile.default_format].profile["count"],
        tile.tile_id,
    )
    assert result.sum() == 3


def test_calc_multi_in(LAYER):
    window = Window(0, 0, 1, 3)
    assert isinstance(LAYER, layers.RasterSrcLayer)
    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)

    tile.layer.calc = "A+B"
    data = np.ones((2, 1, 3))
    result = calc(
        data,
        window,
        tile.layer.calc,
        tile.dst[tile.default_format].profile["count"],
        tile.tile_id,
    )
    assert result.sum() == 6

    tile.layer.calc = "(A+B)*(C+2)"
    data = np.ones((3, 1, 3))
    result = calc(
        data,
        window,
        tile.layer.calc,
        tile.dst[tile.default_format].profile["count"],
        tile.tile_id,
    )
    assert result.sum() == 18


def test_calc_multi_out(LAYER):
    window = Window(0, 0, 1, 3)

    assert isinstance(LAYER, layers.RasterSrcLayer)

    LAYER.band_count = 3
    tile = RasterSrcTile("10N_010E", LAYER.grid, LAYER)

    tile.layer.calc = "np.ma.array([A,A,A])"
    data = np.ones((1, 1, 3))
    result = calc(
        data,
        window,
        tile.layer.calc,
        tile.dst[tile.default_format].profile["count"],
        tile.tile_id,
    )
    assert result.shape == (3, 1, 3)
    assert result.sum() == 9

    tile.layer.calc = "np.ma.array([A+B,B*5,C+2])"
    data = np.ones((3, 1, 3))
    result = calc(
        data,
        window,
        tile.layer.calc,
        tile.dst[tile.default_format].profile["count"],
        tile.tile_id,
    )
    assert result.shape == (3, 1, 3)
    assert result.sum() == 30
