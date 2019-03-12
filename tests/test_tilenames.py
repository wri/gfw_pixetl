from gfw_tile_prep.utils import get_top, get_left, get_tile_id


def test_get_left_east():
    assert get_left(0) == "000E"
    assert get_left(1) == "001E"
    assert get_left(10) == "010E"
    assert get_left(100) == "100E"


def test_get_left_west():
    assert get_left(-1) == "001W"
    assert get_left(-10) == "010W"
    assert get_left(-100) == "100W"


def test_get_top_north():
    assert get_top(0) == "00N"
    assert get_top(1) == "01N"
    assert get_top(10) == "10N"


def test_get_top_south():
    assert get_top(-1) == "01S"
    assert get_top(-10) == "10S"


def test_get_tile_id():
    assert get_tile_id("myfile_10N_010E.tif") == "10N_010E"
    assert get_tile_id("10N_010E_myfile.tif") == "10N_010E"
    assert get_tile_id("10N_010E.tif") == "10N_010E"

    assert get_tile_id("myfile_10S_010W.tif") == "10S_010W"
    assert get_tile_id("10S_010W_myfile.tif") == "10S_010W"
    assert get_tile_id("10S_010W.tif") == "10S_010W"
