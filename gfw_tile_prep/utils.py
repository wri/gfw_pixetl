import re
import argparse


def get_top(coord):
    if coord >= 0:
        return f"{coord:02}" + "N"
    else:
        return f"{-coord:02}" + "S"


def get_left(coord):
    if coord >= 0:
        return f"{coord:003}" + "E"
    else:
        return f"{-coord:003}" + "W"


def get_tile_id(f):
    """
    Finds and returns tile id in file name
    Tile id must match the following pattern
    050W_20S_030E_10N

    :param f: File name
    :return: Tile ID or None
    """
    m = re.search("([0-9]{2}[NS]_[0-9]{3}[EW])", f)
    if m:
        return m.group(0)
    else:
        return None


def str2bool(v):
    """
    Convert various strings to boolean
    :param v: String
    :return: Boolean
    """
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")
