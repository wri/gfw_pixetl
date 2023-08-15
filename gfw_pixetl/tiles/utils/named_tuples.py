from typing import Any, NamedTuple, Optional

from rasterio.vrt import WarpedVRT


class Destination(NamedTuple):
    transform: Any
    crs: Any
    count: Any
    no_data: Any
    datatype: Any
    profile: Any
    tmp_dir: Any
    uri: Any
    write_to_separate_files: bool


class Source(NamedTuple):
    vrt: WarpedVRT
    crs: Any


class Layer(NamedTuple):
    input_bands: Any
    calc_string: Optional[str]
