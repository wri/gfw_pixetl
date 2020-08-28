from typing import List, Optional

from gfw_pixetl.layers import Layer, RasterSrcLayer, VectorSrcLayer
from gfw_pixetl.pipes import Pipe, RasterPipe, VectorPipe


def pipe_factory(layer: Layer, subset: Optional[List[str]] = None) -> Pipe:
    if isinstance(layer, VectorSrcLayer):
        pipe: Pipe = VectorPipe(layer, subset)
    elif isinstance(layer, RasterSrcLayer):
        pipe = RasterPipe(layer, subset)
    else:
        raise ValueError("Unknown layer type")

    return pipe
