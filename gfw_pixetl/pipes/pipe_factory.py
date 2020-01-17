from typing import List, Optional

from gfw_pixetl.layers import RasterSrcLayer, VectorSrcLayer, Layer
from gfw_pixetl.pipes import Pipe, VectorPipe, RasterPipe, CalcRasterPipe


def pipe_factory(
    layer: Layer, subset: Optional[List[str]] = None, divisor: int = 2
) -> Pipe:
    if isinstance(layer, VectorSrcLayer):
        pipe: Pipe = VectorPipe(layer, subset, divisor)
    elif isinstance(layer, RasterSrcLayer) and layer.calc is not None:
        pipe = CalcRasterPipe(layer, subset, divisor)
    elif isinstance(layer, RasterSrcLayer):
        pipe = RasterPipe(layer, subset, divisor)
    else:
        raise ValueError("Unknown layer type")

    return pipe
