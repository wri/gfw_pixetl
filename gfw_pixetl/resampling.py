from enum import Enum

from rasterio.warp import Resampling

methods = {
    "nearest": Resampling.nearest,
    "bilinear": Resampling.bilinear,
    "cubic": Resampling.cubic,
    "cubic_spline": Resampling.cubic_spline,
    "lanczos": Resampling.lanczos,
    "average": Resampling.average,
    "mode": Resampling.mode,
    "max": Resampling.max,
    "min": Resampling.min,
    "med": Resampling.med,
    "q1": Resampling.q1,
    "q3": Resampling.q3,
}

# Make an enum out of methods dict to allow the Pydantic model to verify against
# ResamplingMethod = Enum("ResamplingMethod", [(k, k) for k, v in methods.items()])


def resampling_factory(method: str) -> Resampling:
    try:
        resampling: Resampling = methods[method]
    except KeyError:
        raise ValueError(f"Method `{method}` is not supported")

    return resampling
