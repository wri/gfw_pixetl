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


def resampling_factory(method: str) -> Resampling:
    try:
        resampling: Resampling = methods[method]
    except KeyError:
        raise ValueError(f"Method `{method}` is not supported")

    return resampling
