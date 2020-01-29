from rasterio.warp import Resampling


def resampling_factory(method: str) -> Resampling:
    if method == "nearest":
        resampling: Resampling = Resampling.nearest
    elif method == "bilinear":
        resampling = Resampling.bilinear
    elif method == "cubic":
        resampling = Resampling.cubic
    elif method == "cubic_spline":
        resampling = Resampling.cubic_spline
    elif method == "lanczos":
        resampling = Resampling.lanczos
    elif method == "average":
        resampling = Resampling.average
    elif method == "mode":
        resampling = Resampling.mode
    elif method == "max":
        resampling = Resampling.max
    elif method == "min":
        resampling = Resampling.min
    elif method == "med":
        resampling = Resampling.med
    elif method == "q1":
        resampling = Resampling.q1
    elif method == "q3":
        resampling = Resampling.q3
    else:
        raise ValueError(f"Method `{method}` is not supported")

    return resampling
