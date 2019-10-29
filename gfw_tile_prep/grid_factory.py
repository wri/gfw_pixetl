from gfw_tile_prep.grid import Grid


def grid_factory(grid_name) -> Grid:
    """
    Different Grid layout used for this project
    """

    # RAAD alerts
    if grid_name == "epsg_4326_3x3" or grid_name == "3x3":
        return Grid("epsg:4326", 3, 50000, 500)

    # GLAD alerts and UMD Forest Loss
    elif grid_name == "epsg_4326_10x10" or grid_name == "10x10":
        return Grid("epsg:4326", 10, 40000, 400)

    # VIIRS Fire alerts
    elif grid_name == "epsg_4326_30x30" or grid_name == "30x30":
        return Grid("epsg:4326", 30, 9000, 450)

    # MODIS Fire alerts
    elif grid_name == "epsg_4326_90x90" or grid_name == "90x90":
        return Grid("epsg:4326", 90, 10000, 500)
