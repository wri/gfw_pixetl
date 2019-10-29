import click

from gfw_tile_prep.grid_factory import grid_factory
from gfw_tile_prep.layer_factory import layer_factory


@click.command()
@click.option("--name")
@click.option("--version")
@click.option("--grid_name")
@click.option("--source_type")
@click.option("--field")
@click.option("--overwrite")
def cli(name, version, grid_name, source_type, field=None, overwrite=True):

    grid = grid_factory(grid_name)

    layer = layer_factory(
        source_type, name=name, version=version, grid=grid, field=field
    )

    layer.create_tiles(overwrite)


if __name__ == "__main__":
    cli()
