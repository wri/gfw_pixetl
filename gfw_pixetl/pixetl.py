import os
from typing import List, Optional

import click

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.grids import Grid, grid_factory
from gfw_pixetl.layers import Layer, layer_factory
from gfw_pixetl.tiles import Tile
from gfw_pixetl.logo import logo
from gfw_pixetl.pipes import Pipe, pipe_factory

LOGGER = get_module_logger(__name__)


@click.command()
@click.argument("name", type=str)
@click.option("-v", "--version", type=str, help="Version of dataset")
@click.option(
    "-s",
    "--source_type",
    type=click.Choice(["raster", "vector", "tcd_raster"]),
    help="Type of input file(s)",
)
@click.option("-f", "--field", type=str, help="Field represented in output dataset")
@click.option(
    "-g",
    "--grid_name",
    type=click.Choice(["10/40000", "90/27008"]),
    default="10/40000",
    help="Grid size of output dataset",
)
@click.option(
    "--subset", type=str, default=None, multiple=True, help="Subset of tiles to process"
)
@click.option(
    "-o",
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing tile in output location",
)
def cli(
    name: str,
    version: str,
    source_type: str,
    field: str,
    grid_name: str,
    subset: Optional[List[str]],
    overwrite: bool,
):
    """NAME: Name of dataset"""

    pixetl(
        name, version, source_type, field, grid_name, subset, overwrite,
    )


def pixetl(
    name: str,
    version: str,
    source_type: str,
    field: str,
    grid_name: str = "10/40000",
    subset: Optional[List[str]] = None,
    overwrite: bool = True,
) -> List[Tile]:
    click.echo(logo)

    LOGGER.info(
        "Start tile prepartion for Layer {name}, Version {version}, grid {grid_name}, source type {source_type}, field {field} with overwrite set to {overwrite}.".format(
            name=name,
            version=version,
            grid_name=grid_name,
            source_type=source_type,
            field=field,
            overwrite=overwrite,
        )
    )

    old_cwd = os.getcwd()
    cwd = utils.set_cwd()

    # set available memory here before any major process is running
    utils.set_available_memory()

    try:

        if subset:
            LOGGER.info("Running on subset: {}".format(subset))
        else:
            LOGGER.info("Running on full extent")

        if not utils.verify_version_pattern(version):
            message = "Version number does not match pattern"
            LOGGER.error(message)
            raise ValueError(message)

        grid: Grid = grid_factory(grid_name)
        layer: Layer = layer_factory(name=name, version=version, grid=grid, field=field)

        pipe: Pipe = pipe_factory(layer, subset)

        return pipe.create_tiles(overwrite)

    except Exception as e:
        utils.remove_work_directory(old_cwd, cwd)
        LOGGER.exception(e)
        raise
    finally:
        utils.remove_work_directory(old_cwd, cwd)


if __name__ == "__main__":
    cli()
