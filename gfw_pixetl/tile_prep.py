import logging
import re
from typing import Optional

import click

from gfw_pixetl import get_module_logger
from gfw_pixetl.grid import grid_factory
from gfw_pixetl.layer import layer_factory
from gfw_pixetl.logo import logo

logger = get_module_logger(__name__)


@click.command()
@click.option("--name")
@click.option("--version")
@click.option("--source_type")
@click.option("--field", default=None)
@click.option("--grid_name", default="10x10")
@click.option("--env", default="dev")
@click.option("--overwrite", is_flag=True, default=False)
@click.option("--debug", is_flag=True, default=False)
def cli(
    name: str,
    version: str,
    grid_name: str,
    source_type: str,
    field: Optional[str],
    env: str,
    overwrite: bool,
    debug: bool,
) -> None:

    if debug:
        logger.setLevel(logging.DEBUG)

    print(logo)

    logger.info(
        "Start tile prepartion for Layer {name}, Version {version}, grid {grid_name}, source type {source_type}, field {field} with overwrite set to {overwrite}".format(
            name=name,
            version=version,
            grid_name=grid_name,
            source_type=source_type,
            field=field,
            overwrite=overwrite,
        )
    )

    _verify_version_pattern(version)

    grid = grid_factory(grid_name)

    layer = layer_factory(
        source_type, name=name, version=version, grid=grid, field=field, env=env
    )

    layer.create_tiles(overwrite)


def _verify_version_pattern(version: str) -> None:
    """
    Verify if version matches general pattern
    - Must start with a v
    - Followed by up to three groups of digits seperated with a .
    - First group can have up to 8 digits
    - Second and third group up to 3 digits

    Examples:
    - v20191001
    - v1.1.2
    """
    p = re.compile(r"^v\d{,8}\.?\d{,3}\.?\d{,3}$")
    m = p.match(version)
    if not m:
        message = "Version number does not match pattern"
        logger.error(message)
        raise ValueError(message)


if __name__ == "__main__":
    cli()
