import re
from typing import Optional

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
def cli(
    name: str,
    version: str,
    grid_name: str,
    source_type: str,
    field: Optional[str] = None,
    overwrite: Optional[bool] = True,
) -> None:

    _verify_version_pattern(version)

    grid = grid_factory(grid_name)

    layer = layer_factory(
        source_type, name=name, version=version, grid=grid, field=field
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
        raise ValueError("Version number does not match pattern")


if __name__ == "__main__":
    cli()
