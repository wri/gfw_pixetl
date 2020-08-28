#!/usr/bin/env python

import json
import os
import sys
from typing import List, Optional, Tuple

import click

from gfw_pixetl import get_module_logger, utils
from gfw_pixetl.data_type import dtypes_dict as data_types
from gfw_pixetl.layers import Layer, layer_factory
from gfw_pixetl.logo import logo
from gfw_pixetl.models import LayerModel
from gfw_pixetl.pipes import Pipe, pipe_factory
from gfw_pixetl.resampling import methods as resampling_methods
from gfw_pixetl.tiles import Tile

LOGGER = get_module_logger(__name__)


@click.command()
@click.option(
    "-d", "--dataset", type=str, required=True, help="Name of dataset to process"
)
@click.option(
    "-v", "--version", type=str, required=True, help="Version of dataset to process"
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
@click.argument("layer_json", type=str)
def cli(
    dataset: str,
    version: str,
    subset: Optional[List[str]],
    overwrite: bool,
    layer_json: str,
):
    # Validate fields sooner rather than later
    # On the other hand, moving this validation inside pixetl() would allow
    # for easier testing...
    if not utils.verify_version_pattern(version):
        message = "Invalid version string"
        LOGGER.error(message)
        raise ValueError(message)

    layer_dict = json.loads(layer_json)
    layer_dict.update({"dataset": dataset, "version": version})
    layer_def = LayerModel.parse_obj(layer_dict)

    # Validate resampling_method values. I tried to do this with an enum
    # (see commented-out lines in models.py and resampling.py) but couldn't
    # get it to work
    if layer_def.resampling not in resampling_methods.keys():
        raise ValueError(f"Invalid resampling method specified: {layer_def.resampling}")

    # Validate data_type values. Ideally turn this into an enum for Pydantic
    # to validate automatically.
    if layer_def.data_type not in data_types.keys():
        raise ValueError(f"Invalid data_type specified: {layer_def.data_type}")

    # Raster sources must have an source URI
    if layer_def.source_type == "raster" and layer_def.source_uri is None:
        raise ValueError("URI specification is required for raster sources")

    # Finally, actually process the layer
    tiles, skipped_tiles, failed_tiles = pixetl(
        layer_def,
        subset,
        overwrite,
    )

    nb_tiles = len(tiles)
    nb_skipped_tiles = len(skipped_tiles)
    nb_failed_tiles = len(failed_tiles)

    LOGGER.info(f"Successfully processed {len(tiles)} tiles")
    LOGGER.info(f"{nb_skipped_tiles} tiles skipped.")
    LOGGER.info(f"{nb_failed_tiles} tiles failed.")
    if nb_tiles:
        LOGGER.info(f"Processed tiles: {tiles}")
    if nb_skipped_tiles:
        LOGGER.info(f"Skipped tiles: {skipped_tiles}")
    if nb_failed_tiles:
        LOGGER.info(f"Failed tiles: {failed_tiles}")
        sys.exit("Program terminated with Errors. Some tiles failed to process")


def pixetl(
    layer_def: LayerModel,
    subset: Optional[List[str]] = None,
    overwrite: bool = False,
) -> Tuple[List[Tile], List[Tile], List[Tile]]:
    click.echo(logo)

    LOGGER.info(
        f"Start tile preparation for dataset {layer_def.dataset}, "
        f"version {layer_def.version}, grid {layer_def.grid}, "
        f"source type {layer_def.source_type}, field {layer_def.pixel_meaning}, "
        f"with overwrite set to {overwrite}."
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

        layer: Layer = layer_factory(layer_def)

        pipe: Pipe = pipe_factory(layer, subset)

        tiles, skipped_tiles, failed_tiles = pipe.create_tiles(overwrite)
        utils.remove_work_directory(old_cwd, cwd)

        return tiles, skipped_tiles, failed_tiles

    except Exception as e:
        utils.remove_work_directory(old_cwd, cwd)
        LOGGER.exception(e)
        raise


if __name__ == "__main__":
    cli()
