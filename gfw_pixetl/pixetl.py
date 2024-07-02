#!/usr/bin/env python

import json
import os
import sys
from typing import List, Optional, Tuple

import click

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import Layer, layer_factory
from gfw_pixetl.logo import logo
from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.pipes import Pipe, pipe_factory
from gfw_pixetl.settings.gdal import (  # noqa: F401, import vars to assure they are initialize right in the beginning
    GDAL_ENV,
)
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.cwd import remove_work_directory, set_cwd

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
@click.option(
    "--skip_upload", is_flag=True, default=False, help="Do not upload output to S3"
)
@click.option(
    "--skip_deletion",
    is_flag=True,
    default=False,
    multiple=False,
    help="Do not delete local output on completion",
)
@click.argument("layer_json", type=str)
def cli(
    dataset: str,
    version: str,
    subset: Optional[List[str]],
    overwrite: bool,
    layer_json: str,
    skip_upload: bool,
    skip_deletion: bool,
):
    layer_dict = json.loads(layer_json)
    layer_dict.update({"dataset": dataset, "version": version})
    layer_def = LayerModel.parse_obj(layer_dict)

    # Raster sources must have a source URI
    if layer_def.source_type == "raster" and layer_def.source_uri is None:
        raise ValueError("URI specification is required for raster sources")

    # Finally, actually process the layer
    tiles, skipped_tiles, failed_tiles, existing_tiles = pixetl(
        layer_def, subset, overwrite, skip_upload, skip_deletion
    )

    nb_tiles = len(tiles)
    nb_skipped_tiles = len(skipped_tiles)
    nb_failed_tiles = len(failed_tiles)
    nb_existing_tiles = len(existing_tiles)

    LOGGER.info(f"Successfully processed {len(tiles)} tiles")
    LOGGER.info(f"{nb_skipped_tiles} tiles skipped.")
    LOGGER.info(f"{nb_existing_tiles} tiles already existed.")
    LOGGER.info(f"{nb_failed_tiles} tiles failed.")
    if nb_tiles:
        LOGGER.info(f"Processed tiles: {tiles}")
    if nb_existing_tiles:
        LOGGER.info(f"Existing tiles: {existing_tiles}")
    if nb_failed_tiles:
        LOGGER.info(f"Failed tiles: {failed_tiles}")
        if any(
            tile.status == "failed - subprocess was killed" for tile in failed_tiles
        ):
            LOGGER.info(
                "Detected involuntarily terminated subprocesses, exiting with code 137"
            )
            sys.exit(137)
        else:
            LOGGER.info("Program terminated with errors. Some tiles failed to process")
            sys.exit(1)


def pixetl(
    layer_def: LayerModel,
    subset: Optional[List[str]] = None,
    overwrite: bool = False,
    skip_upload: bool = False,
    skip_deletion: bool = False,
) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
    click.echo(logo)

    LOGGER.info(
        f"Start tile preparation for dataset {layer_def.dataset}, "
        f"version {layer_def.version}, grid {layer_def.grid}, "
        f"source type {layer_def.source_type}, field {layer_def.pixel_meaning}, "
        f"with overwrite set to {overwrite}, "
        f"skip_upload set to {skip_upload}. "
        f"and skip_deletion set to {skip_deletion}."
    )

    LOGGER.debug(f"Full layer_def: {layer_def.json()}")

    old_cwd = os.getcwd()
    cwd = set_cwd()

    # set available memory here before any major process is running
    # utils.set_available_memory()

    try:
        if subset:
            LOGGER.info("Running on subset: {}".format(subset))
        else:
            LOGGER.info("Running on full extent")

        layer: Layer = layer_factory(layer_def)

        pipe: Pipe = pipe_factory(layer, subset)

        tiles, skipped_tiles, failed_tiles, existing_tiles = pipe.create_tiles(
            overwrite, remove_work=not skip_deletion, upload=not skip_upload
        )
        os.chdir(old_cwd)
        return tiles, skipped_tiles, failed_tiles, existing_tiles

    except Exception as e:
        os.chdir(old_cwd)
        LOGGER.exception(e)
        raise
    finally:
        if not skip_deletion:
            remove_work_directory(old_cwd, cwd)


if __name__ == "__main__":
    cli()
