#!/usr/bin/env python

import os
import sys
from typing import List, Optional, Tuple, Union

import click
import typer
from typer import Argument, Option

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import Layer, layer_factory, layer_model_factory
from gfw_pixetl.logo import logo
from gfw_pixetl.models.pydantic import (
    RasterLayerCliModel,
    RasterLayerModel,
    VectorLayerCliModel,
    VectorLayerModel,
)
from gfw_pixetl.pipes import Pipe, pipe_factory
from gfw_pixetl.settings.gdal import (  # noqa: F401, import vars to assure they are initialize right in the beginning
    GDAL_ENV,
)
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.cwd import remove_work_directory, set_cwd

LOGGER = get_module_logger(__name__)


def cli(
    dataset: str = Option(..., help="Name of dataset to process"),
    version: str = Option(..., help="Version of dataset to process"),
    subset: Optional[List[str]] = Option(None, help="Subset of tiles to process"),
    overwrite: bool = Option(False, help="Overwrite existing tile in output location"),
    layer_cli_model: Union[RasterLayerCliModel, VectorLayerCliModel] = Argument(...),
):
    layer_model = layer_model_factory(dataset, version, layer_cli_model)

    # Finally, actually process the layer
    tiles, skipped_tiles, failed_tiles, existing_tiles = pixetl(
        layer_model,
        subset,
        overwrite,
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
    if nb_skipped_tiles:
        LOGGER.info(f"Skipped tiles: {skipped_tiles}")
    if nb_existing_tiles:
        LOGGER.info(f"Existing tiles: {existing_tiles}")
    if nb_failed_tiles:
        LOGGER.info(f"Failed tiles: {failed_tiles}")
        sys.exit("Program terminated with Errors. Some tiles failed to process")


def pixetl(
    layer_model: Union[RasterLayerModel, VectorLayerModel],
    subset: Optional[List[str]] = None,
    overwrite: bool = False,
) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
    click.echo(logo)

    LOGGER.info(
        f"Start tile preparation for dataset {layer_model.dataset}, "
        f"version {layer_model.version}, grid {layer_model.grid}, "
        f"source type {layer_model.source_type}, field {layer_model.pixel_meaning}, "
        f"with overwrite set to {overwrite}."
    )

    old_cwd = os.getcwd()
    cwd = set_cwd()

    # set available memory here before any major process is running
    # utils.set_available_memory()

    try:
        if subset:
            LOGGER.info("Running on subset: {}".format(subset))
        else:
            LOGGER.info("Running on full extent")

        layer: Layer = layer_factory(layer_model)

        pipe: Pipe = pipe_factory(layer, subset)

        tiles, skipped_tiles, failed_tiles, existing_tiles = pipe.create_tiles(
            overwrite
        )
        remove_work_directory(old_cwd, cwd)

        return tiles, skipped_tiles, failed_tiles, existing_tiles

    except Exception as e:
        remove_work_directory(old_cwd, cwd)
        LOGGER.exception(e)
        raise


if __name__ == "__main__":
    typer.run(cli)
