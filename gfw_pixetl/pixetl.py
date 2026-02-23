#!/usr/bin/env python

import json
import multiprocessing as mp
import os
import sys
from logging import getLogger
from typing import List, Optional, Tuple

import click

from gfw_pixetl import get_module_logger
from gfw_pixetl.layers import Layer, layer_factory
from gfw_pixetl.logo import logo
from gfw_pixetl.logs import setup_logging
from gfw_pixetl.models.pydantic import LayerModel
from gfw_pixetl.pipes import Pipe, pipe_factory
from gfw_pixetl.settings.gdal import (  # noqa: F401, import vars to assure they are initialize right in the beginning
    GDAL_ENV,
)
from gfw_pixetl.telemetry import ReporterConfig, telemetry_process_main
from gfw_pixetl.tiles import Tile
from gfw_pixetl.utils.cwd import remove_work_directory, set_cwd

_qh = setup_logging("INFO")  # configure logging immediately for the main proc


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
    LOGGER = get_module_logger(__name__)

    layer_dict = json.loads(layer_json)
    layer_dict.update({"dataset": dataset, "version": version})
    layer_def = LayerModel.parse_obj(layer_dict)

    # Raster sources must have a source URI
    if layer_def.source_type == "raster" and layer_def.source_uri is None:
        raise ValueError("URI specification is required for raster sources")

    # Process the layer
    tiles, skipped_tiles, failed_tiles, existing_tiles = pixetl(
        layer_def,
        subset,
        overwrite,
    )

    nb_tiles = len(tiles)
    nb_skipped_tiles = len(skipped_tiles)
    nb_failed_tiles = len(failed_tiles)
    nb_existing_tiles = len(existing_tiles)

    LOGGER.info(f"Successfully processed {nb_tiles} tiles")
    LOGGER.info(f"{nb_skipped_tiles} tiles skipped.")
    LOGGER.info(f"{nb_existing_tiles} tiles already existed.")
    LOGGER.info(f"{nb_failed_tiles} tiles failed.")
    # if nb_tiles:
    #     LOGGER.info(f"Processed tiles: {tiles}")
    # if nb_existing_tiles:
    #     LOGGER.info(f"Existing tiles: {existing_tiles}")
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
) -> Tuple[List[Tile], List[Tile], List[Tile], List[Tile]]:
    click.echo(logo)

    LOGGER = get_module_logger(__name__)

    LOGGER.info(
        f"Start tile preparation for dataset {layer_def.dataset}, "
        f"version {layer_def.version}, grid {layer_def.grid}, "
        f"source type {layer_def.source_type}, field {layer_def.pixel_meaning}, "
        f"with overwrite set to {overwrite}."
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
            overwrite
        )
        remove_work_directory(old_cwd, cwd)

        return tiles, skipped_tiles, failed_tiles, existing_tiles

    except Exception as e:
        remove_work_directory(old_cwd, cwd)
        LOGGER.exception(e)
        raise


def main() -> None:
    LOGGER = get_module_logger(__name__)

    # NOTE: we are *not* changing the global start method anymore.
    # That avoids forcing all existing multiprocess code to use spawn
    # (and thus avoids the pickling error you’re seeing).

    # Start telemetry in its own *spawned* process
    cfg = ReporterConfig(
        interval=4.0,
        warmup=0.5,
        workdir=".",
        emit_emf=True,
        namespace="Pixetl/Batch",
    )

    ctx = mp.get_context("spawn")
    telemetry_proc = ctx.Process(
        target=telemetry_process_main,
        args=(cfg,),
        name="pixetl-telemetry",
        daemon=True,  # safe for a one-way logging process
    )
    telemetry_proc.start()
    LOGGER.info("Started telemetry process with PID %s", telemetry_proc.pid)

    try:
        # Run the existing Click CLI
        cli()
    finally:
        LOGGER.info("Shutting down telemetry process...")
        if telemetry_proc.is_alive():
            telemetry_proc.terminate()
            telemetry_proc.join(timeout=5.0)

        # Flush all log handlers before the container exits
        for h in getLogger().handlers:
            try:
                h.flush()
            except Exception:
                pass


if __name__ == "__main__":
    main()
