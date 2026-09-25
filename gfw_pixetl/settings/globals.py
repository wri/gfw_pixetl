from os import cpu_count
from typing import Optional

import psutil
import pydantic
from pydantic import Field, PositiveInt

from gfw_pixetl import get_module_logger
from gfw_pixetl.models.enums import DstFormat
from gfw_pixetl.settings.models import EnvSettings

LOGGER = get_module_logger(__name__)


class Secret:
    """Holds a string value that should not be revealed in tracebacks etc.

    You should cast the value to `str` at the point it is required.
    """

    def __init__(self, value: str):
        self._value = value

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}('**********')"

    def __str__(self) -> str:
        return self._value


class Globals(EnvSettings):

    #####################
    # General
    #####################

    default_dst_format = DstFormat.geotiff

    #####################
    # Resource management
    ######################
    cores: PositiveInt = Field(cpu_count(), description="Number of CPU cores available")
    num_processes: PositiveInt = Field(
        cpu_count(), description="Max number of parallel processes to use"
    )
    max_mem: PositiveInt = Field(
        psutil.virtual_memory()[1] / 1000000,
        description="Max memory available to pixETL",
    )
    divisor: PositiveInt = Field(
        4,
        description="Fraction of memory per worker to use to compute maximum block size."
        "(ie 4 => size =  25% of available memory)",
    )
    workers: PositiveInt = Field(
        cpu_count(), description="Number of workers to use to execute job."
    )
    download_workers: PositiveInt = Field(
        8,
        description="Maximum number of source files to download concurrently.",
    )
    upload_workers: PositiveInt = Field(
        8,
        description="Maximum number of tiles to upload concurrently.",
    )
    cleanup_workers: PositiveInt = Field(
        4,
        description="Maximum number of tile work directories to clean up concurrently.",
    )
    memory_admission_enabled: bool = Field(
        True, description="Throttle new raster transforms under cgroup memory pressure."
    )
    memory_admission_high_watermark: float = Field(
        0.80,
        description="Stop admitting new transforms/windows at this memory fraction.",
    )
    memory_admission_resume_watermark: float = Field(
        0.75, description="Resume transform admission below this memory fraction."
    )
    memory_admission_stats_workers: PositiveInt = Field(
        4, description="Maximum concurrent GDAL stats/histogram scans."
    )
    memory_admission_reservation_gib: float = Field(
        4.0,
        description="Temporary memory reservation for each newly admitted transform.",
    )
    memory_admission_window_reservation_gib: float = Field(
        4.0,
        description="Memory reserved atomically before dispatching each raster window.",
    )
    memory_admission_poll_seconds: float = Field(
        1.0, description="Polling interval while memory admission is throttled."
    )
    vector_rasterize_reservation_gib: Optional[float] = Field(
        None,
        description="Manual override for the vector rasterize() memory "
        "reservation, in GiB. Leave unset (the default) to compute it "
        "automatically from the layer's actual grid resolution and output "
        "dtype/band count -- see vector_rasterize_reservation_overhead and "
        "VectorPipe._rasterize_reservation_bytes(). A fixed GiB value here "
        "was tried first and got this backwards: sized for a 10m-resolution "
        "WDPA run, it then over-throttled a 30m run by ~6x (30m tiles have "
        "roughly 1/6 the pixels), and would equally under-reserve for an "
        "even higher resolution or a wider layer (e.g. GADM boundaries with "
        "more bands or a larger dtype) than the run it was tuned from. Set "
        "this only to force a specific value regardless of grid/dtype, e.g. "
        "while diagnosing whether the formula itself is off for a layer.",
    )
    vector_rasterize_reservation_overhead: float = Field(
        1.5,
        description="Multiplier applied to a vector tile's raw, uncompressed "
        "output array size (cols * rows * band_count * dtype itemsize) to "
        "estimate gdal_rasterize's real per-tile memory footprint, when "
        "vector_rasterize_reservation_gib is not set to a manual override. "
        "1.5x is not a principled constant -- it's rounded up from the one "
        "data point we have (a 10/100000 WDPA grid, uint8, 1 band: ~9.3GiB "
        "raw array vs. ~12-13GiB observed real usage, so ~1.3-1.4x). "
        "Re-derive from telemetry on other layers/grids before trusting it "
        "far outside that one case, especially for very different dtypes or "
        "band counts where GDAL's internal buffering may not scale the same "
        "way as the raw array size does.",
    )
    vector_rasterize_reservation_floor_gib: float = Field(
        0.5,
        description="Minimum vector rasterize() memory reservation "
        "regardless of the computed formula result, so a very coarse grid "
        "or a small dtype can't compute a near-zero reservation that would "
        "effectively disable admission throttling for that layer.",
    )
    db_fetch_workers: PositiveInt = Field(
        4,
        description="Maximum number of concurrent worker processes allowed to "
        "query the source database at once (vector pipe's filter_src_tiles "
        "and fetch_tile_data stages). This is a budget for the *source* "
        "database's capacity, not the pixETL host's CPU, so it is "
        "intentionally decoupled from num_processes/workers and should be "
        "tuned to what the database can sustain.",
    )
    db_pool_size: PositiveInt = Field(
        2,
        description="Number of pooled connections each DB worker *process* "
        "keeps open and reuses across tiles, instead of opening a brand new "
        "connection for every tile.",
    )
    db_pool_max_overflow: int = Field(
        1,
        description="Extra connections a DB worker process may open above "
        "db_pool_size before it waits for one to free up.",
    )
    db_pool_recycle_seconds: PositiveInt = Field(
        300,
        description="Recycle pooled DB connections older than this many "
        "seconds, so long-lived worker processes don't hold onto "
        "connections the DB (or an intermediate proxy/load balancer) has "
        "silently dropped.",
    )
    db_connect_timeout_seconds: PositiveInt = Field(
        10,
        description="TCP connect timeout, in seconds, for new connections to "
        "the source database.",
    )
    db_statement_timeout_ms: PositiveInt = Field(
        120000,
        description="Postgres statement_timeout, in milliseconds, applied to "
        "every vector source DB query. Caps how long a single slow/expensive "
        "intersection query can hold a connection (and DB-side memory/CPU) "
        "instead of running indefinitely.",
    )

    ########################
    # PostgreSQL authentication
    ########################
    db_username: Optional[str] = Field(
        None, env="PGUSER", description="PostgreSQL user name"
    )
    db_password: Optional[Secret] = Field(
        None, env="PGPASSWORD", description="PostgreSQL password"
    )
    db_host: Optional[str] = Field(None, env="PGHOST", description="PostgreSQL host")
    db_port: Optional[int] = Field(None, env="PGPORT", description="PostgreSQL port")
    db_name: Optional[str] = Field(
        None, env="PGDATABASE", description="PostgreSQL database name"
    )

    ######################
    # AWS configuration
    ######################
    aws_region: str = Field("us-east-1", description="AWS region")
    aws_batch_job_id: Optional[str] = Field(None, description="AWS Batch job ID")
    aws_job_role_arn: Optional[str] = Field(
        None,
        description="ARN of the AWS IAM role which runs the batch job on docker host",
    )
    aws_gcs_key_secret_arn: Optional[str] = Field(
        None, description="ARN of AWS Secret which holds GCS key"
    )

    aws_endpoint_url: Optional[str] = Field(
        None, description="Endpoint URL for AWS S3 Server (required for Moto)"
    )

    aws_secretsmanager_url: Optional[str] = Field(
        None,
        description="Endpoint URL for AWS Secretsmanager Server (required for Moto)",
    )

    @pydantic.validator("db_password", pre=True, always=True)
    def hide_password(cls, v):
        return Secret(v) or None

    @pydantic.root_validator()
    def set_processes_workers(cls, values):
        cores = values.get("cores")

        # Don't allow specifying more processes than cores
        num_processes = max(min(cores, values.get("num_processes")), 1)

        # Don't allow specifying more workers than processes
        workers = max(min(num_processes, values.get("workers")), 1)

        values["num_processes"] = num_processes
        values["workers"] = workers

        LOGGER.info(f"Set num_processes to {num_processes}")
        LOGGER.info(f"Set workers to {workers}")

        return values

    @pydantic.validator("max_mem", pre=True, always=True)
    def set_max_mem(cls, v, *, values, **kwargs):
        max_mem = max(min(psutil.virtual_memory()[1] / 1000000, float(v)), 1)
        LOGGER.info(f"Set maximum memory to {max_mem} MB")
        return max_mem


GLOBALS = Globals()
