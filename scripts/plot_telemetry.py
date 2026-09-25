#!/usr/bin/env python3
"""Plot pixetl telemetry from downloaded container/CloudWatch logs.

The telemetry producer emits one JSON object per sample with::

    {"event": "pixetl.telemetry", "schema_version": 1, ...}

This script intentionally ignores every other log record. Input may be raw JSONL,
plain-text/ASCII CloudWatch exports, or CSV exported from the CloudWatch Logs console.
CSV input is detected by filename or by a recognizable message-column header.

Examples:

    python scripts/plot_telemetry.py batch.log
    python scripts/plot_telemetry.py batch.csv
    python scripts/plot_telemetry.py batch.csv --output-dir telemetry-plots
    python scripts/plot_telemetry.py batch.log --prefix run-42 --format svg

Matplotlib is imported only when plotting, so parser tests do not require it.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

TELEMETRY_EVENT = "pixetl.telemetry"
SUPPORTED_SCHEMA_VERSIONS = {1}
BYTES_PER_GIB = 1024**3


def _raise_csv_field_size_limit() -> int:
    """Raise csv's conservative default field limit for large log messages.

    CloudWatch exports can contain individual message fields well above
    Python's default 128 KiB CSV limit.  Use the largest value the
    platform's C ``long`` can accept, backing off for platforms where
    ``sys.maxsize`` is too large.
    """
    limit = sys.maxsize
    while True:
        try:
            return csv.field_size_limit(limit)
        except OverflowError:
            limit //= 10


_raise_csv_field_size_limit()


@dataclass(frozen=True)
class TelemetrySample:
    """One normalized telemetry sample."""

    timestamp_ms: int
    values: Mapping[str, Any]

    @property
    def timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ms / 1000.0, tz=timezone.utc)


@dataclass(frozen=True)
class LogSpan:
    """Timestamp bounds of the complete downloaded log, when available."""

    start_ms: int
    end_ms: int

    @property
    def duration_seconds(self) -> float:
        return max(0.0, (self.end_ms - self.start_ms) / 1000.0)


def _json_candidates(line: str) -> Iterator[str]:
    """Yield plausible JSON objects embedded in a log line.

    Raw CloudWatch downloads are commonly JSONL, but some export forms
    prepend timestamp/log-stream columns. Searching every ``{`` makes
    the parser tolerant of those formats without depending on a specific
    CloudWatch export layout.
    """
    stripped = line.strip()
    if not stripped:
        return

    if stripped.startswith("{"):
        yield stripped
        return

    start = stripped.find("{")
    while start >= 0:
        yield stripped[start:]
        start = stripped.find("{", start + 1)


def _parse_json_object(line: str) -> Optional[Dict[str, Any]]:
    decoder = json.JSONDecoder()
    for candidate in _json_candidates(line):
        try:
            value, _ = decoder.raw_decode(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return None


def _timestamp_ms(record: Mapping[str, Any]) -> Optional[int]:
    aws = record.get("_aws")
    if isinstance(aws, Mapping):
        value = aws.get("Timestamp")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return int(value)

    # Useful fallback for hand-produced/debug records.
    value = record.get("timestamp")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Treat plausible Unix seconds as seconds; otherwise assume milliseconds.
        return int(value * 1000 if value < 10_000_000_000 else value)
    return None


def parse_telemetry_lines(lines: Iterable[str]) -> Tuple[List[TelemetrySample], int]:
    """Return telemetry samples and count of unsupported telemetry records."""
    samples: List[TelemetrySample] = []
    unsupported = 0

    for line in lines:
        record = _parse_json_object(line)
        if not record or record.get("event") != TELEMETRY_EVENT:
            continue

        version = record.get("schema_version")
        if version not in SUPPORTED_SCHEMA_VERSIONS:
            unsupported += 1
            continue

        timestamp_ms = _timestamp_ms(record)
        if timestamp_ms is None:
            continue

        samples.append(TelemetrySample(timestamp_ms=timestamp_ms, values=record))

    samples.sort(key=lambda sample: sample.timestamp_ms)
    return samples, unsupported


def _normalize_csv_header(value: str) -> str:
    """Normalize CloudWatch CSV column names for tolerant matching."""
    return "".join(ch for ch in value.lstrip("\ufeff@").lower() if ch.isalnum())


def _message_column(fieldnames: Optional[Sequence[str]]) -> Optional[str]:
    """Return the column containing the log message, if recognizable.

    CloudWatch exports have used headers such as ``message`` and
    ``@message``. We also accept common variations while deliberately
    avoiding timestamp/stream fields that can themselves contain JSON-
    looking text.
    """
    if not fieldnames:
        return None

    preferred = {"message", "logmessage", "eventmessage"}
    for fieldname in fieldnames:
        if _normalize_csv_header(fieldname) in preferred:
            return fieldname

    for fieldname in fieldnames:
        if _normalize_csv_header(fieldname).endswith("message"):
            return fieldname
    return None


def _timestamp_column(fieldnames: Optional[Sequence[str]]) -> Optional[str]:
    if not fieldnames:
        return None
    preferred = {"timestamp", "eventtimestamp", "logtimestamp"}
    for fieldname in fieldnames:
        if _normalize_csv_header(fieldname) in preferred:
            return fieldname
    for fieldname in fieldnames:
        if _normalize_csv_header(fieldname).endswith("timestamp"):
            return fieldname
    return None


def _parse_external_timestamp_ms(value: str) -> Optional[int]:
    value = value.strip()
    if not value:
        return None
    try:
        numeric = float(value)
    except ValueError:
        numeric = None
    if numeric is not None and math.isfinite(numeric):
        return int(numeric * 1000 if numeric < 10_000_000_000 else numeric)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def parse_telemetry_csv_with_span(
    stream: Iterable[str],
) -> Tuple[List[TelemetrySample], int, Optional[LogSpan]]:
    """Parse CloudWatch CSV and retain the complete exported log time span."""
    reader = csv.DictReader(stream)
    message_column = _message_column(reader.fieldnames)
    if message_column is None:
        headers = ", ".join(reader.fieldnames or []) or "<none>"
        raise ValueError(
            "CSV input does not contain a recognizable message column "
            f"(headers: {headers})"
        )
    timestamp_column = _timestamp_column(reader.fieldnames)
    messages: List[str] = []
    timestamps: List[int] = []
    for row in reader:
        message = row.get(message_column)
        if message:
            messages.append(message)
        if timestamp_column:
            timestamp_ms = _parse_external_timestamp_ms(row.get(timestamp_column, ""))
            if timestamp_ms is not None:
                timestamps.append(timestamp_ms)
    samples, unsupported = parse_telemetry_lines(messages)
    span = LogSpan(min(timestamps), max(timestamps)) if timestamps else None
    return samples, unsupported, span


def parse_telemetry_csv(stream: Iterable[str]) -> Tuple[List[TelemetrySample], int]:
    """Parse a CloudWatch CSV export using its message-bearing column."""
    samples, unsupported, _ = parse_telemetry_csv_with_span(stream)
    return samples, unsupported


def parse_telemetry_cloudwatch_json_with_span(
    value: Any,
) -> Tuple[List[TelemetrySample], int, Optional[LogSpan]]:
    """Parse AWS CLI CloudWatch Logs JSON output.

    Supports both ``filter-log-events`` and ``get-log-events`` output,
    whose top-level object contains an ``events`` array. Each event
    carries the raw CloudWatch ``message`` plus its outer millisecond
    ``timestamp``.
    """
    if not isinstance(value, Mapping):
        raise ValueError("CloudWatch JSON input must be a top-level object")

    events = value.get("events")
    if not isinstance(events, list):
        raise ValueError("JSON input does not contain a CloudWatch 'events' array")

    messages: List[str] = []
    timestamps: List[int] = []
    for event in events:
        if not isinstance(event, Mapping):
            continue
        message = event.get("message")
        if isinstance(message, str):
            messages.append(message)

        timestamp = event.get("timestamp")
        if isinstance(timestamp, (int, float)) and not isinstance(timestamp, bool):
            timestamps.append(int(timestamp))
        elif isinstance(timestamp, str):
            timestamp_ms = _parse_external_timestamp_ms(timestamp)
            if timestamp_ms is not None:
                timestamps.append(timestamp_ms)

    samples, unsupported = parse_telemetry_lines(messages)
    span = LogSpan(min(timestamps), max(timestamps)) if timestamps else None
    return samples, unsupported, span


def _load_cloudwatch_json(
    path: Path,
) -> Optional[Tuple[List[TelemetrySample], int, Optional[LogSpan]]]:
    """Return parsed AWS CLI JSON output, or ``None`` for non-envelope JSON."""
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace") as stream:
            value = json.load(stream)
    except json.JSONDecodeError:
        return None

    if isinstance(value, Mapping) and isinstance(value.get("events"), list):
        return parse_telemetry_cloudwatch_json_with_span(value)
    return None


def _has_csv_message_header(path: Path) -> bool:
    """Inspect the first CSV row without consuming the input used for
    parsing."""
    try:
        with path.open(
            "r", encoding="utf-8-sig", errors="replace", newline=""
        ) as stream:
            first_row = next(csv.reader(stream), [])
    except (OSError, csv.Error):
        return False
    return _message_column(first_row) is not None


def load_telemetry(path: Path) -> Tuple[List[TelemetrySample], int]:
    """Load telemetry from AWS CLI JSON, CloudWatch CSV, or line logs."""
    if path.suffix.lower() == ".json":
        parsed = _load_cloudwatch_json(path)
        if parsed is not None:
            samples, unsupported, _ = parsed
            return samples, unsupported

    is_csv = path.suffix.lower() == ".csv" or _has_csv_message_header(path)
    with path.open(
        "r",
        encoding="utf-8-sig",
        errors="replace",
        newline="" if is_csv else None,
    ) as stream:
        if is_csv:
            return parse_telemetry_csv(stream)
        return parse_telemetry_lines(stream)


def load_telemetry_with_span(
    path: Path,
) -> Tuple[List[TelemetrySample], int, Optional[LogSpan]]:
    """Load telemetry plus the complete exported CloudWatch log span."""
    if path.suffix.lower() == ".json":
        parsed = _load_cloudwatch_json(path)
        if parsed is not None:
            return parsed

    is_csv = path.suffix.lower() == ".csv" or _has_csv_message_header(path)
    with path.open(
        "r", encoding="utf-8-sig", errors="replace", newline="" if is_csv else None
    ) as stream:
        if is_csv:
            return parse_telemetry_csv_with_span(stream)
        samples, unsupported = parse_telemetry_lines(stream)
        return samples, unsupported, None


def _number(sample: TelemetrySample, key: str) -> Optional[float]:
    value = sample.values.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    if not math.isfinite(value):
        return None
    return value


def _series(
    samples: Sequence[TelemetrySample], key: str, scale: float = 1.0
) -> List[float]:
    result: List[float] = []
    for sample in samples:
        value = _number(sample, key)
        result.append(math.nan if value is None else value / scale)
    return result


def _elapsed_minutes(
    samples: Sequence[TelemetrySample], origin_ms: Optional[int] = None
) -> List[float]:
    if not samples:
        return []
    start = samples[0].timestamp_ms if origin_ms is None else origin_ms
    return [(sample.timestamp_ms - start) / 60_000.0 for sample in samples]


def _has_finite(values: Sequence[float]) -> bool:
    return any(math.isfinite(value) for value in values)


def _plot_series(ax, x, series: Sequence[Tuple[str, Sequence[float]]]) -> bool:
    plotted = False
    for label, y in series:
        if _has_finite(y):
            ax.plot(x, y, marker=".", label=label)
            plotted = True
    if plotted and len(series) > 1:
        ax.legend()
    return plotted


def _finish_axis(ax, title: str, ylabel: str) -> None:
    ax.set_title(title)
    ax.set_xlabel("Elapsed time (minutes)")
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.25)


def _save_figure(fig, output: Path) -> None:
    fig.tight_layout()
    fig.savefig(output, dpi=160, bbox_inches="tight")


def plot_telemetry(
    samples: Sequence[TelemetrySample],
    output_dir: Path,
    prefix: str,
    fmt: str,
    log_span: Optional[LogSpan] = None,
) -> List[Path]:
    """Create resource plots and return their paths."""
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:  # pragma: no cover - depends on caller environment
        raise SystemExit(
            "matplotlib is required for plotting. Install it or run with "
            "`uv run --with matplotlib python scripts/plot_telemetry.py ...`."
        ) from exc

    output_dir.mkdir(parents=True, exist_ok=True)
    origin_ms = log_span.start_ms if log_span else samples[0].timestamp_ms
    x = _elapsed_minutes(samples, origin_ms)
    x_max = (
        (log_span.end_ms - origin_ms) / 60_000.0 if log_span else (x[-1] if x else 0.0)
    )
    outputs: List[Path] = []

    def finish(ax, title: str, ylabel: str) -> None:
        _finish_axis(ax, title, ylabel)
        if x_max > 0:
            ax.set_xlim(0, x_max)

    # Process fan-out.
    fig, ax = plt.subplots(figsize=(10, 5))
    _plot_series(
        ax,
        x,
        [
            ("All pixetl processes", _series(samples, "ProcessCount")),
            ("Child processes", _series(samples, "ChildProcessCount")),
        ],
    )
    finish(ax, "Process count", "Processes")
    path = output_dir / f"{prefix}-processes.{fmt}"
    _save_figure(fig, path)
    plt.close(fig)
    outputs.append(path)

    # CPU consumption and allocation.
    fig, ax = plt.subplots(figsize=(10, 5))
    _plot_series(
        ax,
        x,
        [
            ("CPU cores used", _series(samples, "CgroupCPUCoresUsed")),
            ("CPU limit", _series(samples, "CgroupCPULimit")),
        ],
    )
    finish(ax, "CPU usage", "vCPU")
    path = output_dir / f"{prefix}-cpu.{fmt}"
    _save_figure(fig, path)
    plt.close(fig)
    outputs.append(path)

    # Memory: authoritative cgroup consumption vs process-tree RSS.
    fig, ax = plt.subplots(figsize=(10, 5))
    _plot_series(
        ax,
        x,
        [
            ("Cgroup used", _series(samples, "CgroupMemUsed", BYTES_PER_GIB)),
            ("Cgroup peak", _series(samples, "CgroupMemPeak", BYTES_PER_GIB)),
            ("Cgroup limit", _series(samples, "CgroupMemLimit", BYTES_PER_GIB)),
            ("Process RSS", _series(samples, "TotalProcessRSS", BYTES_PER_GIB)),
        ],
    )
    finish(ax, "Memory usage", "GiB")
    path = output_dir / f"{prefix}-memory.{fmt}"
    _save_figure(fig, path)
    plt.close(fig)
    outputs.append(path)

    # Percentage/utilization metrics that share the same scale.
    fig, ax = plt.subplots(figsize=(10, 5))
    _plot_series(
        ax,
        x,
        [
            ("CPU", _series(samples, "CgroupCPUPercent")),
            ("Memory", _series(samples, "CgroupMemPercent")),
            ("Disk", _series(samples, "DiskPercent")),
        ],
    )
    finish(ax, "Resource utilization", "Percent")
    ax.set_ylim(bottom=0)
    path = output_dir / f"{prefix}-utilization.{fmt}"
    _save_figure(fig, path)
    plt.close(fig)
    outputs.append(path)

    # OOM counters are useful even when they remain zero; a step plot makes
    # increments visually unambiguous.
    oom_events = _series(samples, "CgroupOOMEvents")
    oom_kills = _series(samples, "CgroupOOMKills")
    fig, ax = plt.subplots(figsize=(10, 5))
    if _has_finite(oom_events):
        ax.step(x, oom_events, where="post", label="OOM events")
    if _has_finite(oom_kills):
        ax.step(x, oom_kills, where="post", label="OOM kills")
    if _has_finite(oom_events) or _has_finite(oom_kills):
        ax.legend()
    finish(ax, "Cgroup OOM counters", "Count")
    path = output_dir / f"{prefix}-oom.{fmt}"
    _save_figure(fig, path)
    plt.close(fig)
    outputs.append(path)

    return outputs


def _summary(samples: Sequence[TelemetrySample]) -> Dict[str, Optional[float]]:
    def maximum(key: str) -> Optional[float]:
        values = [_number(sample, key) for sample in samples]
        finite = [value for value in values if value is not None]
        return max(finite) if finite else None

    duration_seconds = (
        (samples[-1].timestamp_ms - samples[0].timestamp_ms) / 1000.0
        if len(samples) > 1
        else 0.0
    )
    return {
        "duration_seconds": duration_seconds,
        "max_process_count": maximum("ProcessCount"),
        "max_cpu_cores_used": maximum("CgroupCPUCoresUsed"),
        "max_cpu_percent": maximum("CgroupCPUPercent"),
        "max_memory_gib": (
            maximum("CgroupMemUsed") / BYTES_PER_GIB
            if maximum("CgroupMemUsed") is not None
            else None
        ),
        "max_memory_percent": maximum("CgroupMemPercent"),
        "max_oom_events": maximum("CgroupOOMEvents"),
        "max_oom_kills": maximum("CgroupOOMKills"),
    }


def _format_summary_value(key: str, value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    if key in {"max_process_count", "max_oom_events", "max_oom_kills"}:
        return str(int(value))
    if key == "duration_seconds":
        return f"{value:.1f}s"
    if key == "max_memory_gib":
        return f"{value:.2f} GiB"
    if key.endswith("percent"):
        return f"{value:.1f}%"
    if key == "max_cpu_cores_used":
        return f"{value:.2f} vCPU"
    return f"{value:.2f}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plot schema-v1 pixetl telemetry from a downloaded CloudWatch JSON/CSV or log file."
    )
    parser.add_argument(
        "logfile",
        type=Path,
        help="Downloaded CloudWatch JSON/CSV, ASCII/text export, or container log",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("telemetry-plots"),
        help="Directory for generated plots (default: telemetry-plots)",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="Output filename prefix (default: input log stem)",
    )
    parser.add_argument(
        "--format",
        choices=("png", "svg", "pdf"),
        default="png",
        help="Plot format (default: png)",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        samples, unsupported, log_span = load_telemetry_with_span(args.logfile)
    except (OSError, ValueError, csv.Error) as exc:
        print(f"Could not read telemetry from {args.logfile}: {exc}", file=sys.stderr)
        return 2

    if unsupported:
        print(
            f"warning: ignored {unsupported} telemetry record(s) with unsupported "
            "schema versions",
            file=sys.stderr,
        )
    if not samples:
        print(
            f"No schema-v1 {TELEMETRY_EVENT!r} records found in {args.logfile}",
            file=sys.stderr,
        )
        return 2

    prefix = args.prefix or args.logfile.stem
    outputs = plot_telemetry(
        samples, args.output_dir, prefix, args.format, log_span=log_span
    )

    print(f"Parsed {len(samples)} telemetry samples")
    print(
        "  telemetry range: "
        f"{samples[0].timestamp.isoformat()} -> {samples[-1].timestamp.isoformat()}"
    )
    if log_span:
        telemetry_seconds = (
            samples[-1].timestamp_ms - samples[0].timestamp_ms
        ) / 1000.0
        print(f"  exported log duration: {log_span.duration_seconds:.1f}s")
        print(f"  telemetry coverage: {telemetry_seconds:.1f}s")
        if log_span.duration_seconds > telemetry_seconds + 60:
            print(
                "warning: telemetry covers substantially less time than the exported "
                "log; plots show the full log duration so the missing tail is visible",
                file=sys.stderr,
            )
    for key, value in _summary(samples).items():
        print(f"  {key}: {_format_summary_value(key, value)}")
    print("Generated:")
    for output in outputs:
        print(f"  {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
