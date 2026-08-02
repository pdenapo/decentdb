#!/usr/bin/env python3
"""Aggregate repeated DecentDB rust-baseline runs without mixing contracts.

Two historical target cohorts are intentionally reported:

* ``raw`` is the best positive value in every frozen-manifest RustRaw/default
  run.
  It is an aspirational record table and may span benchmark or durability eras.
* ``durable`` starts with the 2026-08-01 runs that first synced the main
  database file before truncating the WAL.  It is the correctness-comparable
  target for totals and checkpoint timings.

Database and WAL sizes are equality constraints taken from the first durable
run at each scale.  They are not independently minimized: old uncheckpointed
runs have a tiny database and a large WAL, while checkpointed runs have the
inverse, so their independent minima cannot describe a valid result.

Usage:
  python scripts/agg_rust_baseline.py --current-root .tmp/rust-baseline-final \
      --history-dir benchmarks/rust-baseline/results
  python scripts/agg_rust_baseline.py --current-root .tmp/rust-baseline-final \
      --target-cohort raw --require-leading
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Literal, Sequence

STEP_ORDER = [
    "connect_open",
    "schema_create",
    "seed_artists",
    "seed_albums",
    "seed_songs",
    "checkpoint_after_seed",
    "query_count_songs",
    "query_aggregate_durations",
    "query_artist_by_id",
    "query_top10_artists_by_songs",
    "query_top10_albums_by_songs",
    "query_view_first_1000",
    "query_songs_for_artist_via_view",
]
LEGACY_STEP_ORDER = [step for step in STEP_ORDER if step != "checkpoint_after_seed"]

SEED_STEPS = ["seed_artists", "seed_albums", "seed_songs"]
QUERY_STEPS = STEP_ORDER[6:]
ALL_SCALES = ("smoke", "medium", "full", "huge")
EXPECTED_SCALES = {"smoke", "medium", "full", "huge"}
EXPECTED_WORKLOADS = {
    # scale: (target artists, target albums, song cap, actual seeded songs)
    "smoke": (500, 5_000, 50_000, 27_783),
    "medium": (5_000, 50_000, 500_000, 276_243),
    "full": (50_000, 500_000, 5_000_000, 2_749_816),
    "huge": (250_000, 2_500_000, 25_000_000, 13_746_520),
}
CRITICAL_METRICS = {
    "total_runtime",
    "step:seed_artists",
    "step:seed_albums",
    "step:seed_songs",
    "step:checkpoint_after_seed",
    "throughput:seed_artists",
    "throughput:seed_albums",
    "throughput:seed_songs",
}

# First frozen-manifest runs containing the main-database durability barrier
# before WAL truncation. Earlier JSON cannot express this distinction: schema-v2
# reports already used the same broad durability label before the barrier was
# implemented.  Keep this explicit boundary until a newer result schema has a
# dedicated checkpoint durability-contract field.
MAIN_DB_SYNC_DURABILITY_EPOCH_UNIX = 1_785_589_406
CHECKPOINT_DURABILITY_CONTRACT = "main-db-sync-before-wal-truncate-v1"
HISTORY_MANIFEST_NAME = "history-manifest.json"
HISTORY_MANIFEST_SCHEMA_VERSION = 1
ENVIRONMENT_MANIFEST_NAME = "environment-manifest.json"
ENVIRONMENT_MANIFEST_SCHEMA_VERSION = 2
RUNNER_CONTRACT_VERSION = 1
CANONICAL_SEED = 42
FINAL_EXPECTED_TRIALS = 9
FINAL_MINIMUM_LEADERS = 5
FINAL_MINIMUM_CRITICAL_LEADERS = 7
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
REVISION_PATTERN = re.compile(r"[0-9a-f]{40,64}")
CANONICAL_COMMAND_TEMPLATE = [
    "{binary}",
    "--engine",
    "decentdb",
    "--profile",
    "default",
    "--scale",
    "{scale}",
    "--out-dir",
    "{trial_dir}",
    "--db-path",
    "{db_path}",
]

CURRENT_CONTRACT_FIELDS = {
    "binding": "RustRaw",
    "benchmark_profile": "default",
    "result_schema_version": 3,
    "measurement_family": "music_library_total_runtime",
    "engine_access_path": "decentdb_native_rust",
    "durability_profile": "decentdb_durable_wal_default",
    "workload_class": "bulk_load_then_read_only_music_library",
    "cache_profile": "decentdb_default_low_memory",
    "query_repetition_policy": "single_execution_per_query_shape",
    "cold_state_policy": "same_process_fresh_create_then_query",
}
HISTORICAL_CONTRACT_FIELDS = {
    **CURRENT_CONTRACT_FIELDS,
    "result_schema_version": 2,
}

Direction = Literal["lower", "higher"]
TargetCohort = Literal["raw", "durable"]


@dataclass(frozen=True)
class Run:
    path: Path
    resolved_path: Path
    content_sha256: str
    semantic_sha256: str
    report: dict
    steps: dict[str, dict]
    step_order: tuple[str, ...]
    total_seconds: float

    @property
    def scale(self) -> str:
        return str(self.report.get("scale_name", ""))

    @property
    def identity(self) -> tuple[object, ...]:
        """Stable report identity, independent of filename and JSON whitespace."""

        return (self.semantic_sha256,)


@dataclass(frozen=True)
class Metric:
    name: str
    direction: Direction
    value: Callable[[Run], float | None]
    kind: Literal["duration", "throughput", "bytes"]


@dataclass(frozen=True)
class Target:
    value: float
    source: Path


@dataclass(frozen=True)
class StorageConstraint:
    database_size_bytes: int
    wal_size_bytes: int
    source: Path


def finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    if not math.isfinite(result):
        return None
    return result


def positive_number(value: object) -> float | None:
    result = finite_number(value)
    return result if result is not None and result > 0.0 else None


def is_unsigned_integer(value: object, *, positive: bool = False) -> bool:
    if isinstance(value, bool) or not isinstance(value, int):
        return False
    return value > 0 if positive else value >= 0


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def load_run(path: Path) -> Run:
    content = path.read_bytes()
    report = json.loads(content)
    if not isinstance(report, dict):
        raise ValueError("report must be a JSON object")
    raw_steps = report.get("steps", [])
    if not isinstance(raw_steps, list):
        raise ValueError("steps must be a list")

    steps: dict[str, dict] = {}
    step_order: list[str] = []
    for step in raw_steps:
        if not isinstance(step, dict) or not isinstance(step.get("name"), str):
            raise ValueError("every step must be an object with a string name")
        name = step["name"]
        if name in steps:
            raise ValueError(f"duplicate step {name!r}")
        steps[name] = step
        step_order.append(name)

    # Match HistoricalRun/report.html exactly: total runtime is the sum of the
    # serialized step vector. Reject malformed durations rather than silently
    # turning a partial or invalid result into a deceptively small total.
    durations: list[float] = []
    for step in raw_steps:
        if not isinstance(step, dict):
            continue
        duration = finite_number(step.get("duration_seconds"))
        if duration is None or duration <= 0.0:
            raise ValueError("every step duration_seconds must be finite and positive")
        durations.append(duration)
    total_seconds = sum(durations)
    return Run(
        path=path,
        resolved_path=path.resolve(),
        content_sha256=sha256_bytes(content),
        semantic_sha256=sha256_bytes(
            json.dumps(
                report,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        ),
        report=report,
        steps=steps,
        step_order=tuple(step_order),
        total_seconds=total_seconds,
    )


def is_default_decentdb_run(run: Run) -> bool:
    return (
        run.report.get("binding") == "RustRaw"
        and run.report.get("benchmark_profile", "default") == "default"
        and run.scale in EXPECTED_SCALES
    )


def is_current_durability_contract(run: Run) -> bool:
    started_unix = run.report.get("started_unix")
    if (
        not is_unsigned_integer(started_unix)
        or started_unix < MAIN_DB_SYNC_DURABILITY_EPOCH_UNIX
    ):
        return False
    labels_match = all(
        run.report.get(key) == value
        for key, value in CURRENT_CONTRACT_FIELDS.items()
        if key != "result_schema_version"
    ) and run.report.get("result_schema_version") in {
        HISTORICAL_CONTRACT_FIELDS["result_schema_version"],
        CURRENT_CONTRACT_FIELDS["result_schema_version"],
    }
    marker = run.report.get("checkpoint_durability_contract", "")
    return labels_match and marker in ("", CHECKPOINT_DURABILITY_CONTRACT)


def validate_fixed_workload(run: Run) -> None:
    expected_artists, expected_albums, expected_song_cap, expected_songs = (
        EXPECTED_WORKLOADS[run.scale]
    )
    count_extra = run.steps["query_count_songs"].get("extra")
    count_value = count_extra.get("count") if isinstance(count_extra, dict) else None
    workload_values = {
        "target_artists": (run.report.get("target_artists"), expected_artists),
        "target_albums": (run.report.get("target_albums"), expected_albums),
        "target_songs_cap": (run.report.get("target_songs_cap"), expected_song_cap),
        "seed_artists.records": (
            run.steps["seed_artists"].get("records"),
            expected_artists,
        ),
        "seed_albums.records": (
            run.steps["seed_albums"].get("records"),
            expected_albums,
        ),
        "seed_songs.records": (
            run.steps["seed_songs"].get("records"),
            expected_songs,
        ),
        "query_count_songs.extra.count": (count_value, expected_songs),
    }
    mismatches = [
        f"{name}={actual!r} (expected {expected})"
        for name, (actual, expected) in workload_values.items()
        if isinstance(actual, bool) or actual != expected
    ]
    if mismatches:
        raise ValueError("workload mismatch: " + ", ".join(mismatches))


def query_evidence(run: Run, step_name: str) -> dict[str, object]:
    extra = run.steps[step_name].get("extra")
    if not isinstance(extra, dict):
        raise ValueError(f"{step_name}.extra must be an object")
    row_count = extra.get("row_count")
    checksum = extra.get("semantic_checksum_sha256")
    if not is_unsigned_integer(row_count):
        raise ValueError(f"{step_name}.extra.row_count must be an unsigned integer")
    if not isinstance(checksum, str) or SHA256_PATTERN.fullmatch(checksum) is None:
        raise ValueError(
            f"{step_name}.extra.semantic_checksum_sha256 must be a SHA-256 digest"
        )
    return extra


def validate_query_evidence(run: Run) -> None:
    _, _, _, expected_songs = EXPECTED_WORKLOADS[run.scale]
    evidence = {step: query_evidence(run, step) for step in QUERY_STEPS}

    count = evidence["query_count_songs"]
    if count.get("row_count") != 1 or count.get("count") != expected_songs:
        raise ValueError("count query evidence does not match the fixed song count")

    aggregate = evidence["query_aggregate_durations"]
    aggregate_count = aggregate.get("song_count")
    duration_sum = finite_number(aggregate.get("duration_sum"))
    duration_average = finite_number(aggregate.get("duration_average"))
    duration_minimum = finite_number(aggregate.get("duration_minimum"))
    duration_maximum = finite_number(aggregate.get("duration_maximum"))
    if (
        aggregate.get("row_count") != 1
        or aggregate_count != expected_songs
        or duration_sum is None
        or duration_sum <= 0
        or duration_average is None
        or duration_minimum is None
        or duration_minimum <= 0
        or duration_maximum is None
        or duration_maximum < duration_minimum
        or not duration_minimum <= duration_average <= duration_maximum
        or not math.isclose(
            duration_sum / expected_songs,
            duration_average,
            rel_tol=1e-12,
            abs_tol=1e-9,
        )
    ):
        raise ValueError("aggregate query evidence is invalid")

    artist = evidence["query_artist_by_id"]
    expected_artist_id = EXPECTED_WORKLOADS[run.scale][0] // 2 + 1
    if (
        artist.get("row_count") != 1
        or artist.get("target_artist_id") != expected_artist_id
        or artist.get("artist_id") != expected_artist_id
        or artist.get("artist_name") != f"Artist {expected_artist_id}"
    ):
        raise ValueError(
            "artist lookup evidence does not match the deterministic target"
        )

    for step_name in (
        "query_top10_artists_by_songs",
        "query_top10_albums_by_songs",
    ):
        if evidence[step_name].get("row_count") != 10:
            raise ValueError(f"{step_name} evidence must contain exactly 10 rows")

    expected_view_rows = min(1000, expected_songs)
    if evidence["query_view_first_1000"].get("row_count") != expected_view_rows:
        raise ValueError(
            "query_view_first_1000 evidence does not match the fixed workload"
        )
    if evidence["query_songs_for_artist_via_view"].get("row_count", 0) <= 0:
        raise ValueError("filtered view evidence must contain at least one row")


def validate_candidate_provenance_fields(run: Run) -> None:
    if run.report.get("runner_contract_version") != RUNNER_CONTRACT_VERSION:
        raise ValueError(f"runner_contract_version must be {RUNNER_CONTRACT_VERSION}")
    if not is_unsigned_integer(run.report.get("seed")):
        raise ValueError("seed must be an unsigned integer")
    argv = run.report.get("invocation_argv")
    if (
        not isinstance(argv, list)
        or not argv
        or any(not isinstance(argument, str) or not argument for argument in argv)
    ):
        raise ValueError("invocation_argv must be a non-empty array of strings")
    digest = run.report.get("executable_sha256")
    if not isinstance(digest, str) or SHA256_PATTERN.fullmatch(digest) is None:
        raise ValueError("executable_sha256 must be a SHA-256 digest")


def validate_canonical_metric_integrity(run: Run) -> None:
    for field in ("started_unix", "finished_unix"):
        if not is_unsigned_integer(run.report.get(field)):
            raise ValueError(f"{field} must be an unsigned integer")
    if run.report["finished_unix"] < run.report["started_unix"]:
        raise ValueError("finished_unix must not precede started_unix")
    for field in ("database_size_bytes", "wal_size_bytes", "peak_rss_bytes"):
        if not is_unsigned_integer(run.report.get(field), positive=True):
            raise ValueError(f"{field} must be a positive integer")
    for field in ("engine_version", "database_path"):
        if not isinstance(run.report.get(field), str) or not run.report[field]:
            raise ValueError(f"{field} must be a non-empty string")

    step_rss: list[int] = []
    for step_name in run.step_order:
        step = run.steps[step_name]
        rss = step.get("rss_bytes")
        if not is_unsigned_integer(rss, positive=True):
            raise ValueError(f"step {step_name!r} rss_bytes must be positive")
        step_rss.append(rss)
        for field in ("rss_anon_kb", "rss_file_kb"):
            if field in step and not is_unsigned_integer(step[field]):
                raise ValueError(f"step {step_name!r} {field} must be unsigned")
        if not isinstance(step.get("extra"), dict):
            raise ValueError(f"step {step_name!r} extra must be an object")

    expected_peak = max(step_rss)
    if run.report["peak_rss_bytes"] != expected_peak:
        raise ValueError(
            "peak_rss_bytes does not equal max step rss_bytes: "
            f"{run.report['peak_rss_bytes']} != {expected_peak}"
        )

    for step_name in SEED_STEPS:
        step = run.steps[step_name]
        records = step.get("records")
        throughput = positive_number(step.get("records_per_second"))
        duration = positive_number(step.get("duration_seconds"))
        if not is_unsigned_integer(records, positive=True):
            raise ValueError(f"step {step_name!r} records must be positive")
        if throughput is None or duration is None:
            raise ValueError(f"step {step_name!r} throughput is missing or invalid")
        expected_throughput = records / duration
        if not math.isclose(
            throughput,
            expected_throughput,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ):
            raise ValueError(
                f"step {step_name!r} records_per_second is inconsistent with "
                "records/duration_seconds"
            )

    for step_name in run.step_order:
        if step_name in SEED_STEPS:
            continue
        step = run.steps[step_name]
        if (
            step.get("records") is not None
            or step.get("records_per_second") is not None
        ):
            raise ValueError(
                f"step {step_name!r} unexpectedly declares records or throughput"
            )


def validate_empty_optional_suites(run: Run, *, allow_missing: bool = False) -> None:
    for field in ("latency_cases", "concurrency_cases", "write_cases", "cold_cases"):
        if field not in run.report and not allow_missing:
            raise ValueError(f"canonical run is missing the {field} array")
        value = run.report.get(field, [])
        if not isinstance(value, list) or value:
            raise ValueError(f"canonical run must contain an empty {field} array")


def validate_checkpoint_extra(run: Run) -> None:
    step = run.steps.get("checkpoint_after_seed")
    if step is None:
        return
    extra = step.get("extra")
    if not isinstance(extra, dict):
        raise ValueError("checkpoint_after_seed.extra must be an object")
    if extra.get("checkpoint_mode") != "wal":
        raise ValueError("checkpoint_after_seed must declare checkpoint_mode='wal'")
    for field in (
        "database_bytes_before",
        "database_bytes_after",
        "wal_bytes_before",
        "wal_bytes_after",
    ):
        if not is_unsigned_integer(extra.get(field), positive=True):
            raise ValueError(f"checkpoint_after_seed.extra.{field} must be positive")
    if extra["database_bytes_after"] != run.report["database_size_bytes"]:
        raise ValueError("checkpoint database_bytes_after does not match final DB size")
    if extra["wal_bytes_after"] != run.report["wal_size_bytes"]:
        raise ValueError("checkpoint wal_bytes_after does not match final WAL size")
    if extra["database_bytes_after"] < extra["database_bytes_before"]:
        raise ValueError("checkpoint database must not shrink")
    if extra["wal_bytes_before"] <= extra["wal_bytes_after"]:
        raise ValueError("checkpoint WAL must shrink")


def validate_history_run(run: Run) -> None:
    if not is_default_decentdb_run(run):
        raise ValueError("manifest entry is not a RustRaw/default canonical run")
    if run.step_order not in (tuple(LEGACY_STEP_ORDER), tuple(STEP_ORDER)):
        raise ValueError(
            f"historical step order/contract is unsupported: {list(run.step_order)!r}"
        )
    validate_fixed_workload(run)
    validate_canonical_metric_integrity(run)
    validate_empty_optional_suites(run, allow_missing=True)
    validate_checkpoint_extra(run)

    schema_version = run.report.get("result_schema_version")
    if schema_version is not None:
        if schema_version != HISTORICAL_CONTRACT_FIELDS["result_schema_version"]:
            raise ValueError(
                f"unsupported historical result_schema_version {schema_version!r}"
            )
        for field, expected in HISTORICAL_CONTRACT_FIELDS.items():
            if run.report.get(field) != expected:
                raise ValueError(
                    f"historical contract mismatch: {field}={run.report.get(field)!r}, "
                    f"expected {expected!r}"
                )

    optional_features = run.report.get("compiled_optional_features", [])
    if not isinstance(optional_features, list) or optional_features:
        raise ValueError("canonical historical run has compiled optional features")
    marker = run.report.get("checkpoint_durability_contract", "")
    if marker not in ("", CHECKPOINT_DURABILITY_CONTRACT):
        raise ValueError(f"unknown checkpoint durability contract {marker!r}")


def validate_candidate_run(run: Run) -> None:
    if not is_default_decentdb_run(run):
        raise ValueError("expected RustRaw/default canonical run")
    if not is_current_durability_contract(run):
        raise ValueError("candidate does not declare the current durability contract")
    if (
        run.report.get("checkpoint_durability_contract")
        != CHECKPOINT_DURABILITY_CONTRACT
    ):
        raise ValueError(
            "candidate checkpoint_durability_contract must be "
            f"{CHECKPOINT_DURABILITY_CONTRACT!r}"
        )
    for field, expected in CURRENT_CONTRACT_FIELDS.items():
        if run.report.get(field) != expected:
            raise ValueError(
                f"candidate contract mismatch: {field}={run.report.get(field)!r}, "
                f"expected {expected!r}"
            )
    if run.report.get("compiled_optional_features") != []:
        raise ValueError("candidate compiled_optional_features must be []")
    if run.step_order != tuple(STEP_ORDER):
        raise ValueError(
            f"candidate step order mismatch: {list(run.step_order)!r}; "
            f"expected {STEP_ORDER!r}"
        )
    validate_fixed_workload(run)
    validate_query_evidence(run)
    validate_candidate_provenance_fields(run)
    validate_canonical_metric_integrity(run)
    validate_empty_optional_suites(run)
    validate_checkpoint_extra(run)


def load_history_manifest(
    history_dir: Path, manifest_path: Path | None = None
) -> list[Run]:
    manifest_path = manifest_path or history_dir / HISTORY_MANIFEST_NAME
    try:
        manifest = json.loads(manifest_path.read_bytes())
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(
            f"cannot read history manifest {manifest_path}: {error}"
        ) from error
    if not isinstance(manifest, dict):
        raise ValueError("history manifest must be a JSON object")
    if set(manifest) != {"schema_version", "files"}:
        raise ValueError("history manifest must contain only schema_version and files")
    if manifest.get("schema_version") != HISTORY_MANIFEST_SCHEMA_VERSION:
        raise ValueError("unsupported history manifest schema_version")
    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        raise ValueError("history manifest files must be a non-empty array")

    history_root = history_dir.resolve()
    runs: list[Run] = []
    manifest_names: set[str] = set()
    manifest_digests: set[str] = set()
    identities: set[tuple[object, ...]] = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"path", "sha256"}:
            raise ValueError("every history manifest entry needs only path and sha256")
        relative = entry.get("path")
        digest = entry.get("sha256")
        if (
            not isinstance(relative, str)
            or not relative
            or Path(relative).is_absolute()
            or len(Path(relative).parts) != 1
        ):
            raise ValueError(f"unsafe history manifest path {relative!r}")
        if not isinstance(digest, str) or SHA256_PATTERN.fullmatch(digest) is None:
            raise ValueError(f"invalid sha256 for history manifest entry {relative!r}")
        if relative in manifest_names:
            raise ValueError(f"duplicate history manifest path {relative!r}")
        if digest in manifest_digests:
            raise ValueError(f"duplicate history manifest digest {digest}")
        manifest_names.add(relative)
        manifest_digests.add(digest)

        path = history_dir / relative
        resolved = path.resolve()
        if resolved.parent != history_root:
            raise ValueError(
                f"history manifest path escapes history directory: {relative}"
            )
        try:
            actual_digest = sha256_file(path)
        except OSError as error:
            raise ValueError(
                f"missing history manifest entry {path}: {error}"
            ) from error
        if actual_digest != digest:
            raise ValueError(
                f"history manifest digest mismatch for {path}: "
                f"expected {digest}, got {actual_digest}"
            )
        try:
            run = load_run(path)
            validate_history_run(run)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            raise ValueError(f"invalid manifested history {path}: {error}") from error
        if run.identity in identities:
            raise ValueError(f"duplicate historical run identity in {path}")
        identities.add(run.identity)
        runs.append(run)

    ambient = sorted(
        path.name
        for path in history_dir.glob("*.json")
        if "rust-baseline" in path.name and path.name not in manifest_names
    )
    if ambient:
        print(
            f"warning: ignoring {len(ambient)} unmanifested rust-baseline history file(s): "
            + ", ".join(ambient),
            file=sys.stderr,
        )
    return runs


def collect_candidate_runs(root: Path) -> list[Run]:
    runs: list[Run] = []
    for path in sorted(root.rglob("*.json")):
        if "rust-baseline" not in path.name:
            continue
        try:
            run = load_run(path)
            validate_candidate_run(run)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            raise ValueError(f"invalid candidate {path}: {error}") from error
        runs.append(run)
    return runs


def validate_candidate_history_disjoint(
    candidate_runs: Sequence[Run], history_runs: Sequence[Run]
) -> None:
    history_paths = {run.resolved_path for run in history_runs}
    history_digests = {run.content_sha256 for run in history_runs}
    history_identities = {run.identity for run in history_runs}
    for run in candidate_runs:
        if run.resolved_path in history_paths:
            raise ValueError(f"candidate path is also manifested history: {run.path}")
        if run.content_sha256 in history_digests:
            raise ValueError(
                f"candidate content duplicates manifested history: {run.path}"
            )
        if run.identity in history_identities:
            raise ValueError(
                f"candidate run identity duplicates manifested history: {run.path}"
            )


def validate_candidate_uniqueness(candidate_runs: Sequence[Run]) -> None:
    for label, values in (
        ("resolved path", [run.resolved_path for run in candidate_runs]),
        ("content digest", [run.content_sha256 for run in candidate_runs]),
        ("run identity", [run.identity for run in candidate_runs]),
    ):
        if len(set(values)) != len(values):
            raise ValueError(f"candidate runs contain a duplicate {label}")


def step_value(run: Run, step_name: str, field: str) -> float | None:
    step = run.steps.get(step_name)
    return None if step is None else positive_number(step.get(field))


def metrics() -> list[Metric]:
    result = [
        Metric(
            "total_runtime",
            "lower",
            lambda run: positive_number(run.total_seconds),
            "duration",
        ),
        Metric(
            "peak_rss",
            "lower",
            lambda run: positive_number(run.report.get("peak_rss_bytes")),
            "bytes",
        ),
    ]
    result.extend(
        Metric(
            f"step:{step_name}",
            "lower",
            lambda run, name=step_name: step_value(run, name, "duration_seconds"),
            "duration",
        )
        for step_name in STEP_ORDER
    )
    result.extend(
        Metric(
            f"throughput:{step_name}",
            "higher",
            lambda run, name=step_name: step_value(run, name, "records_per_second"),
            "throughput",
        )
        for step_name in SEED_STEPS
    )
    return result


def group_by_scale(runs: Iterable[Run]) -> dict[str, list[Run]]:
    grouped: dict[str, list[Run]] = {}
    for run in runs:
        grouped.setdefault(run.scale, []).append(run)
    for scale_runs in grouped.values():
        scale_runs.sort(
            key=lambda run: (int(run.report.get("started_unix", 0)), run.path.name)
        )
    return grouped


def best_target(scale_runs: Iterable[Run], metric: Metric) -> Target | None:
    values = [
        (value, run.path)
        for run in scale_runs
        # A total is comparable only when it sums the candidate's exact timed
        # step contract. Legacy 12-step runs remain eligible for their common
        # step, throughput, and RSS records.
        if (metric.name != "total_runtime" or run.step_order == tuple(STEP_ORDER))
        and (value := metric.value(run)) is not None
    ]
    if not values:
        return None
    select = min if metric.direction == "lower" else max
    value, source = select(values, key=lambda item: item[0])
    return Target(value=value, source=source)


def storage_constraint(scale_runs: list[Run]) -> StorageConstraint | None:
    if not scale_runs:
        return None
    first = min(
        scale_runs,
        key=lambda run: (int(run.report.get("started_unix", 0)), run.path.name),
    )
    database_size = first.report.get("database_size_bytes")
    wal_size = first.report.get("wal_size_bytes")
    if not isinstance(database_size, int) or not isinstance(wal_size, int):
        return None
    return StorageConstraint(database_size, wal_size, first.path)


def format_duration(value: float) -> str:
    if value < 0.001:
        return f"{value * 1_000_000:.0f}us"
    if value < 1.0:
        return f"{value * 1_000:.2f}ms"
    return f"{value:.4f}s"


def format_bytes(value: float) -> str:
    units = ["B", "KiB", "MiB", "GiB"]
    amount = value
    for unit in units:
        if amount < 1024.0 or unit == units[-1]:
            return f"{amount:.1f}{unit}"
        amount /= 1024.0
    raise AssertionError("unreachable")


def format_value(metric: Metric, value: float | None) -> str:
    if value is None:
        return "-"
    if metric.kind == "duration":
        return format_duration(value)
    if metric.kind == "bytes":
        return format_bytes(value)
    return f"{value:,.0f}/s"


def current_stats(runs: list[Run], metric: Metric) -> tuple[float, float, float] | None:
    values = [value for run in runs if (value := metric.value(run)) is not None]
    if len(values) != len(runs) or not values:
        return None
    return statistics.median(values), min(values), max(values)


def is_leading(value: float, target: Target, direction: Direction) -> bool:
    if direction == "lower":
        return value <= target.value
    return value >= target.value


def ratio(value: float, target: Target) -> float:
    return value / target.value


def print_target_table(
    scale: str,
    metric_list: list[Metric],
    raw_runs: list[Run],
    durable_runs: list[Run],
) -> None:
    print(f"\n### {scale} historical targets")
    print(
        f"  {'metric':<43} {'dir':<6} {'raw best':>13} {'raw source':<38} "
        f"{'durable best':>13} {'durable source'}"
    )
    for metric in metric_list:
        raw = best_target(raw_runs, metric)
        durable = best_target(durable_runs, metric)
        print(
            f"  {metric.name:<43} {metric.direction:<6} "
            f"{format_value(metric, raw.value) if raw else '-':>13} "
            f"{raw.source.name if raw else '-':<38} "
            f"{format_value(metric, durable.value) if durable else '-':>13} "
            f"{durable.source.name if durable else '-'}"
        )


def print_current_table(
    scale: str,
    metric_list: list[Metric],
    current_runs: list[Run],
    raw_runs: list[Run],
    durable_runs: list[Run],
    minimum_leaders: int,
    minimum_critical_leaders: int,
) -> dict[TargetCohort, bool]:
    print(f"\n### {scale} current comparison ({len(current_runs)} trial(s))")
    print(
        f"  {'metric':<43} {'median':>13} {'range':>29} "
        f"{'raw ratio':>10} {'raw wins':>10} {'dur ratio':>10} {'dur wins':>10}"
    )
    passed: dict[TargetCohort, bool] = {"raw": True, "durable": True}
    for metric in metric_list:
        stats = current_stats(current_runs, metric)
        raw = best_target(raw_runs, metric)
        durable = best_target(durable_runs, metric)
        if stats is None:
            print(f"  {metric.name:<43} {'missing':>13}")
            passed["raw"] = False
            passed["durable"] = False
            continue

        median, minimum, maximum = stats
        range_text = (
            f"[{format_value(metric, minimum)}, {format_value(metric, maximum)}]"
        )
        target_cells: list[str] = []
        for cohort, target in (("raw", raw), ("durable", durable)):
            if target is None:
                target_cells.extend(["-", "-"])
                passed[cohort] = False
                continue
            wins = sum(
                1
                for run in current_runs
                if (value := metric.value(run)) is not None
                and is_leading(value, target, metric.direction)
            )
            median_leads = is_leading(median, target, metric.direction)
            passed[cohort] &= median_leads
            required_leaders = minimum_leaders
            if metric.name in CRITICAL_METRICS:
                required_leaders = max(required_leaders, minimum_critical_leaders)
            passed[cohort] &= wins >= required_leaders
            marker = "*" if median_leads else ""
            target_cells.extend(
                [f"{ratio(median, target):.3f}{marker}", f"{wins}/{len(current_runs)}"]
            )

        print(
            f"  {metric.name:<43} {format_value(metric, median):>13} "
            f"{range_text:>29} {target_cells[0]:>10} {target_cells[1]:>10} "
            f"{target_cells[2]:>10} {target_cells[3]:>10}"
        )
    print("  * median is metric-leading for that target cohort")
    return passed


def print_storage_constraint(
    scale: str,
    current_runs: list[Run],
    constraint: StorageConstraint | None,
) -> bool:
    if constraint is None:
        print("  storage constraint: missing durable historical reference")
        return False
    observed = sorted(
        {
            (
                run.report.get("database_size_bytes"),
                run.report.get("wal_size_bytes"),
            )
            for run in current_runs
        },
        key=repr,
    )
    expected = (constraint.database_size_bytes, constraint.wal_size_bytes)
    passed = observed == [expected]
    observed_text = ", ".join(
        f"db={format_bytes(float(db)) if isinstance(db, int) else db} "
        f"wal={format_bytes(float(wal)) if isinstance(wal, int) else wal}"
        for db, wal in observed
    )
    print(
        "  storage equality: "
        f"expected db={format_bytes(float(expected[0]))} "
        f"wal={format_bytes(float(expected[1]))} "
        f"from {constraint.source.name}; observed {observed_text}; "
        f"{'PASS' if passed else 'FAIL'}"
    )
    return passed


def command_template_sha256(template: Sequence[str]) -> str:
    encoded = json.dumps(
        list(template), separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return sha256_bytes(encoded)


def _manifest_artifact_path(current_root: Path, value: object, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError(f"environment {label}.path must be a non-empty string")
    path = Path(value)
    if not path.is_absolute():
        path = current_root / path
    return path.resolve()


def _validate_manifest_artifact(
    current_root: Path,
    value: object,
    label: str,
    *,
    require_under_root: bool,
) -> None:
    if not isinstance(value, dict) or set(value) != {"path", "sha256"}:
        raise ValueError(f"environment {label} must contain only path and sha256")
    digest = value.get("sha256")
    if not isinstance(digest, str) or SHA256_PATTERN.fullmatch(digest) is None:
        raise ValueError(f"environment {label}.sha256 is invalid")
    path = _manifest_artifact_path(current_root, value.get("path"), label)
    root = current_root.resolve()
    if require_under_root and path != root and root not in path.parents:
        raise ValueError(f"environment {label}.path must be under current root")
    try:
        actual = sha256_file(path)
    except OSError as error:
        raise ValueError(
            f"cannot read environment {label} artifact {path}: {error}"
        ) from error
    if actual != digest:
        raise ValueError(
            f"environment {label} digest mismatch: expected {digest}, got {actual}"
        )


def validate_environment_manifest(
    current_root: Path,
    manifest_path: Path,
    *,
    scales: Sequence[str],
    expected_trials: int,
) -> dict[str, object]:
    """Validate final-gate provenance captured before running any trial.

    Schema v1 is deliberately explicit. ``binary`` identifies the frozen
    executable. ``source_state``, ``command``, and ``platform`` are immutable
    evidence files under ``current_root``. The command object also records the
    canonical argv template, whose JSON digest prevents ambiguous shell-text
    interpretation.
    """

    try:
        manifest = json.loads(manifest_path.read_bytes())
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(
            f"cannot read environment manifest {manifest_path}: {error}"
        ) from error
    expected_fields = {
        "schema_version",
        "binary",
        "source_revision",
        "source_state",
        "command",
        "platform",
        "engine",
        "profile",
        "scales",
        "expected_trials",
        "runner_contract_version",
        "seed",
    }
    if not isinstance(manifest, dict) or set(manifest) != expected_fields:
        raise ValueError(
            "environment manifest fields must be exactly "
            + ", ".join(sorted(expected_fields))
        )
    if manifest.get("schema_version") != ENVIRONMENT_MANIFEST_SCHEMA_VERSION:
        raise ValueError("unsupported environment manifest schema_version")
    revision = manifest.get("source_revision")
    if not isinstance(revision, str) or REVISION_PATTERN.fullmatch(revision) is None:
        raise ValueError(
            "environment source_revision must be a 40-64 digit lowercase hex ID"
        )
    if manifest.get("engine") != "decentdb" or manifest.get("profile") != "default":
        raise ValueError("environment manifest must declare decentdb/default")
    if manifest.get("scales") != list(scales):
        raise ValueError("environment manifest scales do not match requested scales")
    if manifest.get("expected_trials") != expected_trials:
        raise ValueError("environment manifest expected_trials does not match the gate")
    if manifest.get("runner_contract_version") != RUNNER_CONTRACT_VERSION:
        raise ValueError("environment runner_contract_version is not canonical")
    if manifest.get("seed") != CANONICAL_SEED:
        raise ValueError("environment seed is not canonical")

    _validate_manifest_artifact(
        current_root,
        manifest.get("binary"),
        "binary",
        require_under_root=False,
    )
    _validate_manifest_artifact(
        current_root,
        manifest.get("source_state"),
        "source_state",
        require_under_root=True,
    )
    _validate_manifest_artifact(
        current_root,
        manifest.get("platform"),
        "platform",
        require_under_root=True,
    )

    command = manifest.get("command")
    if not isinstance(command, dict) or set(command) != {
        "path",
        "sha256",
        "argv_template",
        "argv_template_sha256",
    }:
        raise ValueError(
            "environment command must contain path, sha256, argv_template, "
            "and argv_template_sha256"
        )
    template = command.get("argv_template")
    if template != CANONICAL_COMMAND_TEMPLATE:
        raise ValueError("environment command argv_template is not canonical")
    template_digest = command.get("argv_template_sha256")
    if not isinstance(
        template_digest, str
    ) or template_digest != command_template_sha256(CANONICAL_COMMAND_TEMPLATE):
        raise ValueError("environment command argv_template_sha256 is invalid")
    _validate_manifest_artifact(
        current_root,
        {"path": command.get("path"), "sha256": command.get("sha256")},
        "command",
        require_under_root=True,
    )
    return manifest


def validate_trial_matrix(
    current_root: Path,
    runs: Sequence[Run],
    *,
    scales: Sequence[str],
    expected_trials: int,
    environment_manifest: dict[str, object] | None = None,
) -> None:
    if not current_root.is_dir():
        raise ValueError(f"current root is not a directory: {current_root}")
    expected_trial_names = {
        f"trial-{index:02d}" for index in range(1, expected_trials + 1)
    }
    actual_trial_names = {
        path.name
        for path in current_root.iterdir()
        if path.is_dir() and path.name.startswith("trial-")
    }
    if actual_trial_names != expected_trial_names:
        raise ValueError(
            "trial directory mismatch: "
            f"missing={sorted(expected_trial_names - actual_trial_names)}, "
            f"extra={sorted(actual_trial_names - expected_trial_names)}"
        )

    expected_cells = {
        (trial_name, scale) for trial_name in expected_trial_names for scale in scales
    }
    cells: dict[tuple[str, str], Run] = {}
    database_paths: set[Path] = set()
    engine_versions: set[str] = set()
    root = current_root.resolve()
    for run in runs:
        try:
            relative = run.resolved_path.relative_to(root)
        except ValueError as error:
            raise ValueError(
                f"candidate path escapes current root: {run.path}"
            ) from error
        if len(relative.parts) != 3:
            raise ValueError(
                f"candidate must be trial-NN/<scale>/<result>.json: {run.path}"
            )
        trial_name, scale, _ = relative.parts
        cell = (trial_name, scale)
        if cell not in expected_cells:
            raise ValueError(
                f"candidate occupies unexpected trial cell {cell}: {run.path}"
            )
        if run.scale != scale:
            raise ValueError(f"candidate scale metadata/path mismatch in {run.path}")
        if cell in cells:
            raise ValueError(f"multiple candidate results in trial cell {cell}")
        cells[cell] = run

        expected_db_path = (
            current_root / trial_name / scale / f"run-rust-{scale}.ddb"
        ).resolve()
        reported_db_path = Path(run.report["database_path"])
        if not reported_db_path.is_absolute():
            reported_db_path = (Path.cwd() / reported_db_path).resolve()
        else:
            reported_db_path = reported_db_path.resolve()
        if reported_db_path != expected_db_path:
            raise ValueError(
                f"candidate database_path mismatch in {run.path}: "
                f"expected {expected_db_path}, got {reported_db_path}"
            )
        if reported_db_path in database_paths:
            raise ValueError(f"duplicate candidate database_path {reported_db_path}")
        database_paths.add(reported_db_path)
        engine_versions.add(run.report["engine_version"])

        if environment_manifest is not None:
            binary = environment_manifest["binary"]
            assert isinstance(binary, dict)
            binary_path = _manifest_artifact_path(
                current_root, binary.get("path"), "binary"
            )
            binary_digest = binary.get("sha256")
            trial_dir = (current_root / trial_name / scale).resolve()
            substitutions = {
                "binary": str(binary_path),
                "scale": scale,
                "trial_dir": str(trial_dir),
                "db_path": str(expected_db_path),
            }
            expected_argv = [
                argument.format_map(substitutions)
                for argument in CANONICAL_COMMAND_TEMPLATE
            ]
            if run.report.get("runner_contract_version") != RUNNER_CONTRACT_VERSION:
                raise ValueError(f"candidate runner contract mismatch in {run.path}")
            if run.report.get("seed") != CANONICAL_SEED:
                raise ValueError(f"candidate seed mismatch in {run.path}")
            if run.report.get("invocation_argv") != expected_argv:
                raise ValueError(
                    f"candidate invocation_argv mismatch in {run.path}: "
                    f"expected {expected_argv!r}"
                )
            if run.report.get("executable_sha256") != binary_digest:
                raise ValueError(f"candidate executable digest mismatch in {run.path}")

    if set(cells) != expected_cells:
        raise ValueError(
            "trial result matrix mismatch: "
            f"missing={sorted(expected_cells - set(cells))}, "
            f"extra={sorted(set(cells) - expected_cells)}"
        )
    if len(engine_versions) != 1:
        raise ValueError(
            f"candidate engine_version changed across trials: {engine_versions}"
        )

    for scale in scales:
        scale_runs = [
            cells[(trial_name, scale)] for trial_name in sorted(expected_trial_names)
        ]
        reference = {
            step_name: scale_runs[0].steps[step_name]["extra"]
            for step_name in QUERY_STEPS
        }
        for run in scale_runs[1:]:
            observed = {
                step_name: run.steps[step_name]["extra"] for step_name in QUERY_STEPS
            }
            if observed != reference:
                raise ValueError(
                    f"query semantic evidence changed across {scale} trials: {run.path}"
                )

    # Reject extra/missing scale directories and extra result JSONs even if a
    # filename does not happen to match the collector's rust-baseline filter.
    for trial_name in sorted(expected_trial_names):
        trial_dir = current_root / trial_name
        actual_scale_dirs = {path.name for path in trial_dir.iterdir() if path.is_dir()}
        if actual_scale_dirs != set(scales):
            raise ValueError(
                f"scale directory mismatch in {trial_dir}: "
                f"missing_scales={sorted(set(scales) - actual_scale_dirs)}, "
                f"extra_scales={sorted(actual_scale_dirs - set(scales))}"
            )
        for scale in scales:
            json_files = sorted((trial_dir / scale).glob("*.json"))
            if json_files != [cells[(trial_name, scale)].path]:
                raise ValueError(
                    f"trial cell {(trial_name, scale)} must contain exactly one JSON result"
                )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current-root", type=Path, required=True)
    parser.add_argument(
        "--history-dir",
        type=Path,
        default=Path("benchmarks/rust-baseline/results"),
    )
    parser.add_argument(
        "--history-manifest",
        type=Path,
        help=f"Frozen history manifest (default: <history-dir>/{HISTORY_MANIFEST_NAME})",
    )
    parser.add_argument(
        "--scales",
        nargs="+",
        choices=sorted(EXPECTED_SCALES),
        default=list(ALL_SCALES),
    )
    parser.add_argument(
        "--target-cohort",
        choices=["raw", "durable"],
        default="raw",
        help="Target used by --require-leading (both cohorts are always printed)",
    )
    parser.add_argument(
        "--require-leading",
        action="store_true",
        help=(
            "Run the final 9x4 provenance gate and exit nonzero unless every "
            "median, per-trial leader threshold, and storage constraint passes"
        ),
    )
    parser.add_argument(
        "--minimum-trials",
        type=int,
        default=1,
        help="Minimum current trials required per scale",
    )
    parser.add_argument(
        "--expected-trials",
        type=int,
        help=(
            "Require an exact trial-NN x scale result matrix; --require-leading "
            f"defaults this to {FINAL_EXPECTED_TRIALS}"
        ),
    )
    parser.add_argument(
        "--environment-manifest",
        type=Path,
        help=(
            "Final-gate provenance manifest (default: "
            f"<current-root>/{ENVIRONMENT_MANIFEST_NAME})"
        ),
    )
    parser.add_argument(
        "--minimum-leaders",
        type=int,
        default=0,
        help=(
            "Minimum co-leading trials required for every metric "
            f"(--require-leading enforces at least {FINAL_MINIMUM_LEADERS})"
        ),
    )
    parser.add_argument(
        "--minimum-critical-leaders",
        type=int,
        default=0,
        help=(
            "Minimum co-leading trials for total, seed-duration, seed-throughput, "
            "and checkpoint metrics (--require-leading enforces at least "
            f"{FINAL_MINIMUM_CRITICAL_LEADERS})"
        ),
    )
    args = parser.parse_args()
    if len(set(args.scales)) != len(args.scales):
        parser.error("--scales must contain unique values")
    if args.require_leading and set(args.scales) != EXPECTED_SCALES:
        parser.error("--require-leading requires all four canonical scales")
    if args.require_leading:
        if args.expected_trials not in (None, FINAL_EXPECTED_TRIALS):
            parser.error(
                f"--require-leading requires exactly {FINAL_EXPECTED_TRIALS} trials"
            )
        args.expected_trials = FINAL_EXPECTED_TRIALS
        args.minimum_leaders = max(args.minimum_leaders, FINAL_MINIMUM_LEADERS)
        args.minimum_critical_leaders = max(
            args.minimum_critical_leaders, FINAL_MINIMUM_CRITICAL_LEADERS
        )
    if args.expected_trials is not None and args.expected_trials < 1:
        parser.error("--expected-trials must be >= 1")
    leader_limit = args.expected_trials or args.minimum_trials
    if (
        args.minimum_trials < 1
        or args.minimum_leaders < 0
        or args.minimum_critical_leaders < 0
        or args.minimum_leaders > leader_limit
        or args.minimum_critical_leaders > leader_limit
        or (
            args.expected_trials is not None
            and args.minimum_trials > args.expected_trials
        )
    ):
        parser.error(
            "trial/leader minimums must be positive, must not exceed the exact "
            "or minimum trial count, and --minimum-trials must be >= 1"
        )

    try:
        history_runs = load_history_manifest(args.history_dir, args.history_manifest)
        current_runs = collect_candidate_runs(args.current_root)
        validate_candidate_uniqueness(current_runs)
        validate_candidate_history_disjoint(current_runs, history_runs)
        validated_environment = None
        if args.require_leading:
            environment_manifest = (
                args.environment_manifest
                or args.current_root / ENVIRONMENT_MANIFEST_NAME
            )
            validated_environment = validate_environment_manifest(
                args.current_root,
                environment_manifest,
                scales=args.scales,
                expected_trials=args.expected_trials,
            )
        if args.expected_trials is not None:
            validate_trial_matrix(
                args.current_root,
                current_runs,
                scales=args.scales,
                expected_trials=args.expected_trials,
                environment_manifest=validated_environment,
            )
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    if not history_runs:
        print(
            f"error: no manifested RustRaw/default history under {args.history_dir}",
            file=sys.stderr,
        )
        return 2

    current_by_scale = group_by_scale(current_runs)
    raw_by_scale = group_by_scale(history_runs)
    durable_by_scale = group_by_scale(
        run for run in history_runs if is_current_durability_contract(run)
    )
    metric_list = metrics()

    print("Rust baseline target cohorts")
    print(f"  raw: {len(history_runs)} manifested RustRaw/default run(s), all eras")
    print(
        "  durable: "
        f"{sum(len(runs) for runs in durable_by_scale.values())} run(s) at/after "
        f"started_unix={MAIN_DB_SYNC_DURABILITY_EPOCH_UNIX} with current contract labels"
    )
    print(
        "  ratios are current_median/target; <=1 leads lower-is-better metrics, "
        ">=1 leads higher-is-better metrics"
    )

    for scale in args.scales:
        print_target_table(
            scale,
            metric_list,
            raw_by_scale.get(scale, []),
            durable_by_scale.get(scale, []),
        )

    overall: dict[TargetCohort, bool] = {"raw": True, "durable": True}
    storage_passed = True
    for scale in args.scales:
        runs = current_by_scale.get(scale, [])
        if not runs:
            print(f"\n### {scale} current comparison\n  no current runs")
            overall["raw"] = False
            overall["durable"] = False
            storage_passed = False
            continue
        count_passed = (
            len(runs) == args.expected_trials
            if args.expected_trials is not None
            else len(runs) >= args.minimum_trials
        )
        if not count_passed:
            requirement = (
                f"exactly {args.expected_trials}"
                if args.expected_trials is not None
                else f"at least {args.minimum_trials}"
            )
            print(
                f"\n### {scale} trial-count gate\n"
                f"  found {len(runs)} trial(s), require {requirement}: FAIL"
            )
            overall["raw"] = False
            overall["durable"] = False
        passed = print_current_table(
            scale,
            metric_list,
            runs,
            raw_by_scale.get(scale, []),
            durable_by_scale.get(scale, []),
            args.minimum_leaders,
            args.minimum_critical_leaders,
        )
        overall["raw"] &= passed["raw"]
        overall["durable"] &= passed["durable"]
        storage_passed &= print_storage_constraint(
            scale,
            runs,
            storage_constraint(durable_by_scale.get(scale, [])),
        )

    print("\nSummary")
    print(f"  raw median leaders: {'PASS' if overall['raw'] else 'FAIL'}")
    print(f"  durable median leaders: {'PASS' if overall['durable'] else 'FAIL'}")
    print(f"  storage equality constraints: {'PASS' if storage_passed else 'FAIL'}")

    selected = args.target_cohort
    if args.require_leading and (not overall[selected] or not storage_passed):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
