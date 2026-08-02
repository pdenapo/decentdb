from __future__ import annotations

import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "scripts" / "agg_rust_baseline.py"
SPEC = importlib.util.spec_from_file_location("agg_rust_baseline", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
agg = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = agg
SPEC.loader.exec_module(agg)


def canonical_report(
    database_path: Path,
    *,
    scale: str = "smoke",
    started_unix: int = agg.MAIN_DB_SYNC_DURABILITY_EPOCH_UNIX + 100,
    step_order: list[str] | None = None,
    result_schema_version: int = agg.CURRENT_CONTRACT_FIELDS["result_schema_version"],
) -> dict:
    artists, albums, song_cap, songs = agg.EXPECTED_WORKLOADS[scale]
    order = step_order or list(agg.STEP_ORDER)
    database_size = {
        "smoke": 1_482_752,
        "medium": 15_314_944,
        "full": 161_189_888,
        "huge": 833_892_352,
    }[scale]
    records_by_step = {
        "seed_artists": artists,
        "seed_albums": albums,
        "seed_songs": songs,
    }
    steps = []
    for index, name in enumerate(order):
        duration = 0.001 + index * 0.0001
        records = records_by_step.get(name)
        extra = {}
        if name == "checkpoint_after_seed":
            extra = {
                "checkpoint_mode": "wal",
                "database_bytes_before": 8192,
                "database_bytes_after": database_size,
                "wal_bytes_before": 16_777_216,
                "wal_bytes_after": 32,
            }
        elif name == "query_count_songs":
            extra = {"count": songs, "row_count": 1}
        elif name == "query_aggregate_durations":
            extra = {
                "row_count": 1,
                "song_count": songs,
                "duration_sum": songs * 200,
                "duration_average": 200.0,
                "duration_minimum": 100,
                "duration_maximum": 300,
            }
        elif name == "query_artist_by_id":
            artist_id = artists // 2 + 1
            extra = {
                "row_count": 1,
                "target_artist_id": artist_id,
                "artist_id": artist_id,
                "artist_name": f"Artist {artist_id}",
            }
        elif name in (
            "query_top10_artists_by_songs",
            "query_top10_albums_by_songs",
        ):
            extra = {"row_count": 10}
        elif name == "query_view_first_1000":
            extra = {"row_count": min(1000, songs)}
        elif name == "query_songs_for_artist_via_view":
            extra = {"row_count": 5}
        if name in agg.QUERY_STEPS:
            extra["semantic_checksum_sha256"] = hashlib.sha256(
                f"{scale}:{name}".encode()
            ).hexdigest()
        steps.append(
            {
                "name": name,
                "duration_seconds": duration,
                "records": records,
                "records_per_second": records / duration if records else None,
                "rss_bytes": 10_000_000 + index * 4096,
                "rss_anon_kb": 1000 + index,
                "rss_file_kb": 2000 + index,
                "extra": extra,
            }
        )
    report = {
        **agg.CURRENT_CONTRACT_FIELDS,
        "result_schema_version": result_schema_version,
        "runner_contract_version": agg.RUNNER_CONTRACT_VERSION,
        "seed": agg.CANONICAL_SEED,
        "invocation_argv": [
            "/test/rust-baseline",
            "--engine",
            "decentdb",
            "--profile",
            "default",
            "--scale",
            scale,
            "--out-dir",
            str(database_path.resolve().parent),
            "--db-path",
            str(database_path.resolve()),
        ],
        "executable_sha256": "b" * 64,
        "compiled_optional_features": [],
        "checkpoint_durability_contract": agg.CHECKPOINT_DURABILITY_CONTRACT,
        "scale_name": scale,
        "target_artists": artists,
        "target_albums": albums,
        "target_songs_cap": song_cap,
        "started_unix": started_unix,
        "finished_unix": started_unix + 1,
        "engine_version": "test-engine",
        "database_path": str(database_path.resolve()),
        "database_size_bytes": database_size,
        "wal_size_bytes": 32,
        "peak_rss_bytes": max(step["rss_bytes"] for step in steps),
        "steps": steps,
        "latency_cases": [],
        "concurrency_cases": [],
        "write_cases": [],
        "cold_cases": [],
    }
    return report


def legacy_report(database_path: Path, *, started_unix: int = 100) -> dict:
    report = canonical_report(
        database_path,
        started_unix=started_unix,
        step_order=list(agg.LEGACY_STEP_ORDER),
    )
    for field in agg.CURRENT_CONTRACT_FIELDS:
        if field != "binding":
            report.pop(field, None)
    report.pop("compiled_optional_features")
    report.pop("checkpoint_durability_contract")
    return report


def write_report(path: Path, report: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return path


def write_history_manifest(history_dir: Path, paths: list[Path]) -> Path:
    manifest = {
        "schema_version": agg.HISTORY_MANIFEST_SCHEMA_VERSION,
        "files": [
            {"path": path.name, "sha256": agg.sha256_file(path)} for path in paths
        ],
    }
    path = history_dir / agg.HISTORY_MANIFEST_NAME
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return path


def make_matrix(root: Path, *, trials: int, scales: tuple[str, ...]) -> list[agg.Run]:
    paths = []
    ordinal = 0
    for trial in range(1, trials + 1):
        for scale in scales:
            ordinal += 1
            cell = root / f"trial-{trial:02d}" / scale
            report = canonical_report(
                cell / f"run-rust-{scale}.ddb",
                scale=scale,
                started_unix=agg.MAIN_DB_SYNC_DURABILITY_EPOCH_UNIX + 100 + ordinal,
            )
            paths.append(
                write_report(
                    cell / f"2026-08-01-rust-baseline-default-{scale}.json",
                    report,
                )
            )
    return [agg.load_run(path) for path in paths]


class AggregateRustBaselineTests(unittest.TestCase):
    def test_repository_frozen_manifest_is_valid(self) -> None:
        history_dir = REPO_ROOT / "benchmarks" / "rust-baseline" / "results"
        runs = agg.load_history_manifest(history_dir)
        self.assertEqual(len(runs), 95)

    def test_manifest_ignores_unmanifested_and_rejects_changed_entry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            history = Path(temp)
            frozen = write_report(
                history / "frozen-rust-baseline.json",
                canonical_report(history / "frozen.ddb", result_schema_version=2),
            )
            ambient_report = canonical_report(
                history / "ambient.ddb",
                started_unix=agg.MAIN_DB_SYNC_DURABILITY_EPOCH_UNIX + 200,
            )
            ambient_report["steps"][0]["duration_seconds"] /= 100
            write_report(history / "ambient-rust-baseline.json", ambient_report)
            write_history_manifest(history, [frozen])

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                runs = agg.load_history_manifest(history)
            self.assertEqual([run.path.name for run in runs], [frozen.name])
            self.assertIn("ignoring 1 unmanifested", stderr.getvalue())

            frozen.write_text(
                frozen.read_text(encoding="utf-8") + " ", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                agg.load_history_manifest(history)

    def test_manifest_hard_fails_corrupt_history(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            history = Path(temp)
            report = canonical_report(history / "bad.ddb", result_schema_version=2)
            report["steps"][0], report["steps"][1] = (
                report["steps"][1],
                report["steps"][0],
            )
            path = write_report(history / "bad-rust-baseline.json", report)
            write_history_manifest(history, [path])
            with self.assertRaisesRegex(ValueError, "step order"):
                agg.load_history_manifest(history)

    def test_candidate_history_content_and_identity_must_be_disjoint(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            history_path = write_report(
                root / "history" / "record-rust-baseline.json",
                canonical_report(root / "record.ddb"),
            )
            candidate_path = root / "candidate" / "copy-rust-baseline.json"
            candidate_path.parent.mkdir()
            candidate_path.write_bytes(history_path.read_bytes())
            history = [agg.load_run(history_path)]
            candidates = [agg.load_run(candidate_path)]
            with self.assertRaisesRegex(ValueError, "duplicates manifested history"):
                agg.validate_candidate_history_disjoint(candidates, history)

            rewritten = json.loads(candidate_path.read_text(encoding="utf-8"))
            candidate_path.write_text(json.dumps(rewritten), encoding="utf-8")
            candidates = [agg.load_run(candidate_path)]
            with self.assertRaisesRegex(ValueError, "run identity"):
                agg.validate_candidate_history_disjoint(candidates, history)

    def test_candidate_requires_exact_order_features_and_durability_marker(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            base = canonical_report(root / "run.ddb")
            cases = {
                "step order": lambda report: report["steps"].reverse(),
                "compiled_optional_features": lambda report: report.update(
                    compiled_optional_features=["extended-suites"]
                ),
                "checkpoint_durability_contract": lambda report: report.update(
                    checkpoint_durability_contract=""
                ),
            }
            for expected, mutate in cases.items():
                with self.subTest(expected=expected):
                    report = copy.deepcopy(base)
                    mutate(report)
                    path = write_report(root / f"{expected}-rust-baseline.json", report)
                    with self.assertRaisesRegex(ValueError, expected):
                        agg.validate_candidate_run(agg.load_run(path))

    def test_candidate_rejects_missing_or_forged_semantic_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            base = canonical_report(root / "run.ddb")
            cases = {
                "row_count": lambda report: report["steps"][6]["extra"].pop(
                    "row_count"
                ),
                "semantic_checksum_sha256": lambda report: report["steps"][7][
                    "extra"
                ].update(semantic_checksum_sha256="not-a-digest"),
                "exactly 10": lambda report: report["steps"][9]["extra"].update(
                    row_count=9
                ),
                "aggregate query evidence": lambda report: report["steps"][7][
                    "extra"
                ].update(duration_sum=1),
            }
            for expected, mutate in cases.items():
                with self.subTest(expected=expected):
                    report = copy.deepcopy(base)
                    mutate(report)
                    path = write_report(root / f"{expected}-rust-baseline.json", report)
                    with self.assertRaisesRegex(ValueError, expected):
                        agg.validate_candidate_run(agg.load_run(path))

    def test_candidate_rejects_invalid_provenance_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            base = canonical_report(root / "run.ddb")
            cases = {
                "runner_contract_version": lambda report: report.update(
                    runner_contract_version=0
                ),
                "seed": lambda report: report.update(seed=-1),
                "invocation_argv": lambda report: report.update(invocation_argv=[]),
                "executable_sha256": lambda report: report.update(
                    executable_sha256="bad"
                ),
            }
            for expected, mutate in cases.items():
                with self.subTest(expected=expected):
                    report = copy.deepcopy(base)
                    mutate(report)
                    path = write_report(root / f"{expected}-rust-baseline.json", report)
                    with self.assertRaisesRegex(ValueError, expected):
                        agg.validate_candidate_run(agg.load_run(path))

    def test_metric_integrity_rejects_throughput_peak_and_checkpoint_forgery(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cases = {
                "records_per_second": lambda report: report["steps"][2].update(
                    records_per_second=1e99
                ),
                "peak_rss_bytes": lambda report: report.update(peak_rss_bytes=1),
                "database_bytes_after": lambda report: report["steps"][5][
                    "extra"
                ].update(database_bytes_after=1),
                "database must not shrink": lambda report: report["steps"][5][
                    "extra"
                ].update(database_bytes_before=report["database_size_bytes"] + 1),
                "WAL must shrink": lambda report: report["steps"][5]["extra"].update(
                    wal_bytes_before=32
                ),
            }
            for expected, mutate in cases.items():
                with self.subTest(expected=expected):
                    report = canonical_report(root / "run.ddb")
                    mutate(report)
                    path = write_report(root / f"{expected}-rust-baseline.json", report)
                    with self.assertRaisesRegex(ValueError, expected):
                        agg.validate_candidate_run(agg.load_run(path))

    def test_total_target_excludes_legacy_partial_total(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            legacy_path = write_report(
                root / "legacy-rust-baseline.json", legacy_report(root / "legacy.ddb")
            )
            current_path = write_report(
                root / "current-rust-baseline.json",
                canonical_report(root / "current.ddb"),
            )
            legacy = agg.load_run(legacy_path)
            current = agg.load_run(current_path)
            agg.validate_history_run(legacy)
            metric = next(
                metric for metric in agg.metrics() if metric.name == "total_runtime"
            )
            target = agg.best_target([legacy, current], metric)
            self.assertIsNotNone(target)
            self.assertEqual(target.source, current_path)

    def test_exact_trial_matrix_accepts_valid_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            runs = make_matrix(root, trials=2, scales=("smoke", "medium"))
            agg.validate_candidate_uniqueness(runs)
            agg.validate_trial_matrix(
                root, runs, scales=("smoke", "medium"), expected_trials=2
            )

    def test_trial_matrix_rejects_inconsistent_query_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            runs = make_matrix(root, trials=2, scales=("smoke",))
            runs[1].steps["query_view_first_1000"]["extra"][
                "semantic_checksum_sha256"
            ] = ("c" * 64)
            with self.assertRaisesRegex(ValueError, "semantic evidence changed"):
                agg.validate_trial_matrix(
                    root, runs, scales=("smoke",), expected_trials=2
                )

    def test_strict_matrix_rejects_wrong_seed_argv_and_binary_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / "environment" / "rust-baseline"
            binary.parent.mkdir()
            binary.write_bytes(b"frozen executable")
            binary_digest = agg.sha256_file(binary)
            environment_manifest = {
                "binary": {"path": str(binary), "sha256": binary_digest}
            }
            cases = {
                "seed mismatch": lambda report: report.update(seed=41),
                "invocation_argv mismatch": lambda report: report[
                    "invocation_argv"
                ].append("--unexpected"),
                "executable digest mismatch": lambda report: report.update(
                    executable_sha256="d" * 64
                ),
            }
            for expected, mutate in cases.items():
                with self.subTest(expected=expected):
                    runs = make_matrix(root, trials=1, scales=("smoke",))
                    run = runs[0]
                    trial_dir = run.path.parent.resolve()
                    db_path = Path(run.report["database_path"]).resolve()
                    run.report["invocation_argv"] = [
                        argument.format_map(
                            {
                                "binary": str(binary.resolve()),
                                "scale": "smoke",
                                "trial_dir": str(trial_dir),
                                "db_path": str(db_path),
                            }
                        )
                        for argument in agg.CANONICAL_COMMAND_TEMPLATE
                    ]
                    run.report["executable_sha256"] = binary_digest
                    mutate(run.report)
                    with self.assertRaisesRegex(ValueError, expected):
                        agg.validate_trial_matrix(
                            root,
                            runs,
                            scales=("smoke",),
                            expected_trials=1,
                            environment_manifest=environment_manifest,
                        )

    def test_exact_trial_matrix_rejects_missing_extra_and_duplicate_cells(self) -> None:
        for case in ("missing", "extra", "duplicate"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                runs = make_matrix(root, trials=2, scales=("smoke",))
                if case == "missing":
                    runs[-1].path.unlink()
                    runs = runs[:-1]
                elif case == "extra":
                    (root / "trial-03").mkdir()
                else:
                    original = runs[0]
                    report = canonical_report(
                        Path(original.report["database_path"]),
                        started_unix=agg.MAIN_DB_SYNC_DURABILITY_EPOCH_UNIX + 999,
                    )
                    duplicate_path = write_report(
                        original.path.parent / "duplicate-rust-baseline.json", report
                    )
                    runs.append(agg.load_run(duplicate_path))
                with self.assertRaises(ValueError):
                    agg.validate_trial_matrix(
                        root, runs, scales=("smoke",), expected_trials=2
                    )

    def test_environment_manifest_validates_all_evidence_digests(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            environment = root / "environment"
            environment.mkdir()
            binary = environment / "rust-baseline"
            source_state = environment / "source-state.txt"
            command = environment / "command.txt"
            platform = environment / "platform.txt"
            for path, content in (
                (binary, b"binary"),
                (source_state, b"revision and status"),
                (command, b"canonical command"),
                (platform, b"uname and lscpu"),
            ):
                path.write_bytes(content)
            manifest = {
                "schema_version": agg.ENVIRONMENT_MANIFEST_SCHEMA_VERSION,
                "binary": {"path": str(binary), "sha256": agg.sha256_file(binary)},
                "source_revision": "a" * 40,
                "source_state": {
                    "path": str(source_state.relative_to(root)),
                    "sha256": agg.sha256_file(source_state),
                },
                "command": {
                    "path": str(command.relative_to(root)),
                    "sha256": agg.sha256_file(command),
                    "argv_template": agg.CANONICAL_COMMAND_TEMPLATE,
                    "argv_template_sha256": agg.command_template_sha256(
                        agg.CANONICAL_COMMAND_TEMPLATE
                    ),
                },
                "platform": {
                    "path": str(platform.relative_to(root)),
                    "sha256": agg.sha256_file(platform),
                },
                "engine": "decentdb",
                "profile": "default",
                "scales": list(agg.ALL_SCALES),
                "expected_trials": agg.FINAL_EXPECTED_TRIALS,
                "runner_contract_version": agg.RUNNER_CONTRACT_VERSION,
                "seed": agg.CANONICAL_SEED,
            }
            manifest_path = root / agg.ENVIRONMENT_MANIFEST_NAME
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            agg.validate_environment_manifest(
                root,
                manifest_path,
                scales=agg.ALL_SCALES,
                expected_trials=agg.FINAL_EXPECTED_TRIALS,
            )
            platform.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "platform digest mismatch"):
                agg.validate_environment_manifest(
                    root,
                    manifest_path,
                    scales=agg.ALL_SCALES,
                    expected_trials=agg.FINAL_EXPECTED_TRIALS,
                )

    def test_empty_and_duplicate_scales_are_parser_errors(self) -> None:
        cases = [
            ["agg", "--current-root", "missing", "--scales"],
            [
                "agg",
                "--current-root",
                "missing",
                "--scales",
                "smoke",
                "smoke",
            ],
        ]
        for argv in cases:
            with self.subTest(argv=argv), unittest.mock.patch.object(sys, "argv", argv):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(
                    SystemExit
                ):
                    agg.main()

    def test_require_leading_cannot_pass_without_current_trials(self) -> None:
        history_dir = REPO_ROOT / "benchmarks" / "rust-baseline" / "results"
        argv = [
            "agg",
            "--current-root",
            "/definitely/missing/rust-baseline-current",
            "--history-dir",
            str(history_dir),
            "--require-leading",
        ]
        with unittest.mock.patch.object(sys, "argv", argv):
            with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(
                io.StringIO()
            ):
                self.assertEqual(agg.main(), 2)


if __name__ == "__main__":
    unittest.main()
