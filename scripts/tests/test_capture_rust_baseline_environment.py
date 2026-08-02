from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


aggregate = load_module("agg_rust_baseline", SCRIPTS_DIR / "agg_rust_baseline.py")
capture = load_module(
    "capture_rust_baseline_environment",
    SCRIPTS_DIR / "capture_rust_baseline_environment.py",
)


def git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


class CaptureRustBaselineEnvironmentTests(unittest.TestCase):
    def test_capture_includes_source_and_excludes_artifacts_and_sensitive_files(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp)
            git(repo, "init", "-q")
            git(repo, "config", "user.email", "benchmark@example.invalid")
            git(repo, "config", "user.name", "Benchmark Test")

            (repo / "src").mkdir()
            (repo / "src" / "tracked.rs").write_text("fn tracked() {}\n")
            (repo / "README.md").write_text("tracked docs\n")
            git(repo, "add", "src/tracked.rs", "README.md")
            git(repo, "commit", "-qm", "fixture")

            (repo / "src" / "untracked.py").write_text("VALUE = 1\n")
            (repo / ".env").write_text("TOKEN=do-not-record\n")
            (repo / "target").mkdir()
            (repo / "target" / "generated.rs").write_text("generated\n")
            results = repo / "benchmarks" / "rust-baseline" / "results"
            results.mkdir(parents=True)
            (results / "record.json").write_text("{}\n")

            binary = repo / "target" / "release" / "rust-baseline"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"frozen binary")
            current_root = repo / ".tmp" / "evidence" / "trials"

            with unittest.mock.patch.object(
                capture, "platform_evidence", return_value="test platform\n"
            ):
                manifest_path = capture.capture_environment(
                    current_root,
                    binary,
                    repo_root=repo,
                )

            aggregate.validate_environment_manifest(
                current_root,
                manifest_path,
                scales=aggregate.ALL_SCALES,
                expected_trials=aggregate.FINAL_EXPECTED_TRIALS,
            )
            manifest = json.loads(manifest_path.read_text())
            source_path = current_root / manifest["source_state"]["path"]
            source_state = json.loads(source_path.read_text())
            entries = {entry["path"]: entry for entry in source_state["files"]}
            self.assertEqual(entries["src/tracked.rs"]["origin"], "tracked")
            self.assertEqual(entries["src/untracked.py"]["origin"], "untracked")
            self.assertNotIn(".env", entries)
            self.assertNotIn("target/generated.rs", entries)
            self.assertNotIn("benchmarks/rust-baseline/results/record.json", entries)
            serialized = source_path.read_text()
            self.assertNotIn("do-not-record", serialized)
            self.assertEqual(
                manifest["command"]["argv_template"],
                aggregate.CANONICAL_COMMAND_TEMPLATE,
            )

    def test_capture_refuses_existing_trials(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp)
            git(repo, "init", "-q")
            git(repo, "config", "user.email", "benchmark@example.invalid")
            git(repo, "config", "user.name", "Benchmark Test")
            (repo / "README.md").write_text("fixture\n")
            git(repo, "add", "README.md")
            git(repo, "commit", "-qm", "fixture")
            binary = repo / "rust-baseline"
            binary.write_bytes(b"binary")
            current_root = repo / "evidence"
            (current_root / "trial-01").mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "before trials"):
                capture.capture_environment(current_root, binary, repo_root=repo)


if __name__ == "__main__":
    unittest.main()
