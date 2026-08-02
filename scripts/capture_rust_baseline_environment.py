#!/usr/bin/env python3
"""Capture immutable provenance for the strict rust-baseline record gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import stat
import subprocess
import tempfile
from pathlib import Path
from typing import Iterable

import agg_rust_baseline as aggregate

SOURCE_STATE_SCHEMA_VERSION = 1
COMMAND_EVIDENCE_SCHEMA_VERSION = 2
SOURCE_SUFFIXES = {
    ".c",
    ".cc",
    ".cpp",
    ".cs",
    ".css",
    ".dart",
    ".go",
    ".h",
    ".hpp",
    ".html",
    ".java",
    ".js",
    ".json",
    ".lock",
    ".md",
    ".py",
    ".rs",
    ".sh",
    ".sql",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
}
SOURCE_FILENAMES = {
    "AGENTS.md",
    "Dockerfile",
    "LICENSE",
    "Makefile",
    "README",
    "README.md",
}
EXCLUDED_COMPONENTS = {".git", ".tmp", "__pycache__", "target"}
SENSITIVE_NAMES = {
    ".env",
    "credentials",
    "credentials.json",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "id_rsa",
    "secrets.json",
}


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def run_command(repo_root: Path, args: list[str]) -> bytes:
    completed = subprocess.run(
        args,
        cwd=repo_root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return completed.stdout


def git_paths(repo_root: Path, *args: str) -> set[Path]:
    output = run_command(repo_root, ["git", "ls-files", "-z", *args])
    return {Path(os.fsdecode(raw_path)) for raw_path in output.split(b"\0") if raw_path}


def is_sensitive_name(path: Path) -> bool:
    name = path.name.lower()
    return (
        name in SENSITIVE_NAMES
        or name.startswith(".env.")
        or name.endswith((".key", ".pem", ".p12", ".pfx"))
        or "secret" in name
    )


def is_benchmark_result_artifact(path: Path) -> bool:
    parts = path.parts
    return bool(parts) and parts[0] == "benchmarks" and "results" in parts


def is_in_scope_source(path: Path) -> bool:
    if path.is_absolute() or ".." in path.parts:
        return False
    if any(part in EXCLUDED_COMPONENTS for part in path.parts):
        return False
    if is_benchmark_result_artifact(path) or is_sensitive_name(path):
        return False
    return path.name in SOURCE_FILENAMES or path.suffix.lower() in SOURCE_SUFFIXES


def source_entry(repo_root: Path, relative: Path, origin: str) -> dict[str, object]:
    path = repo_root / relative
    if path.is_symlink():
        target = os.readlink(path)
        content = os.fsencode(target)
        return {
            "path": relative.as_posix(),
            "origin": origin,
            "kind": "symlink",
            "mode": "symlink",
            "size": len(content),
            "sha256": sha256_bytes(content),
        }
    if not path.exists():
        return {
            "path": relative.as_posix(),
            "origin": origin,
            "kind": "missing",
            "mode": "missing",
            "size": 0,
            "sha256": None,
        }
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"in-scope source is not a regular file: {relative}")
    content = path.read_bytes()
    return {
        "path": relative.as_posix(),
        "origin": origin,
        "kind": "file",
        "mode": f"{stat.S_IMODE(metadata.st_mode):04o}",
        "size": len(content),
        "sha256": sha256_bytes(content),
    }


def collect_source_state(repo_root: Path, revision: str) -> dict[str, object]:
    tracked = git_paths(repo_root, "--cached")
    untracked = git_paths(repo_root, "--others", "--exclude-standard")
    selected: dict[Path, str] = {}
    for path in sorted(tracked):
        if is_in_scope_source(path):
            selected[path] = "tracked"
    for path in sorted(untracked):
        if is_in_scope_source(path):
            selected[path] = "untracked"
    return {
        "schema_version": SOURCE_STATE_SCHEMA_VERSION,
        "git_revision": revision,
        "files": [
            source_entry(repo_root, path, origin)
            for path, origin in sorted(
                selected.items(), key=lambda item: item[0].as_posix()
            )
        ],
    }


def platform_evidence(repo_root: Path) -> str:
    sections = [
        "platform.uname",
        " ".join(platform.uname()),
        "",
        "rustc -Vv",
        run_command(repo_root, ["rustc", "-Vv"])
        .decode("utf-8", errors="replace")
        .rstrip(),
        "",
        "lscpu",
    ]
    try:
        lscpu = (
            run_command(repo_root, ["lscpu"]).decode("utf-8", errors="replace").rstrip()
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        lscpu = "unavailable"
    sections.append(lscpu)
    return "\n".join(sections) + "\n"


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except BaseException:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
        raise


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def ensure_pretrial_root(current_root: Path) -> None:
    current_root.mkdir(parents=True, exist_ok=True)
    trial_entries = sorted(current_root.glob("trial-*"))
    result_entries = sorted(current_root.rglob("*rust-baseline*.json"))
    if trial_entries or result_entries:
        details = [str(path) for path in (*trial_entries, *result_entries)]
        raise ValueError(
            "environment capture must run before trials; found existing trial/result "
            + ", ".join(details)
        )


def relative_evidence_path(current_root: Path, path: Path) -> str:
    return path.resolve().relative_to(current_root.resolve()).as_posix()


def capture_environment(
    current_root: Path,
    binary: Path,
    *,
    repo_root: Path | None = None,
) -> Path:
    repo_root = (repo_root or Path(__file__).resolve().parent.parent).resolve()
    current_root = current_root.resolve()
    binary = binary.resolve()
    if not binary.is_file() or binary.is_symlink():
        raise ValueError(f"binary must be a regular non-symlink file: {binary}")
    ensure_pretrial_root(current_root)

    revision = (
        run_command(repo_root, ["git", "rev-parse", "HEAD"]).decode("ascii").strip()
    )
    if aggregate.REVISION_PATTERN.fullmatch(revision) is None:
        raise ValueError(f"git returned an invalid revision: {revision!r}")

    environment_dir = current_root / "environment"
    source_state_path = environment_dir / "source-state.json"
    command_path = environment_dir / "command.json"
    platform_path = environment_dir / "platform.txt"
    binary_digest_path = environment_dir / "binary-sha256.txt"

    source_state = collect_source_state(repo_root, revision)
    command_evidence = {
        "schema_version": COMMAND_EVIDENCE_SCHEMA_VERSION,
        "argv_template": aggregate.CANONICAL_COMMAND_TEMPLATE,
        "runner_contract_version": aggregate.RUNNER_CONTRACT_VERSION,
        "seed": aggregate.CANONICAL_SEED,
    }
    atomic_write(source_state_path, json_bytes(source_state))
    atomic_write(command_path, json_bytes(command_evidence))
    atomic_write(platform_path, platform_evidence(repo_root).encode("utf-8"))
    binary_digest = sha256_file(binary)
    atomic_write(
        binary_digest_path,
        f"{binary_digest}  {binary}\n".encode("utf-8"),
    )

    manifest = {
        "schema_version": aggregate.ENVIRONMENT_MANIFEST_SCHEMA_VERSION,
        "binary": {"path": str(binary), "sha256": binary_digest},
        "source_revision": revision,
        "source_state": {
            "path": relative_evidence_path(current_root, source_state_path),
            "sha256": sha256_file(source_state_path),
        },
        "command": {
            "path": relative_evidence_path(current_root, command_path),
            "sha256": sha256_file(command_path),
            "argv_template": aggregate.CANONICAL_COMMAND_TEMPLATE,
            "argv_template_sha256": aggregate.command_template_sha256(
                aggregate.CANONICAL_COMMAND_TEMPLATE
            ),
        },
        "platform": {
            "path": relative_evidence_path(current_root, platform_path),
            "sha256": sha256_file(platform_path),
        },
        "engine": "decentdb",
        "profile": "default",
        "scales": list(aggregate.ALL_SCALES),
        "expected_trials": aggregate.FINAL_EXPECTED_TRIALS,
        "runner_contract_version": aggregate.RUNNER_CONTRACT_VERSION,
        "seed": aggregate.CANONICAL_SEED,
    }
    manifest_path = current_root / aggregate.ENVIRONMENT_MANIFEST_NAME
    atomic_write(manifest_path, json_bytes(manifest))
    aggregate.validate_environment_manifest(
        current_root,
        manifest_path,
        scales=aggregate.ALL_SCALES,
        expected_trials=aggregate.FINAL_EXPECTED_TRIALS,
    )
    return manifest_path


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Capture strict rust-baseline provenance before any trial"
    )
    parser.add_argument("--current-root", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        manifest_path = capture_environment(args.current_root, args.binary)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(2, f"error: {error}\n")
    print(f"Wrote {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
