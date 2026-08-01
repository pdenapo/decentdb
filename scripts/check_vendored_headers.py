#!/usr/bin/env python3
"""Check that vendored copies of the stable C ABI header are in sync.

The canonical header is include/decentdb.h. Some bindings vendor a byte-for-byte
copy for their build toolchains; those copies must not drift stale.

Exits 0 when every vendored copy matches, 1 otherwise.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CANONICAL = ROOT / "include" / "decentdb.h"
VENDORED = [
    ROOT / "bindings" / "go" / "decentdb-go" / "decentdb.h",
    ROOT / "bindings" / "dart" / "native" / "decentdb.h",
]


def main() -> int:
    canonical = CANONICAL.read_bytes()
    stale = [p for p in VENDORED if not p.is_file() or p.read_bytes() != canonical]
    if not stale:
        print(f"OK: {len(VENDORED)} vendored header(s) match {CANONICAL.relative_to(ROOT)}")
        return 0
    for path in stale:
        rel = path.relative_to(ROOT)
        if path.is_file():
            print(f"STALE: {rel} differs from include/decentdb.h", file=sys.stderr)
        else:
            print(f"MISSING: {rel} does not exist", file=sys.stderr)
    print(
        "Refresh with: cp include/decentdb.h <vendored-path>",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
