# Releases

Release readiness is gated through the Phase 4 workflows, benchmark binary, and
binding matrix.

## Triggering the GitHub release workflow

The GitHub release workflow in `.github/workflows/release.yml` auto-starts when
a GitHub Release is **published** (`release: types: [published]`). Creating or
pushing a version tag alone does not start the release workflows.

For a normal release:

1. Update the version and changelog following
   [`design/VERSIONING_GUIDE.md`](https://github.com/sphildreth/decentdb/blob/main/design/VERSIONING_GUIDE.md),
   complete validation, and merge the release changes into `main`.
2. Create a GitHub Release with a `vX.Y.Z` tag targeting the intended merged
   commit. GitHub can create the tag when the release is published, or the tag
   can already exist. Add the release notes from `docs/about/changelog.md`.
3. Publish the release. Saving a draft does not trigger the builds.
4. Monitor the release workflows. `Release (GitHub)` runs full validation,
   builds the native/CLI, Dart native, JDBC, and DBeaver packages, and attaches
   the completed packages to the published release. Assets arrive after the
   workflow succeeds.

Publishing also starts the NuGet and Mobile Native Artifacts workflows.
NuGet builds and publishes the .NET packages using `NUGET_API_KEY`; mobile
builds are uploaded as workflow artifacts. These workflows run independently
of the GitHub release workflow's validation and artifact builds.

For manual recovery, `release.yml` supports `workflow_dispatch`. To build all
assets and create/update a release, run it against the existing version tag
with `build_platform=all`, `run_validation=true`, and `create_release=true`.
The default `create_release=false` leaves the builds as workflow artifacts.

The primary NuGet workflow in `.github/workflows/nuget.yml` supports the
published-release trigger and a manual `workflow_dispatch` path.

For manual recovery or validation of an existing tag, start `nuget.yml` from
`main`, with:

- `release_tag` set to the existing release tag, such as `v2.1.0`
- `publish_to_nuget` left at `false` for a safe dry run that builds, packs, and
  verifies package contents without publishing

Manual runs use the selected `release_tag` in the workflow run title.

Set `publish_to_nuget` to `true` only when you intentionally want that manual
run to push packages to NuGet.org.

If you want manual recovery runs kept in a separate workflow list, the optional
`.github/workflows/nuget-manual.yml` workflow provides the same manual path in a
dedicated workflow entry.

## CI lanes

- `CI`: `.github/workflows/ci.yml`
  - format
  - clippy
  - engine tests
  - CLI tests
  - C ABI binding validation and smoke matrix
  - rustdoc + doctests
- `Nightly Extended`: `.github/workflows/nightly-extended.yml`
  - workspace clippy and tests
  - storage soak harness
  - release benchmark run
  - full binding matrix
  - docs build

## Release benchmark workloads

The benchmark binary lives at:

```text
crates/decentdb/benches/release_metrics.rs
```

It produces named metrics for:
- point lookup
- FK join expansion
- trigram-backed substring search
- bulk load
- crash recovery / reopen

Run locally with:

```bash
cargo bench -p decentdb --bench release_metrics
```

## Binding verification

The Phase 4 compatibility matrix is documented in:

```text
docs/api/bindings-matrix.md
```

Each listed binding is validated or smoke-tested directly against the stable C ABI before release.

## Release checklist

1. `CI` is green on `main`.
2. The nightly soak and benchmark jobs are green.
3. The benchmark output is captured for the named workloads.
4. The binding matrix remains green.
5. The docs build cleanly.
