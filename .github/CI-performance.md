# CI performance experiments

Measure compilation, cache restore/save, artifact transfer, test execution, and
the full build/test
path separately. Count a candidate as validated only after its entire target
test suite passes.

## Platform checklist

For each target:

- [ ] Record a native cold build and full test run.
- [ ] Repeat on a fresh runner with a dependency cache.
- [ ] Build on Ubuntu 26.04 and run the archive on the native target runner.
- [ ] Measure cold and warm cross builds separately.
- [ ] Compare default and doubled test concurrency using the same binaries.
- [ ] Compare workspace optimization against total build-plus-test time.
- [ ] Check a small source change and default-branch cache reuse across PRs.
- [ ] Verify source revision, compiler, target, profile, features, checksum, and
  test coverage.

## Coverage so far

| Target | Native cache | Linux build + tests | Optimization | Concurrency |
| --- | --- | --- | --- | --- |
| macOS x86 | Measured | Passed on Intel and Rosetta | Measured | Measured |
| macOS ARM | Pending | Pending | Pending | Pending |
| Windows x86 MSVC | Measured | New experiment | Measured | Measured |
| Windows ARM MSVC | Investigate | New experiment | Measured | Measured |
| Linux x86 | Measured | Native Linux build | Measured | Measured |
| Linux ARM | Measured | Native Linux build | Measured | Measured |

An ARM macOS runner executing x86 binaries tests Rosetta, not native ARM
binaries.

## Windows cross-build experiment

The manually dispatched workflow `linux-windows-benchmark.yml` builds the same
source on Ubuntu
26.04 using Rust 1.97.1
and cargo-xwin 0.23.1, targeting the existing MSVC triples. It retains the
development profile,
workspace optimization level zero, static CRT, CI configuration, and test helper
feature.

Each target has a cold Linux build followed by a fresh-runner warm Linux build.
Windows executes
both archives. The cold cache namespace includes the workflow run ID, so a new
run cannot silently
restore an old dependency cache. The warm build shares that namespace. Both Rust
dependencies and
the Windows SDK are cached; workspace artifacts are excluded.

Artifacts identify the stage and target and include a JSON manifest with source
revision, build
command, compiler, profile, features, and archive checksum. Windows checks the
manifest before
listing and executing tests. Test logs and coverage metadata are retained with
matching labels.

The test path changes use runtime paths provided by nextest instead of Linux
paths embedded by
Cargo during compilation. They affect test infrastructure only. Native Windows
coverage is 3,369 executed tests plus 16 intentionally skipped tests on each
architecture; compare Windows archives with that baseline, not macOS counts.

Timing comparisons must exclude deliberate experimental sequencing and repeated
test executions.
Report complete job durations alongside individual build and test durations, and
distinguish
measured end-to-end totals from estimates assembled from separate jobs.
