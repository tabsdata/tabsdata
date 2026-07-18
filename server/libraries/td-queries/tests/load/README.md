<!--
Copyright 2026 Tabs Data Inc.
-->

# td-queries query load suite

A load case for **every** public query in `td-queries`: each seeds a sized
dataset for the tables that query reads, runs one cold execution, and records
wall time + row count as JSONL (`target/query-load/queries.jsonl`, override
with `TD_QUERY_LOAD_OUT`). The `audit.rs` guard fails the build if the
`cases.rs` registry drifts from the crate's query surface.

## Running

```sh
# smoke (default) — runs in the normal test path, safe for CI
cargo test -p td-queries --test load

# bigger datasets, on demand
TD_QUERY_LOAD_PROFILE=load   cargo test -p td-queries --test load -- --nocapture
TD_QUERY_LOAD_PROFILE=stress cargo test -p td-queries --test load -- --nocapture
```

## Scaling guards

`report_scaling_*` (`scaling.rs`, `dataversion_cases.rs`) seed each dataset at
two sizes, time every query at both (min of 3), and assert two things per
query: an absolute budget at both sizes (500 ms — a plan-level regression can
be born slow at every size, which the exponent alone cannot see) and
sub-quadratic growth (`k ≤ 1.6` in `t ∝ n^k`); `report_scaling_coverage`
fails if any registered case is timed by no pass. They run on every `cargo make test` /
`test_rs` (serial, after the main suite — timings need a quiet machine; skip
with `--notest-scaling`), or directly:

```sh
cargo nextest run -p td-queries --test load --run-ignored ignored-only --test-threads 1 report_scaling
```

## Regression vs baseline

`report_regression` compares the latest run against `baseline.jsonl`, a
**per-machine, gitignored** artifact (absolute timings don't transfer between
hosts; the portable teeth are the scaling guards). Both passes must be serial
— parallel contention inflates timings:

```sh
cargo test -p td-queries --test load -- --test-threads=1              # write JSONL
cargo test -p td-queries --test load -- --ignored --test-threads=1    # check

# capture / refresh the baseline (best at the load profile)
TD_QUERY_LOAD_BASELINE_UPDATE=1 cargo test -p td-queries --test load -- --ignored report_regression
```

Thresholds: `TD_QUERY_LOAD_REGRESS_PCT` (default 50) over baselines above
`TD_QUERY_LOAD_REGRESS_FLOOR_US` (default 5000).

## Adding a query

Add the method, add a `LoadCase` to `cases.rs` (the audit tells you it's
missing), and wire a `#[td_test::test(db)]` case in the matching `*_cases.rs`.
The scaling coverage guard will then demand a timing in a `time_*_queries`
pass.
