# td-queries query load suite

A load case for **every** public query in `td-queries`: each seeds a large
dataset for the tables that query reads, runs **one cold execution**, and
records its wall time + row count. The point is to see how each query scales
with table size, and to catch plan regressions.

## Layout

| File | What |
|------|------|
| `cases.rs` | `LOAD_CASES` — the registry: one entry per query (`name`, `covers`, `family`, `status`). |
| `audit.rs` | Coverage guard: `every_query_has_a_load_case` scans `src/{daos,generic}` and fails if the registry drifts from the public query surface. Plus uniqueness/non-empty/tally checks. |
| `profile.rs` | `SizeProfile` (smoke/load/stress) from `TD_QUERY_LOAD_PROFILE`, with per-family knobs. |
| `metric.rs` | `MetricRow` → appended as JSONL to `$TD_QUERY_LOAD_OUT` (default `target/query-load/queries.jsonl`); each row carries `run_id` + `git_sha` + `ts_ms`. Also parses the file back for the report. |
| `dataset.rs` | `metrics_dataset` and `chain_dataset` — the shared seeded graphs, built through the public Seed/Live/Current test surface. `ChainHandle` exposes representative ids each case turns into query args. |
| `report.rs` | `#[ignore]` post-run checks over the JSONL: runtime coverage + regression vs baseline. |
| `scaling.rs` | `#[ignore]` scaling guards: every query timed at two seed sizes, asserting sub-quadratic growth. (DataVersion's is in `dataversion_cases.rs` with its DAO.) |
| `baseline.jsonl` | Per-machine (gitignored) `(case,backend,profile)` wall-time baseline for the regression check. |
| `*_cases.rs` | The cases, grouped by family (metrics, chain, pointer, versioned, current/live, live writes, dataversion, recursive, list). |

Two seeding strategies:
- **Shared dataset** (`chain_dataset` / `metrics_dataset`) for families that
  read the real DAO graph (chain, pointer, versioned/historic, current, live,
  recursive, metrics, list, sessions).
- **Specialized fixture** for `DataVersion` (`dataversion_cases::LoadDvFixture`)
  — a purpose-built DAO + dedicated table sized from the profile. This is the
  template for any future family with no production DAO.

## Running

The suite is plain `#[td_test::test(db)]` tests, one arm per enabled backend:

```sh
# smoke (default) — safe for CI
cargo test -p td-queries --test load --features td-test

# load — ~hundreds of units per case; on demand
TD_QUERY_LOAD_PROFILE=load cargo test -p td-queries --test load --features td-test -- --nocapture

# stress — large; run deliberately (seeding is the slow part, see below)
TD_QUERY_LOAD_PROFILE=stress cargo test -p td-queries --test load --features td-test -- --nocapture
```

## Report (run after a full run)

The checks in `report.rs` are `#[ignore]` so they don't run mid-suite (they
read the JSONL the cases write). Run them as a second pass:

```sh
cargo test -p td-queries --test load --features td-test -- --test-threads=1             # 1. writes JSONL
cargo test -p td-queries --test load --features td-test -- --ignored --test-threads=1   # 2. checks
```

They isolate the most recent run by `run_id`, so step 2 must follow step 1.

Run the write pass **serially** (`--test-threads=1`): the cases share one
embedded postgres, so under parallel execution contention inflates the small
`smoke` timings several-fold and run-to-run — enough to trip `report_regression`
falsely. Coverage is unaffected (emits are atomic appends), but a meaningful
regression comparison needs the low-variance serial timings, and baselines are
captured the same way.

- **`report_runtime_coverage`** — every registered case emitted a row (runtime
  complement to the compile-time audit; catches a case that silently no-ops).
- **`report_regression`** — fails if any `(case, backend, profile)` is more
  than `TD_QUERY_LOAD_REGRESS_PCT`% slower (default 50) than `baseline.jsonl`.
  The baseline is a **per-machine artifact** (gitignored — absolute timings
  don't transfer between hosts; the machine-independent teeth are the scaling
  guards). Capture one, preferably at the `load` profile so timings clear the
  noise floor, or re-baseline after intentional changes:
  ```sh
  TD_QUERY_LOAD_BASELINE_UPDATE=1 cargo test -p td-queries --test load --features td-test -- --ignored report_regression
  ```
- **`report_scaling_*`** (in `scaling.rs` / `dataversion_cases.rs`) — seed each
  dataset at two sizes and time **every** query against it, asserting cost
  grows sub-quadratically (`k ≤ 1.6` in `t ∝ n^k`). One test per dataset
  (`chain` ≈40 queries, `metrics` 3, `data_version` 4) so the expensive seeding
  is amortized. Queries under a few ms at the big size are run for coverage but
  not judged (their `k` would be noise). No allow-list — every query is
  enforced.

Results land in `target/query-load/queries.jsonl` (one object per run):

```json
{"case":"metrics::metrics_edges","backend":"postgres","profile":"load","wall_us":..., "rows":..., "seeded":...}
```

`seeded` is the dominant size axis for the case, so `wall_us` vs `seeded`
across profiles is the scaling curve.

## Profiles and the seeding cost

The datasets build every row with the real DAO builders, then split inserts:
the structural backbone (collections/functions/tables/deps/executions/
transactions) goes through the per-row Seed API (each id feeds the next), while
the high-volume run graph (LFR/PFR/pointers/LTDV/PTDV/LFRI) is accumulated and
**bulk-inserted** via `bulk::bulk_insert` — chunked multi-row `INSERT`
statements. Multi-row `INSERT` (not `COPY`) is deliberate: it still fires the
table triggers that maintain the `_current` views and sticky-status, so the
seeded data is identical to the per-row path.

This makes `stress` practical. For `lfr::to_commit`:

| profile | seeded | query | whole test |
|---------|-------:|------:|-----------:|
| smoke   | 20     | 1.4ms | ~1s |
| load    | 600    | 6.9ms | ~1.7s |
| stress  | 6000   | 51ms  | ~10s |

The remaining seeding cost at `stress` is the eager per-row backbone; bulking
that too would require reconstructing the function/table/dependency builders
(which the Seed API encapsulates) and is the natural next optimization if
`stress` ever needs to be larger still.

### First catch (fixed)

The suite's first find: `metrics::metrics_nodes`/`metrics_edges` were ~O(n²) in
node count — ~280s at 15 000 nodes — because `function_runs__with_names` and
`table_data_versions__with_names` resolved the currently-bound concrete row
with a **correlated per-row subquery** in a `LEFT JOIN … ON`. Fixed in
`td-schema`: each pointer event log now maintains a `_current` snapshot table
(one row per logical id, kept by an `AFTER INSERT` trigger), so the views
resolve the binding with a plain PK-backed equi-join and never touch pointer
history at read time. Both now scale linearly (`k≈0` / `k≈0.95` at smoke→load),
and the scaling guard enforces it (allow-list empty).

## Adding a query

Add the method, then add a `LoadCase` to `cases.rs` (the audit will tell you
it's missing) and wire a `#[td_test::test(db)]` case in the matching
`*_cases.rs`, flipping the case to `Status::Implemented`.
