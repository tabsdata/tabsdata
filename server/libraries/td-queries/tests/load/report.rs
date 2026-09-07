//
// Copyright 2026 Tabsdata Inc.
//

//! Post-run analysis over the JSONL the cases emit. `#[ignore]`: they read a
//! half-written file mid-suite, and a meaningful regression comparison needs a
//! serial (`--test-threads=1`) write pass — parallel contention inflates the
//! timings. The baseline (`baseline.jsonl`, gitignored) is a per-machine
//! artifact; the machine-independent teeth are in [`crate::scaling`]. See
//! `tests/load/README.md` for the runbook.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use crate::cases::LOAD_CASES;
use crate::metric::{self, MetricRecord};

#[test]
#[ignore = "run after a full suite run; reads the emitted JSONL"]
fn report_runtime_coverage() {
    let latest = metric::latest_run(&metric::read_rows());
    assert!(
        !latest.is_empty(),
        "no metrics found — run the full suite first (cargo test --test load)"
    );
    let seen: BTreeSet<&str> = latest.iter().map(|r| r.case.as_str()).collect();
    let missing: Vec<&str> = LOAD_CASES
        .iter()
        .map(|c| c.name)
        .filter(|n| !seen.contains(n))
        .collect();
    assert!(
        missing.is_empty(),
        "these registered cases emitted no metric in the latest run \
         (did the full suite run, or did a case silently no-op?): {missing:#?}"
    );
}

fn baseline_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/load/baseline.jsonl")
}

fn regression_pct() -> f64 {
    std::env::var("TD_QUERY_LOAD_REGRESS_PCT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50.0)
}

/// Baselines below this (µs) are noise-dominated and not judged; at `smoke`
/// almost everything is below it, so the guard is meaningful at `load`/`stress`.
fn regression_floor_us() -> f64 {
    std::env::var("TD_QUERY_LOAD_REGRESS_FLOOR_US")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000.0)
}

#[test]
#[ignore = "run after a full suite run; compares the JSONL to baseline.jsonl"]
fn report_regression() {
    let latest = metric::latest_run(&metric::read_rows());
    assert!(
        !latest.is_empty(),
        "no metrics found — run the full suite first (cargo test --test load)"
    );

    if std::env::var("TD_QUERY_LOAD_BASELINE_UPDATE").as_deref() == Ok("1") {
        write_baseline(&latest).expect("write baseline");
        eprintln!(
            "[query-load] wrote baseline ({} rows) to {}",
            latest.len(),
            baseline_path().display()
        );
        return;
    }

    let baseline = match fs::read_to_string(baseline_path()) {
        Ok(t) => metric::parse_rows(&t),
        Err(_) => {
            eprintln!(
                "[query-load] no baseline at {} — capture one with \
                 TD_QUERY_LOAD_BASELINE_UPDATE=1",
                baseline_path().display()
            );
            return;
        }
    };

    let pct = regression_pct();
    let floor = regression_floor_us();
    let mut violations = Vec::new();
    for cur in &latest {
        // The baseline may predate a case.
        let Some(base) = baseline
            .iter()
            .find(|b| b.case == cur.case && b.profile == cur.profile)
        else {
            continue;
        };
        if (base.wall_us as f64) < floor {
            continue;
        }
        let limit = base.wall_us as f64 * (1.0 + pct / 100.0);
        if (cur.wall_us as f64) > limit {
            violations.push(format!(
                "{} [{}]: {}us vs baseline {}us (+{:.0}% allowed)",
                cur.case, cur.profile, cur.wall_us, base.wall_us, pct
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "query-load regressions (> {pct:.0}% slower than baseline):\n{}",
        violations.join("\n")
    );
}

fn write_baseline(rows: &[MetricRecord]) -> std::io::Result<()> {
    let mut body = String::new();
    for r in rows {
        body.push_str(
            &serde_json::json!({
                "case": r.case,
                "profile": r.profile,
                "wall_us": r.wall_us,
                "rows": r.rows,
                "seeded": r.seeded,
            })
            .to_string(),
        );
        body.push('\n');
    }
    fs::write(baseline_path(), body)
}
