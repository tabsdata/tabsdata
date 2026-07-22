//
// Copyright 2026 Tabsdata Inc.
//

//! Metric capture: one cold execution per case → one [`MetricRow`] appended
//! as JSONL to `$TD_QUERY_LOAD_OUT` (default
//! `target/query-load/queries.jsonl`).

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;
use td_database::pool::TdPool;
use td_schema::schema::backend::Backend;

/// One id per test process — lets the report pick the most recent run out of
/// the append-only file.
pub static RUN_ID: LazyLock<i64> = LazyLock::new(now_ms);

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// One timed query execution.
pub struct MetricRow {
    pub case: &'static str,
    pub backend: &'static str,
    pub profile: &'static str,
    pub wall_us: u128,
    pub rows: usize,
    /// Seed-dataset size, for cost-per-row context.
    pub seeded: usize,
}

impl MetricRow {
    fn to_json(&self) -> String {
        json!({
            "run_id": *RUN_ID,
            "ts_ms": now_ms(),
            "case": self.case,
            "backend": self.backend,
            "profile": self.profile,
            "wall_us": self.wall_us as u64,
            "rows": self.rows,
            "seeded": self.seeded,
        })
        .to_string()
    }

    pub fn emit(&self) {
        let path = output_path();
        if let Some(dir) = path.parent() {
            let _ = fs::create_dir_all(dir);
        }
        // One write_all per line: with O_APPEND a single write is atomic, so
        // parallel emits can't interleave into corrupt rows (writeln! splits).
        let line = format!("{}\n", self.to_json());
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(mut f) => {
                let _ = f.write_all(line.as_bytes());
            }
            Err(e) => eprintln!("[query-load] could not write {}: {e}", path.display()),
        }
        eprintln!(
            "[query-load] {:<40} {:>8} backend={:<8} {:>10}us rows={} seeded={}",
            self.case, self.profile, self.backend, self.wall_us, self.rows, self.seeded
        );
    }
}

pub fn output_path() -> PathBuf {
    match std::env::var("TD_QUERY_LOAD_OUT") {
        Ok(p) => PathBuf::from(p),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../target/query-load/queries.jsonl"),
    }
}

pub fn backend_of(db: &TdPool) -> &'static str {
    match db.backend() {
        Backend::Postgres => "postgres",
        _ => "sqlite",
    }
}

pub fn micros(d: Duration) -> u128 {
    d.as_micros()
}

/// A parsed JSONL row. Run metadata (`run_id`) is defaulted because baseline
/// rows don't carry it.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct MetricRecord {
    #[serde(default)]
    pub run_id: i64,
    pub case: String,
    pub backend: String,
    pub profile: String,
    pub wall_us: u64,
    pub rows: u64,
    pub seeded: u64,
}

pub fn read_rows() -> Vec<MetricRecord> {
    let path = output_path();
    let Ok(text) = fs::read_to_string(&path) else {
        return vec![];
    };
    parse_rows(&text)
}

/// Unparseable lines are skipped.
pub fn parse_rows(text: &str) -> Vec<MetricRecord> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

pub fn latest_run(rows: &[MetricRecord]) -> Vec<MetricRecord> {
    let Some(max) = rows.iter().map(|r| r.run_id).max() else {
        return vec![];
    };
    rows.iter().filter(|r| r.run_id == max).cloned().collect()
}
