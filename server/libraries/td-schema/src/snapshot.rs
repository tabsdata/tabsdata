//
// Copyright 2026 Tabsdata Inc.
//

use std::path::Path;

const HEADER: &str = "-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.\n\
                      -- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.\n\n";

const REFRESH: &str = "If intentional, rerun this test with TD_SNAPSHOT_UPDATE=1 to refresh the snapshots.";

/// Assert the built DDL matches the committed `postgres.sql` under `dir`,
/// panicking with a statement-level diff on drift. With the
/// `TD_SNAPSHOT_UPDATE` env var set, rewrites the snapshot instead.
pub fn assert_snapshot(dir: &Path, build: impl Fn() -> Vec<String>) {
    let path = dir.join("postgres.sql");
    let actual = render(&build());
    if std::env::var_os("TD_SNAPSHOT_UPDATE").is_some() {
        std::fs::create_dir_all(dir).expect("create snapshot dir");
        std::fs::write(&path, &actual).expect("write snapshot");
        println!("wrote {}", path.display());
        return;
    }
    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("missing DDL snapshot at {}: {e}\n{REFRESH}", path.display()))
        .replace("\r\n", "\n");
    if actual != expected {
        panic!(
            "\n\nSchema DDL changed (snapshot out of date).\n\
             --- {path}\n+++ built\n{diff}\n{REFRESH}\n",
            path = path.display(),
            diff = line_diff(&expected, &actual),
        );
    }
}

fn render(stmts: &[String]) -> String {
    let mut out = String::from(HEADER);
    out.push_str(&stmts.join("\n\n"));
    out.push('\n');
    out
}

fn line_diff(expected: &str, actual: &str) -> String {
    let exp: Vec<&str> = expected.lines().collect();
    let act: Vec<&str> = actual.lines().collect();
    let (n, m) = (exp.len(), act.len());
    let mut lcs = vec![vec![0usize; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            lcs[i][j] = if exp[i] == act[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let (mut i, mut j) = (0, 0);
    let mut out = String::new();
    while i < n && j < m {
        if exp[i] == act[j] {
            i += 1;
            j += 1;
        } else if lcs[i + 1][j] >= lcs[i][j + 1] {
            out.push_str(&format!("- {}\n", exp[i]));
            i += 1;
        } else {
            out.push_str(&format!("+ {}\n", act[j]));
            j += 1;
        }
    }
    for e in &exp[i..] {
        out.push_str(&format!("- {e}\n"));
    }
    for a in &act[j..] {
        out.push_str(&format!("+ {a}\n"));
    }
    out
}
