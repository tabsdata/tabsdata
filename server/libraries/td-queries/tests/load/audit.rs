//
// Copyright 2026 Tabs Data Inc.
//

//! Coverage audit: the [`LOAD_CASES`] registry must name every public query
//! in the crate, and no more. This scans `src/{daos,generic}` for the
//! `pub async fn`s declared on query handles and diffs the resulting
//! `Struct::method` set against the union of every case's `covers`.
//!
//! Adding a query without a load case (or removing/renaming one without
//! updating the registry) fails this test with an explicit diff.

use crate::cases::{Family, LOAD_CASES, Status};
use regex::Regex;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

/// Directories whose `*.rs` files declare query handles. `mod.rs` files only
/// re-export, so they're skipped.
const QUERY_DIRS: &[&str] = &["src/daos", "src/generic"];

/// Walk the query-handle source and collect `Struct::method` for every
/// `pub async fn`. Typestate-split handles (the `Unbound`/`Bound` impls of
/// `Current` / `Historic` / `Versioned`) declare the same method name twice;
/// the `BTreeSet` dedups them into one logical query, matching the registry.
fn scan_surface() -> BTreeSet<String> {
    // `impl<..generics..> Type<..>` or `impl<..> Trait for Type` — capture the
    // first type identifier after the (flat) impl generic list. Trait impls
    // capture the trait name, but those carry no `pub async fn`, so they never
    // contribute a method.
    let impl_re = Regex::new(r"^\s*impl\b\s*(?:<[^>]*>)?\s*([A-Za-z_][A-Za-z0-9_]*)").unwrap();
    let fn_re = Regex::new(r"\bpub\s+async\s+fn\s+([A-Za-z_][A-Za-z0-9_]*)").unwrap();

    let crate_dir = env!("CARGO_MANIFEST_DIR");
    let mut surface = BTreeSet::new();

    for sub in QUERY_DIRS {
        let dir = Path::new(crate_dir).join(sub);
        let mut files: Vec<_> = fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("rs"))
            .filter(|p| p.file_name().and_then(|n| n.to_str()) != Some("mod.rs"))
            .collect();
        files.sort();
        for path in files {
            scan_file(&path, &impl_re, &fn_re, &mut surface);
        }
    }
    surface
}

fn scan_file(path: &Path, impl_re: &Regex, fn_re: &Regex, surface: &mut BTreeSet<String>) {
    let src = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

    // Drop the `#[cfg(test)] mod tests { ... }` tail so test-helper fns and
    // inline `pub async fn` examples can't leak into the surface.
    let body = match src.find("#[cfg(test)]") {
        Some(idx) => &src[..idx],
        None => &src[..],
    };

    let mut current_struct: Option<String> = None;
    for line in body.lines() {
        if let Some(c) = impl_re.captures(line) {
            current_struct = Some(c[1].to_string());
        }
        if let Some(c) = fn_re.captures(line) {
            let method = &c[1];
            let st = current_struct.as_deref().unwrap_or_else(|| {
                panic!(
                    "{}: `pub async fn {method}` found outside an impl block",
                    path.display()
                )
            });
            surface.insert(format!("{st}::{method}"));
        }
    }
}

#[test]
fn every_query_has_a_load_case() {
    let surface = scan_surface();
    let covered: BTreeSet<String> = LOAD_CASES
        .iter()
        .flat_map(|c| c.covers.iter().map(|s| s.to_string()))
        .collect();

    let missing: Vec<_> = surface.difference(&covered).cloned().collect();
    let extra: Vec<_> = covered.difference(&surface).cloned().collect();

    assert!(
        missing.is_empty() && extra.is_empty(),
        "load-case registry drifted from the query surface.\n\
         MISSING — query exists but no LoadCase.covers names it: {missing:#?}\n\
         EXTRA   — LoadCase.covers names a method not found in src: {extra:#?}\n\
         (surface has {} methods; registry covers {})",
        surface.len(),
        covered.len(),
    );
}

#[test]
fn case_names_are_unique() {
    let mut seen = BTreeSet::new();
    for c in LOAD_CASES {
        assert!(seen.insert(c.name), "duplicate load-case name: {}", c.name);
    }
}

#[test]
fn every_case_covers_something() {
    for c in LOAD_CASES {
        assert!(
            !c.covers.is_empty(),
            "load-case `{}` covers no query method",
            c.name
        );
    }
}

/// Prints the per-family implemented/total tally (visible under
/// `cargo test -- --nocapture`). `status` is set per case when its
/// `#[td_test::test(db)]` lands; a broken wiring fails its own case loudly,
/// so this test only reports progress rather than re-deriving it.
#[test]
fn cases_tally() {
    use std::collections::BTreeMap;

    let mut by_family: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for c in LOAD_CASES {
        let entry = by_family.entry(family_name(c.family)).or_default();
        entry.0 += 1;
        if matches!(c.status, Status::Implemented) {
            entry.1 += 1;
        }
    }
    eprintln!("load cases by family (implemented/total):");
    for (fam, (total, done)) in &by_family {
        eprintln!("  {fam:<12} {done}/{total}");
    }
    eprintln!(
        "  {:<12} {}/{}",
        "TOTAL",
        implemented_count(),
        LOAD_CASES.len()
    );
}

fn implemented_count() -> usize {
    LOAD_CASES
        .iter()
        .filter(|c| matches!(c.status, Status::Implemented))
        .count()
}

fn family_name(f: Family) -> &'static str {
    match f {
        Family::Live => "Live",
        Family::Current => "Current",
        Family::Versioned => "Versioned",
        Family::Historic => "Historic",
        Family::DataVersion => "DataVersion",
        Family::Recursive => "Recursive",
        Family::Chain => "Chain",
        Family::Pointer => "Pointer",
        Family::Metrics => "Metrics",
        Family::Sessions => "Sessions",
    }
}
