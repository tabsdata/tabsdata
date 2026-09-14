//
// Copyright 2026 Tabsdata Inc.
//

//! Coverage audit: scans all of `src/` for the public query surface and diffs
//! it against the union of every case's `covers` minus [`EXEMPT`], so the
//! [`LOAD_CASES`] registry cannot drift from the crate.
//!
//! A query is a public method that awaits: an `async fn` on a handle's impl,
//! or a `-> impl Future` method on a public trait (how the [`Queries`] entry
//! surface is written, since a trait method taking `self` by value cannot be
//! `async fn` and stay object-safe-ish under RPIT). Methods returning a
//! handle are not queries — the handle's own methods are scanned instead.

use crate::cases::{Family, LOAD_CASES, Status};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use syn::{Attribute, ImplItem, Item, ReturnType, TraitItem, Type, TypeParamBound, Visibility};

/// Test-only subdirs of `src/`; everything else is scanned recursively.
const SKIP_DIRS: &[&str] = &["test_utils", "fixtures"];

/// Queries with no load case, each with a reason (`Type::*` matches every
/// method on the type; stale entries fail the audit):
/// - `initialize_logical_ids`: one-shot startup bootstrap, not request-path.
const EXEMPT: &[&str] = &["initialize_logical_ids"];

fn is_exempt(key: &str) -> bool {
    EXEMPT.iter().any(|e| match e.strip_suffix("::*") {
        Some(ty) => key
            .strip_prefix(ty)
            .is_some_and(|rest| rest.starts_with("::")),
        None => key == *e,
    })
}

/// Collect `Struct::method` for every public query, keyed by the impl block's
/// self type or the trait's name (free fns get a bare name). Typestate-split
/// handles declare the same method twice; the `BTreeSet` dedups them into one
/// logical query, matching the registry. Trait *impl* methods have inherited
/// visibility, so they never contribute — only the trait's own declarations
/// do.
fn scan_surface() -> BTreeSet<String> {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files: Vec<PathBuf> = Vec::new();
    let mut stack = vec![src];
    while let Some(dir) = stack.pop() {
        for entry in
            fs::read_dir(&dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
        {
            let path = entry.unwrap().path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if path.is_dir() {
                if !SKIP_DIRS.contains(&name) {
                    stack.push(path);
                }
            } else if name.ends_with(".rs") {
                files.push(path);
            }
        }
    }
    files.sort();

    let mut surface = BTreeSet::new();
    for path in &files {
        let src =
            fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let file =
            syn::parse_file(&src).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
        collect_items(&file.items, &mut surface);
    }
    surface
}

fn collect_items(items: &[Item], surface: &mut BTreeSet<String>) {
    for item in items {
        match item {
            Item::Fn(f) if !is_cfg_test(&f.attrs) => {
                if is_pub_async(&f.vis, f.sig.asyncness) {
                    surface.insert(f.sig.ident.to_string());
                }
            }
            Item::Impl(imp) if !is_cfg_test(&imp.attrs) => {
                let Type::Path(ty) = imp.self_ty.as_ref() else {
                    continue;
                };
                let ty = ty.path.segments.last().unwrap().ident.to_string();
                for ii in &imp.items {
                    if let ImplItem::Fn(m) = ii
                        && !is_cfg_test(&m.attrs)
                        && is_pub_async(&m.vis, m.sig.asyncness)
                    {
                        surface.insert(format!("{ty}::{}", m.sig.ident));
                    }
                }
            }
            Item::Trait(t) if !is_cfg_test(&t.attrs) => {
                if !matches!(t.vis, Visibility::Public(_)) {
                    continue;
                }
                let ty = t.ident.to_string();
                for ti in &t.items {
                    if let TraitItem::Fn(m) = ti
                        && !is_cfg_test(&m.attrs)
                        && (m.sig.asyncness.is_some() || returns_future(&m.sig.output))
                    {
                        surface.insert(format!("{ty}::{}", m.sig.ident));
                    }
                }
            }
            Item::Mod(m) if !is_cfg_test(&m.attrs) => {
                if let Some((_, items)) = &m.content {
                    collect_items(items, surface);
                }
            }
            _ => {}
        }
    }
}

fn is_pub_async(vis: &Visibility, asyncness: Option<syn::token::Async>) -> bool {
    matches!(vis, Visibility::Public(_)) && asyncness.is_some()
}

/// `-> impl Future<..>`, the trait-method spelling of an `async fn`.
fn returns_future(output: &ReturnType) -> bool {
    let ReturnType::Type(_, ty) = output else {
        return false;
    };
    let Type::ImplTrait(imp) = ty.as_ref() else {
        return false;
    };
    imp.bounds.iter().any(|b| match b {
        TypeParamBound::Trait(t) => t.path.segments.last().is_some_and(|s| s.ident == "Future"),
        _ => false,
    })
}

/// `cfg(test)` and any `cfg(any(test, ..))` gate: test-only code is not
/// request-path surface.
fn is_cfg_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().is_ident("cfg")
            && a.meta.require_list().is_ok_and(|list| {
                list.tokens
                    .to_string()
                    .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .any(|word| word == "test")
            })
    })
}

#[test]
fn every_query_has_a_load_case() {
    let surface = scan_surface();
    let covered: BTreeSet<String> = LOAD_CASES
        .iter()
        .flat_map(|c| c.covers.iter().map(|s| s.to_string()))
        .collect();

    let stale: Vec<&str> = EXEMPT
        .iter()
        .copied()
        .filter(|e| {
            let probe = e.strip_suffix("::*").unwrap_or(e);
            !surface.iter().any(|k| is_exempt(k) && k.starts_with(probe))
        })
        .collect();
    assert!(
        stale.is_empty(),
        "EXEMPT names a query not present in src (stale waiver): {stale:#?}",
    );

    let effective: BTreeSet<String> = surface.iter().filter(|k| !is_exempt(k)).cloned().collect();
    let missing: Vec<_> = effective.difference(&covered).cloned().collect();
    let extra: Vec<_> = covered.difference(&surface).cloned().collect();

    assert!(
        missing.is_empty() && extra.is_empty(),
        "load-case registry drifted from the query surface.\n\
         MISSING — query exists (and isn't EXEMPT) but no LoadCase.covers names it: {missing:#?}\n\
         EXTRA   — LoadCase.covers names a method not found in src: {extra:#?}\n\
         (surface has {} methods, {} exempt; registry covers {})",
        surface.len(),
        surface.iter().filter(|k| is_exempt(k)).count(),
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

/// Prints the per-family implemented/total tally under `--nocapture`.
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
        Family::Default => "Default",
        Family::Latest => "Current",
        Family::At => "At",
        Family::AsOf => "FindAt",
        Family::DataVersion => "DataVersion",
        Family::Recursive => "Recursive",
        Family::Chain => "Chain",
        Family::Attempts => "Attempts",
        Family::History => "Versions",
        Family::Chunked => "Chunked",
        Family::Metrics => "Metrics",
        Family::Sessions => "Sessions",
    }
}
