//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn live(pairs: &[(&str, &str)]) -> BTreeMap<String, Vec<u8>> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.as_bytes().to_vec()))
        .collect()
}

fn generators(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

#[test]
fn generated_empty_manifest_with_live_non_empty_preserves() {
    let manifest = map(&[("JWT__SECRET", "")]);
    let live_map = live(&[("JWT__SECRET", "uuid-xyz")]);
    let gens = generators(&["JWT__SECRET"]);
    let result = merge_with_live(manifest, Some(&live_map), &gens);
    assert_eq!(
        result.data.get("JWT__SECRET").map(String::as_str),
        Some("uuid-xyz")
    );
    assert_eq!(result.breakdown.preserved, vec!["JWT__SECRET"]);
    assert!(result.breakdown.overwritten.is_empty());
    assert!(result.breakdown.created.is_empty());
}

#[test]
fn generated_non_empty_manifest_overwrites_live() {
    let manifest = map(&[("JWT__SECRET", "user-chosen")]);
    let live_map = live(&[("JWT__SECRET", "uuid-xyz")]);
    let gens = generators(&["JWT__SECRET"]);
    let result = merge_with_live(manifest, Some(&live_map), &gens);
    assert_eq!(
        result.data.get("JWT__SECRET").map(String::as_str),
        Some("user-chosen")
    );
    assert_eq!(result.breakdown.overwritten, vec!["JWT__SECRET"]);
}

#[test]
fn bare_secret_empty_manifest_overwrites_live() {
    let manifest = map(&[("API_KEY", "")]);
    let live_map = live(&[("API_KEY", "current")]);
    let gens = generators(&[]);
    let result = merge_with_live(manifest, Some(&live_map), &gens);
    assert_eq!(result.data.get("API_KEY").map(String::as_str), Some(""));
    assert_eq!(result.breakdown.overwritten, vec!["API_KEY"]);
}

#[test]
fn manifest_key_absent_from_live_is_created() {
    let manifest = map(&[("NEW_KEY", "value")]);
    let result = merge_with_live(manifest, None, &generators(&[]));
    assert_eq!(
        result.data.get("NEW_KEY").map(String::as_str),
        Some("value")
    );
    assert_eq!(result.breakdown.created, vec!["NEW_KEY"]);
}

#[test]
fn generated_empty_manifest_with_live_empty_is_overwritten() {
    let manifest = map(&[("JWT__SECRET", "")]);
    let live_map = live(&[("JWT__SECRET", "")]);
    let gens = generators(&["JWT__SECRET"]);
    let result = merge_with_live(manifest, Some(&live_map), &gens);
    assert_eq!(result.data.get("JWT__SECRET").map(String::as_str), Some(""));
    assert_eq!(result.breakdown.overwritten, vec!["JWT__SECRET"]);
    assert!(result.breakdown.preserved.is_empty());
}

#[test]
fn mixed_breakdown() {
    let manifest = map(&[
        ("BARE_NEW", "supplied"),
        ("GEN_PRESERVE", ""),
        ("GEN_OVERWRITE", "new"),
        ("BARE_OVERWRITE", ""),
    ]);
    let live_map = live(&[
        ("GEN_PRESERVE", "kept-value"),
        ("GEN_OVERWRITE", "old"),
        ("BARE_OVERWRITE", "old-bare"),
    ]);
    let gens = generators(&["GEN_PRESERVE", "GEN_OVERWRITE"]);
    let result = merge_with_live(manifest, Some(&live_map), &gens);
    assert_eq!(result.breakdown.created, vec!["BARE_NEW"]);
    assert_eq!(result.breakdown.preserved, vec!["GEN_PRESERVE"]);
    assert_eq!(
        result.breakdown.overwritten,
        vec!["BARE_OVERWRITE", "GEN_OVERWRITE"]
    );
    assert_eq!(
        result.data.get("GEN_PRESERVE").map(String::as_str),
        Some("kept-value")
    );
    assert_eq!(
        result.data.get("GEN_OVERWRITE").map(String::as_str),
        Some("new")
    );
}
