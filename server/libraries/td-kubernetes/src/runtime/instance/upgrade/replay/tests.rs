//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

const REGENERATE_VARIABLE: &str = "TD_KUPGRADE_TEMPLATES_REGENERATE";
const DIVERGED_OUTPUT_VARIABLE: &str = "TD_KUPGRADE_REPLAY_OUTPUT";
const HEAD_VERSION: &str = env!("CARGO_PKG_VERSION");

fn variant_folder(path: &str) -> PathBuf {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for _ in 0..3 {
        root.pop();
    }
    root.join("variant").join(path)
}

fn kupgrade() -> PathBuf {
    variant_folder("resources/kupgrade")
}

fn kprofile() -> PathBuf {
    variant_folder("resources/kprofile")
}

fn templates() -> PathBuf {
    variant_folder("config/kupgrade")
}

fn report(mismatches: &[Mismatch]) -> String {
    mismatches
        .iter()
        .map(|mismatch| format!("  - {mismatch}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn the_inventory_head_is_the_version_being_built() {
    let inventory = load_inventory(&kupgrade()).expect("the shipped inventory loads");
    assert_eq!(
        inventory.newest(),
        HEAD_VERSION,
        "the newest version in the upgrade inventory must be the one being built"
    );
}

#[test]
fn every_template_version_is_in_the_inventory() {
    let inventory = load_inventory(&kupgrade()).expect("the shipped inventory loads");
    let known: BTreeSet<&str> = inventory
        .versions
        .iter()
        .map(|entry| entry.version.as_str())
        .collect();
    for version in template_versions(&templates()) {
        assert!(
            known.contains(version.as_str()),
            "template version '{version}' is not in the upgrade inventory"
        );
    }
}

#[test]
fn the_inventory_floor_has_templates() {
    let inventory = load_inventory(&kupgrade()).expect("the shipped inventory loads");
    let covered = ClusterProvider::supported_providers()
        .any(|provider| template_folder(&templates(), inventory.floor(), provider).is_dir());
    assert!(
        covered,
        "the inventory floor '{}' has no template, so no upgrade from it is verified",
        inventory.floor()
    );
}

#[test]
fn every_supported_provider_has_templates_from_its_first_version_to_the_head() {
    let inventory = load_inventory(&kupgrade()).expect("the shipped inventory loads");
    let mut oldest_first = inventory.versions.clone();
    oldest_first.reverse();
    for provider in ClusterProvider::supported_providers() {
        let covered: Vec<bool> = oldest_first
            .iter()
            .map(|entry| template_folder(&templates(), &entry.version, provider).is_dir())
            .collect();
        let first = covered.iter().position(|present| *present);
        let Some(first) = first else {
            panic!(
                "provider '{}' has no template in any version",
                provider.as_str()
            );
        };
        for (entry, present) in oldest_first.iter().zip(&covered).skip(first) {
            assert!(
                present,
                "provider '{}' has templates from '{}' but none for '{}'",
                provider.as_str(),
                oldest_first[first].version,
                entry.version
            );
        }
    }
}

#[test]
fn the_head_templates_match_the_working_kprofile() {
    if std::env::var(REGENERATE_VARIABLE).is_ok_and(|value| !value.is_empty()) {
        write_templates(&kprofile(), &templates(), HEAD_VERSION)
            .expect("the head templates are written");
    }
    let mismatches = stale_templates(&kprofile(), &templates(), HEAD_VERSION)
        .expect("the working kprofile resolves");
    assert!(
        mismatches.is_empty(),
        "the head templates are stale; rerun with {REGENERATE_VARIABLE}=1 to rewrite them:\n{}",
        report(&mismatches)
    );
}

#[test]
fn the_working_kprofile_serves_every_supported_provider() {
    let providers = kprofile_providers(&kprofile());
    let supported: Vec<ClusterProvider> = ClusterProvider::supported_providers().collect();
    assert_eq!(providers, supported);
}

#[tokio::test]
async fn every_supported_provider_upgrades_from_its_first_template_to_the_head() {
    let output = std::env::var_os(DIVERGED_OUTPUT_VARIABLE).map(PathBuf::from);
    let mismatches = replay_all(&kupgrade(), &templates(), output.as_deref())
        .await
        .expect("the templates replay");
    assert!(
        mismatches.is_empty(),
        "upgrading the templates does not reproduce the next version's templates; set \
         {DIVERGED_OUTPUT_VARIABLE} to a folder to keep each diverging result:\n{}",
        report(&mismatches)
    );
}
