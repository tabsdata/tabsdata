//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

const DIVERGED_OUTPUT_VARIABLE: &str = "TD_KUPGRADE_REPLAY_OUTPUT";
const HEAD_VERSION: &str = env!("CARGO_PKG_VERSION");

fn repository() -> PathBuf {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for _ in 0..3 {
        root.pop();
    }
    root
}

fn sources() -> Sources {
    Sources::of(&repository())
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
    let inventory = load_inventory(&sources().kupgrade).expect("the shipped inventory loads");
    assert_eq!(
        inventory.newest(),
        HEAD_VERSION,
        "the newest version in the upgrade inventory must be the one being built"
    );
}

#[test]
fn the_working_kprofile_serves_every_supported_provider() {
    let providers = kprofile_providers(&sources().kprofile);
    let supported: Vec<ClusterProvider> = ClusterProvider::supported_providers().collect();
    assert_eq!(providers, supported);
}

#[test]
fn every_inventory_version_resolves_into_templates() {
    let (_scratch, templates) = materialize_scratch(&sources()).expect("every version resolves");
    let inventory = load_inventory(&sources().kupgrade).expect("the shipped inventory loads");
    assert_eq!(templates.versions.len(), inventory.versions.len());
    for (entry, providers) in &templates.versions {
        assert!(
            !providers.is_empty(),
            "the kprofile of '{}' serves no supported provider",
            entry.version
        );
    }
}

#[test]
fn the_released_templates_match_their_recorded_digests() {
    let (_scratch, templates) = materialize_scratch(&sources()).expect("every version resolves");
    let mismatches = drifted_digests(&sources(), &templates).expect("the digests read");
    assert!(
        mismatches.is_empty(),
        "released templates no longer resolve as recorded; explain the drift before \
         touching the digests, which 'cargo make upgrader build' only ever adds:\n{}",
        report(&mismatches)
    );
}

#[tokio::test]
async fn every_supported_provider_upgrades_from_its_first_version_to_the_head() {
    let (_scratch, templates) = materialize_scratch(&sources()).expect("every version resolves");
    let output = std::env::var_os(DIVERGED_OUTPUT_VARIABLE).map(PathBuf::from);
    let mismatches = replay_all(&sources(), &templates, output.as_deref())
        .await
        .expect("the templates replay");
    assert!(
        mismatches.is_empty(),
        "upgrading the templates does not reproduce the next version's templates; set \
         {DIVERGED_OUTPUT_VARIABLE} to a folder to keep each diverging result:\n{}",
        report(&mismatches)
    );
}

#[test]
fn a_relative_repository_yields_absolute_sources() {
    let sources = Sources::of(Path::new("."));
    for path in [
        &sources.repository,
        &sources.kupgrade,
        &sources.kprofile,
        &sources.config,
    ] {
        assert!(path.is_absolute(), "'{}' is not absolute", path.display());
    }
}
