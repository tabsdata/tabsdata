//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use crate::runtime::instance::upgrade::spec::deserialize_spec;
use crate::runtime::provider::ClusterProvider;

fn planned(yaml: &str, provider: ClusterProvider) -> Vec<Step> {
    let spec = deserialize_spec("2.0.1", yaml).expect("parses");
    plan(&spec, provider.axes(), provider.preferred_values())
}

#[test]
fn the_provider_variant_wins_over_the_generic_one() {
    let yaml = "\
schema: 1
configs:
  tabsdata.yaml:
    tabsdata.yaml:
      action: retain
    tabsdata_kminus.yaml:
      action: replace
";
    let steps = planned(yaml, ClusterProvider::Kminus);
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].variant, "tabsdata_kminus.yaml");
    assert!(matches!(steps[0].action, Action::Replace(_)));
}

#[test]
fn the_generic_variant_is_used_when_the_provider_has_none() {
    let yaml = "\
schema: 1
configs:
  tabsdata.yaml:
    tabsdata.yaml:
      action: replace
    tabsdata_kminus.yaml:
      action: retain
";
    let steps = planned(yaml, ClusterProvider::Eks);
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].variant, "tabsdata.yaml");
    assert!(matches!(steps[0].action, Action::Replace(_)));
}

#[test]
fn a_group_naming_no_applicable_variant_yields_no_step() {
    let yaml = "\
schema: 1
configs:
  tabsdata.yaml:
    tabsdata_kminus.yaml:
      action: replace
";
    assert!(planned(yaml, ClusterProvider::Eks).is_empty());
}

#[test]
fn the_entry_key_matches_the_manifest_key() {
    let yaml = "\
schema: 1
assets:
  venvloader.py:
    venvloader.py:
      action: replace
configs:
  tabsdata.yaml:
    tabsdata.yaml:
      action: retain
pods:
  pymanager.yaml:
    pymanager.yaml:
      action: delete
";
    let steps = planned(yaml, ClusterProvider::Kminus);
    let entries: Vec<&str> = steps.iter().map(|step| step.entry.as_str()).collect();
    assert_eq!(
        entries,
        vec![
            "assets/venvloader.py",
            "configs/tabsdata.yaml",
            "pods/pymanager.yaml"
        ]
    );
}

#[test]
fn modify_operations_ride_along_with_the_step() {
    let yaml = "\
schema: 1
configs:
  tabsdata.yaml:
    tabsdata.yaml:
      action: modify
      changes:
        - op: modify-leaf-value-if
          leaf: pods.global.envs.UV_INDEX
          from: \"\"
          to: ~
";
    let steps = planned(yaml, ClusterProvider::Kminus);
    assert!(matches!(steps[0].action, Action::Modify { .. }));
    assert_eq!(steps[0].action.changes().len(), 1);
}

#[test]
fn a_non_modify_step_carries_no_operations() {
    let yaml = "\
schema: 1
configs:
  a.yaml:
    a.yaml:
      action: retain
";
    let steps = planned(yaml, ClusterProvider::Kminus);
    assert!(steps[0].action.changes().is_empty());
}

#[test]
fn every_family_is_planned() {
    let yaml = "\
schema: 1
assets:
  a.py:
    a.py:
      action: replace
configs:
  b.yaml:
    b.yaml:
      action: replace
pods:
  c.yaml:
    c.yaml:
      action: replace
";
    let steps = planned(yaml, ClusterProvider::Kminus);
    assert_eq!(steps.len(), 3);
}

#[test]
fn a_schema_file_is_never_planned_as_a_config() {
    let yaml = "\
schema: 1
configs:
  apiserver.storage.schema.yaml:
    apiserver.storage.schema.yaml:
      action: replace
    apiserver.storage.schema_eks.yaml:
      action: replace
  apiserver.storage.yaml:
    apiserver.storage.yaml:
      action: replace
";
    let steps = planned(yaml, ClusterProvider::Eks);
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].file, "apiserver.storage.yaml");
}

#[test]
fn a_schema_named_asset_is_still_planned() {
    let yaml = "\
schema: 1
assets:
  bundle.schema.yaml:
    bundle.schema.yaml:
      action: replace
";
    let steps = planned(yaml, ClusterProvider::Kminus);
    assert_eq!(steps.len(), 1);
}
