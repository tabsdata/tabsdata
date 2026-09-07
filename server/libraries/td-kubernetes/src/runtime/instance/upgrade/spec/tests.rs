//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

const SAMPLE: &str = "\
schema: 1
configs:
  tabsdata.yaml:
    tabsdata_kminus.yaml:
      action: retain
    tabsdata_docker.yaml:
      action: replace
    tabsdata_eks.yaml:
      action: modify
      changes:
        - op: modify-leaf-value-if
          leaf: pods.global.envs.UV_INDEX
          from: \"\"
          to: https://pypi.org/simple/
pods:
  pymanager.yaml:
    pymanager_kminus.yaml:
      action: replace
    pymanager_eks.yaml:
      action: create
  legacy.yaml:
    legacy_kminus.yaml:
      action: delete
";

#[test]
fn a_spec_parses_every_action() {
    let spec = deserialize_spec("2.0.1", SAMPLE).expect("parses");
    let configs = &spec.configs["tabsdata.yaml"];
    assert!(matches!(configs["tabsdata_kminus.yaml"], Action::Retain(_)));
    assert!(matches!(
        configs["tabsdata_docker.yaml"],
        Action::Replace(_)
    ));
    assert!(matches!(
        configs["tabsdata_eks.yaml"],
        Action::Modify { .. }
    ));

    let pods = &spec.pods["pymanager.yaml"];
    assert!(matches!(pods["pymanager_kminus.yaml"], Action::Replace(_)));
    assert!(matches!(pods["pymanager_eks.yaml"], Action::Create(_)));
    assert!(matches!(
        spec.pods["legacy.yaml"]["legacy_kminus.yaml"],
        Action::Delete(_)
    ));
}

#[test]
fn a_modify_block_parses_as_a_tagged_operation_list() {
    let spec = deserialize_spec("2.0.1", SAMPLE).expect("parses");
    let operations = &spec.configs["tabsdata.yaml"]["tabsdata_eks.yaml"].changes();
    assert_eq!(operations.len(), 1);
    let Operation::ModifyLeafValueIf(modify) = &operations[0] else {
        panic!("expected a leaf-value operation")
    };
    assert_eq!(modify.leaf, "pods.global.envs.UV_INDEX");
    assert_eq!(modify.from, serde_yaml::Value::from(""));
    assert_eq!(
        modify.to,
        serde_yaml::Value::from("https://pypi.org/simple/")
    );
}

#[test]
fn several_operations_are_kept_in_order() {
    let yaml = "\
schema: 1
configs:
  a.yaml:
    a_kminus.yaml:
      action: modify
      changes:
        - op: modify-leaf-value-if
          leaf: one
          from: x
          to: y
        - op: modify-leaf-value-if
          leaf: two
          from: p
          to: q
";
    let spec = deserialize_spec("2.0.1", yaml).expect("parses");
    let operations = &spec.configs["a.yaml"]["a_kminus.yaml"].changes();
    assert_eq!(operations.len(), 2);
    let Operation::ModifyLeafValueIf(first) = &operations[0] else {
        panic!("expected a leaf-value operation")
    };
    let Operation::ModifyLeafValueIf(second) = &operations[1] else {
        panic!("expected a leaf-value operation")
    };
    assert_eq!(first.leaf, "one");
    assert_eq!(second.leaf, "two");
}

#[test]
fn a_null_from_or_to_parses_as_null() {
    let yaml = "\
schema: 1
configs:
  a.yaml:
    a_kminus.yaml:
      action: modify
      changes:
        - op: modify-leaf-value-if
          leaf: pods.global.envs.UV_INDEX
        - op: modify-leaf-value-if
          leaf: pods.global.envs.UV_INDEX_STRATEGY
          from: unsafe-best-match
          to: ~
";
    let spec = deserialize_spec("2.0.1", yaml).expect("parses");
    let operations = &spec.configs["a.yaml"]["a_kminus.yaml"].changes();
    let Operation::ModifyLeafValueIf(absent) = &operations[0] else {
        panic!("expected a leaf-value operation")
    };
    assert!(absent.from.is_null());
    assert!(absent.to.is_null());
    let Operation::ModifyLeafValueIf(nulled) = &operations[1] else {
        panic!("expected a leaf-value operation")
    };
    assert_eq!(nulled.from, serde_yaml::Value::from("unsafe-best-match"));
    assert!(nulled.to.is_null());
}

#[test]
fn numbers_and_booleans_parse_as_scalars() {
    let yaml = "\
schema: 1
configs:
  a.yaml:
    a_kminus.yaml:
      action: modify
      changes:
        - op: modify-leaf-value-if
          leaf: pool.max_connections
          from: 50
          to: 100
        - op: modify-leaf-value-if
          leaf: pool.test_before_acquire
          from: true
          to: false
";
    let spec = deserialize_spec("2.0.1", yaml).expect("parses");
    let operations = &spec.configs["a.yaml"]["a_kminus.yaml"].changes();
    let Operation::ModifyLeafValueIf(number) = &operations[0] else {
        panic!("expected a leaf-value operation")
    };
    assert_eq!(number.from, serde_yaml::Value::from(50));
    assert_eq!(number.to, serde_yaml::Value::from(100));
    let Operation::ModifyLeafValueIf(flag) = &operations[1] else {
        panic!("expected a leaf-value operation")
    };
    assert_eq!(flag.from, serde_yaml::Value::from(true));
    assert_eq!(flag.to, serde_yaml::Value::from(false));
}

#[test]
fn an_empty_spec_parses() {
    let spec = deserialize_spec("2.0.1", "schema: 1\n").expect("parses");
    assert!(spec.assets.is_empty());
    assert!(spec.configs.is_empty());
    assert!(spec.pods.is_empty());
}

#[test]
fn an_unsupported_schema_is_rejected() {
    let outcome = deserialize_spec("2.0.1", "schema: 99\n");
    assert!(matches!(
        outcome,
        Err(UpgradeError::SpecSchemaUnsupported { .. })
    ));
}

#[test]
fn changes_on_a_non_modify_action_are_rejected() {
    let yaml = "\
schema: 1
configs:
  a.yaml:
    a_kminus.yaml:
      action: retain
      changes:
        - op: modify-leaf-value-if
          leaf: one
          from: x
          to: y
";
    assert!(deserialize_spec("2.0.1", yaml).is_err());
}

#[test]
fn a_modify_action_without_changes_is_rejected() {
    let yaml = "\
schema: 1
configs:
  a.yaml:
    a_kminus.yaml:
      action: modify
";
    assert!(deserialize_spec("2.0.1", yaml).is_err());
}

#[test]
fn a_stray_key_on_a_bare_action_is_rejected() {
    let yaml = "\
schema: 1
configs:
  a.yaml:
    a_kminus.yaml:
      action: retain
      bogus: 1
";
    assert!(deserialize_spec("2.0.1", yaml).is_err());
}

#[test]
fn an_unknown_operation_is_rejected() {
    let yaml = "\
schema: 1
configs:
  a.yaml:
    a_kminus.yaml:
      action: modify
      changes:
        - op: rewrite-everything
          leaf: one
";
    let outcome = deserialize_spec("2.0.1", yaml);
    assert!(matches!(outcome, Err(UpgradeError::SpecParseFailed { .. })));
}

#[test]
fn a_nest_children_operation_parses() {
    let yaml = "\
schema: 1
configs:
  a.yaml:
    a_kminus.yaml:
      action: modify
      changes:
        - op: nest-children-if-flat
          leaf: database_server.pool
          under: admin
";
    let spec = deserialize_spec("2.1.0", yaml).expect("parses");
    assert_eq!(
        spec.configs["a.yaml"]["a_kminus.yaml"].changes(),
        [Operation::NestChildrenIfFlat(NestChildrenIfFlat {
            leaf: "database_server.pool".to_owned(),
            under: "admin".to_owned(),
        })]
    );
}

#[test]
fn the_shipped_specs_parse() {
    let folder = format!(
        "{}/../../../variant/resources/kupgrade/upgrades",
        env!("CARGO_MANIFEST_DIR")
    );
    for entry in std::fs::read_dir(&folder).expect(&folder) {
        let path = entry.expect("entry").path();
        if path.extension().is_none_or(|suffix| suffix != "yaml") {
            continue;
        }
        let version = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .expect("a version");
        let yaml = std::fs::read_to_string(&path).expect("readable");
        deserialize_spec(version, &yaml)
            .unwrap_or_else(|error| panic!("{} must parse: {error}", path.display()));
    }
}
