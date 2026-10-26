//
// Copyright 2026 Tabsdata Inc.
//

use indoc::indoc;

use super::*;

const STORAGE: &str = indoc! {"
    storage:
      mounts:
      - id: ROOT
        path: /
        uri: s3://bucket/
        options:
          aws_region: ${{secret}}
"};

const SCHEMA: &str = indoc! {"
    directives:
      - at: storage.mounts[*].uri
        treat: secret
      - at: storage.mounts[*].options.*
        treat: secret
      - at: storage.mounts[*].options
        treat: open
"};

fn configs(entries: &[(&str, &str)]) -> RenderedConfigmap {
    entries
        .iter()
        .map(|(file, yaml)| ((*file).to_owned(), (*yaml).to_owned()))
        .collect()
}

#[test]
fn the_default_source_lists_the_secrets_its_schema_declares() {
    let entries = kprofile_entries(&configs(&[
        ("apiserver.storage.yaml", STORAGE),
        ("apiserver.storage.yamls", SCHEMA),
    ]))
    .expect("entries");
    assert_eq!(entries.len(), 1, "the schema file is not a config");
    let mut yaml: serde_yaml::Value = serde_yaml::from_str(&entries[0].yaml).expect("yaml");
    let leaves =
        crate::runtime::secrets::discovery::extract_secret_leaves(&entries[0].file, &mut yaml)
            .expect("leaves");
    let keys: Vec<&str> = leaves.iter().map(|leaf| leaf.key.as_str()).collect();
    assert_eq!(
        keys,
        [
            "STORAGE__MOUNTS__ROOT__URI",
            "STORAGE__MOUNTS__ROOT__OPTIONS__AWS_REGION",
        ]
    );
}

#[test]
fn the_default_source_keeps_a_config_without_schema_as_is() {
    let entries =
        kprofile_entries(&configs(&[("apiserver.storage.yaml", STORAGE)])).expect("entries");
    assert_eq!(entries[0].yaml, STORAGE);
}
