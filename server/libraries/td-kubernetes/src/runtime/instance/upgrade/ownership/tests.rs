//
// Copyright 2026 Tabsdata Inc.
//

use indoc::indoc;

use super::*;
use crate::runtime::instance::upgrade::spec::deserialize_spec;

const OPEN_OPTIONS: &str = indoc! {"
    directives:
      - at: settings.options
        treat: open
"};

const CONFIG: &str = indoc! {"
    settings:
      name: tabsdata
      options:
        region: eu
"};

fn problems_of(action_yaml: &str, previous: &str) -> Vec<String> {
    let action: String = action_yaml
        .lines()
        .map(|line| format!("      {line}\n"))
        .collect();
    let yaml = format!(
        "schema: 1\nconfigs:\n  apiserver.storage.yaml:\n    apiserver.storage.yaml:\n{action}"
    );
    let spec = deserialize_spec("2.3.0", &yaml).expect("parses");
    let action = spec.configs["apiserver.storage.yaml"]["apiserver.storage.yaml"].clone();
    let step = Step {
        entry: "configs/apiserver.storage.yaml".to_owned(),
        folder: KPROFILE_CONFIGS_FAMILY.folder,
        file: "apiserver.storage.yaml".to_owned(),
        variant: "apiserver.storage.yaml".to_owned(),
        action,
    };
    let schema = Schema::parse("apiserver.storage.schema.yaml", OPEN_OPTIONS).expect("schema");
    step_problems("2.3.0", &step, previous, &[schema]).expect("checks")
}

#[test]
fn a_change_outside_the_open_mapping_is_allowed() {
    let action = indoc! {"
              action: modify
              changes:
                - op: modify-leaf-value-if
                  leaf: settings.name
                  from: tabsdata
                  to: renamed
    "};
    assert!(problems_of(action, CONFIG).is_empty());
}

#[test]
fn inserting_a_key_into_the_open_mapping_is_a_problem() {
    let action = indoc! {"
              action: modify
              changes:
                - op: insert-child-if-missing
                  leaf: settings.options
                  child: endpoint
                  value: http://localhost
    "};
    let problems = problems_of(action, CONFIG);
    assert_eq!(problems.len(), 1);
    assert!(problems[0].contains("changes the open mapping 'settings.options'"));
}

#[test]
fn rewriting_the_open_mapping_as_text_is_a_problem() {
    let action = indoc! {"
              action: modify
              changes:
                - op: modify-text-if
                  pattern: 'region: eu'
                  replacement: 'region: us'
    "};
    assert_eq!(problems_of(action, CONFIG).len(), 1);
}

#[test]
fn removing_the_open_mapping_is_a_problem() {
    let action = indoc! {"
              action: modify
              changes:
                - op: remove-leaf-if
                  leaf: settings.options
    "};
    let problems = problems_of(action, CONFIG);
    assert_eq!(problems.len(), 1);
    assert!(problems[0].contains("removes the open mapping"));
}

#[test]
fn creating_an_empty_open_mapping_is_allowed() {
    let previous = indoc! {"
        settings:
          name: tabsdata
    "};
    let action = indoc! {"
              action: modify
              changes:
                - op: insert-child-if-missing
                  leaf: settings
                  child: options
                  value: {}
    "};
    assert!(problems_of(action, previous).is_empty());
}

#[test]
fn creating_a_filled_open_mapping_is_a_problem() {
    let previous = indoc! {"
        settings:
          name: tabsdata
    "};
    let action = indoc! {"
              action: modify
              changes:
                - op: insert-child-if-missing
                  leaf: settings
                  child: options
                  value:
                    region: eu
    "};
    let problems = problems_of(action, previous);
    assert_eq!(problems.len(), 1);
    assert!(problems[0].contains("fills the open mapping"));
}

#[test]
fn replacing_a_file_with_an_open_mapping_is_a_problem() {
    assert_eq!(problems_of("action: replace", CONFIG).len(), 1);
}

#[test]
fn retaining_a_file_with_an_open_mapping_is_allowed() {
    assert!(problems_of("action: retain", CONFIG).is_empty());
}

#[test]
fn every_schema_variant_of_a_config_is_read() {
    let kprofile = tempfile::tempdir().expect("a temporary kprofile");
    let configs = kprofile.path().join(KPROFILE_CONFIGS_FAMILY.folder);
    std::fs::create_dir_all(&configs).expect("configs folder");
    for name in [
        "apiserver.storage.schema.yaml",
        "apiserver.storage.schema_eks.yaml",
        "apiserver.service.schema.yaml",
        "apiserver.storage.yaml",
    ] {
        std::fs::write(configs.join(name), OPEN_OPTIONS).expect("written");
    }
    let schemas = schemas_of(kprofile.path(), "apiserver.storage.yaml").expect("schemas");
    assert_eq!(schemas.len(), 2);
}
