//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn sources(entries: &[(&str, &str)]) -> Vec<(String, String)> {
    entries
        .iter()
        .map(|(name, content)| ((*name).to_owned(), (*content).to_owned()))
        .collect()
}

#[test]
fn editable_placeholders_are_collected() {
    let entries = collect_entries(sources(&[(
        "aiagent.yaml",
        "key: ${{secret:openai-key}}\n",
    )]));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, "secret");
    assert_eq!(entries[0].category, "openai-key");
}

#[test]
fn non_editable_placeholders_are_ignored() {
    let entries = collect_entries(sources(&[(
        "apiserver.service.yaml",
        "port: ${{system:tdk_apiserver_public_port}}\nhome: ${{env:HOME}}\n",
    )]));
    assert!(entries.is_empty());
}

#[test]
fn optional_placeholders_are_marked_optional() {
    let entries = collect_entries(sources(&[(
        "aiagent.yaml",
        "key: ${{secret:openai-key?}}\n",
    )]));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].rule, CheckRule::Optional);
    assert_eq!(entries[0].category, "openai-key");
}

#[test]
fn auto_placeholders_are_delegated_not_pending() {
    let entries = collect_entries(sources(&[(
        "apiserver.service.yaml",
        "a: ${{secretAuto:jwt_signing_key}}\nb: ${{secretAuto:service_token?}}\n",
    )]));
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|e| e.rule == CheckRule::Delegated));
}

#[test]
fn a_mandatory_secret_stays_mandatory() {
    let entries = collect_entries(sources(&[(
        "apiserver.yaml",
        "key: ${{secret:license_key}}\n",
    )]));
    assert_eq!(entries[0].rule, CheckRule::Mandatory);
}

#[test]
fn a_category_provided_once_is_one_entry_across_files() {
    let entries = collect_entries(sources(&[
        (
            "apiserver.database.yaml",
            "p: ${{secretAuto:pg_password}}\n",
        ),
        (
            "dbmanager.database.yaml",
            "p: ${{secretAuto:pg_password}}\n",
        ),
    ]));
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].files,
        vec![
            "apiserver.database.yaml".to_owned(),
            "dbmanager.database.yaml".to_owned()
        ]
    );
}

#[test]
fn mandatory_entries_are_listed_before_optional_ones() {
    let entries = collect_entries(sources(&[(
        "config.yaml",
        "a: ${{secret:zzz?}}\nb: ${{secret:aaa}}\n",
    )]));
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].rule, CheckRule::Mandatory);
    assert_eq!(entries[0].category, "aaa");
    assert_eq!(entries[1].rule, CheckRule::Optional);
}

#[test]
fn provided_is_what_the_template_has_and_the_input_no_longer_shows() {
    let expected = collect_entries(sources(&[(
        "aiagent.yaml",
        "one: ${{secret:openai-key}}\ntwo: ${{secret:anthropic-key}}\n",
    )]));
    let pending = collect_entries(sources(&[(
        "aiagent.yaml",
        "one: sk-supplied\ntwo: ${{secret:anthropic-key}}\n",
    )]));
    let provided = remaining(expected, &pending);
    assert_eq!(provided.len(), 1);
    assert_eq!(provided[0].category, "openai-key");
}

#[test]
fn nothing_is_provided_when_the_input_still_carries_every_token() {
    let template = "one: ${{secret:openai-key}}\n";
    let expected = collect_entries(sources(&[("aiagent.yaml", template)]));
    let pending = collect_entries(sources(&[("aiagent.yaml", template)]));
    assert!(remaining(expected, &pending).is_empty());
}

#[test]
fn only_yaml_templates_are_considered() {
    let mut configs = BTreeMap::new();
    configs.insert(
        "aiagent.yaml".to_owned(),
        "key: ${{secret:openai-key}}\n".to_owned(),
    );
    configs.insert(
        "aiagent.req.txt".to_owned(),
        "key: ${{secret:ignored}}\n".to_owned(),
    );
    let entries = collect_entries(placeholders_in_templates(&configs));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].category, "openai-key");
}

fn templates(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(name, content)| ((*name).to_owned(), (*content).to_owned()))
        .collect()
}

#[test]
fn a_category_supplied_in_one_file_is_settled_for_every_file() {
    let template = "host: ${{literal:tdk_database_host}}\n";
    let settled = settled_categories(
        &templates(&[
            ("apiserver.database.yaml", template),
            ("dbmanager.database.yaml", template),
        ]),
        &sources(&[
            ("apiserver.database.yaml", template),
            ("dbmanager.database.yaml", "host: dimax\n"),
        ]),
    );
    assert!(settled.contains("tdk_database_host"));
}

#[test]
fn a_category_supplied_nowhere_is_not_settled() {
    let template = "host: ${{literal:tdk_database_host}}\n";
    let settled = settled_categories(
        &templates(&[("apiserver.database.yaml", template)]),
        &sources(&[("apiserver.database.yaml", template)]),
    );
    assert!(settled.is_empty());
}

#[test]
fn a_supplied_secret_category_is_settled_too() {
    let template = "username: ${{secret:postgres_system_user}}\n";
    let settled = settled_categories(
        &templates(&[
            ("apiserver.database.yaml", template),
            ("dbmanager.database.yaml", template),
        ]),
        &sources(&[
            ("apiserver.database.yaml", template),
            ("dbmanager.database.yaml", "username: dbadmin\n"),
        ]),
    );
    assert!(settled.contains("postgres_system_user"));
}

#[test]
fn a_file_without_a_template_is_ignored() {
    let settled = settled_categories(
        &templates(&[]),
        &sources(&[("unknown.yaml", "host: dimax\n")]),
    );
    assert!(settled.is_empty());
}
