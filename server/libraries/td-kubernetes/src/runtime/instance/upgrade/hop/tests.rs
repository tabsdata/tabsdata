//
// Copyright 2026 Tabsdata Inc.
//

use indoc::indoc;

use super::*;

const CONFIGMAP: &str = "td-app-aiagent-config-aiagent.yaml";
const CONFIG: &str = "aiagent.yaml";

fn resolver() -> ConfigurationResolver {
    ConfigurationResolver::new(SystemValues {
        apiserver_public_port: Some("20022".to_owned()),
        ..SystemValues::default()
    })
}

fn entries(content: &str) -> Vec<(String, Vec<u8>)> {
    vec![(CONFIG.to_owned(), content.as_bytes().to_vec())]
}

#[test]
fn system_placeholders_an_upgrade_left_in_a_config_are_materialized() {
    let content = indoc! {"
        internal_addresses:
        - 127.0.0.1:${{system:tdk_apiserver_public_port}}
        - '[::1]:${{system:tdk_apiserver_public_port}}'
    "};
    let data = materialized_entries(&resolver(), CONFIGMAP, entries(content))
        .expect("the placeholders resolve")
        .expect("the config changed");
    assert_eq!(
        data[CONFIG],
        indoc! {"
            internal_addresses:
            - 127.0.0.1:20022
            - '[::1]:20022'
        "}
    );
}

#[test]
fn a_config_without_system_placeholders_is_left_alone() {
    let content = indoc! {"
        internal_addresses:
        - 127.0.0.1:20022
    "};
    let data =
        materialized_entries(&resolver(), CONFIGMAP, entries(content)).expect("the config reads");
    assert!(data.is_none());
}

#[test]
fn a_system_placeholder_without_a_value_fails_the_upgrade() {
    let content = "port: ${{system:tdk_database_port}}\n";
    let outcome = materialized_entries(&resolver(), CONFIGMAP, entries(content));
    assert!(outcome.is_err());
}

#[test]
fn placeholders_of_other_kinds_are_kept_for_the_pods_to_resolve() {
    let content = indoc! {"
        port: ${{system:tdk_apiserver_public_port}}
        password: ${{secret:postgres_apiserver_password}}
    "};
    let data = materialized_entries(&resolver(), CONFIGMAP, entries(content))
        .expect("the placeholders resolve")
        .expect("the config changed");
    assert_eq!(
        data[CONFIG],
        indoc! {"
            port: 20022
            password: ${{secret:postgres_apiserver_password}}
        "}
    );
}
