//
// Copyright 2026 Tabs Data Inc.
//

use super::*;
use crate::runtime::gateway::resolver::schema::Schema;

fn reveal_default(template: &str, configmap: &str) -> String {
    reveal(
        template,
        configmap,
        &Schema::default(),
        false,
        &HashMap::new(),
    )
    .expect("reveal")
}

#[test]
fn editable_literal_shows_resolved_value() {
    let template = "name: ${{literal:label}}\n";
    let configmap = "name: production\n";
    let rendered = reveal_default(template, configmap);
    assert!(rendered.contains("name: production"));
}

#[test]
fn non_editable_system_value_is_restored_to_token() {
    let template = "port: ${{system:tdk_apiserver_public_port}}\n";
    let configmap = "port: 2457\n";
    let rendered = reveal_default(template, configmap);
    assert!(rendered.contains("${{system:tdk_apiserver_public_port}}"));
    assert!(!rendered.contains("2457"));
}

#[test]
fn embedded_system_value_restores_full_template_text() {
    let template = "address: 0.0.0.0:${{system:tdk_apiserver_public_port}}\n";
    let configmap = "address: 0.0.0.0:2457\n";
    let rendered = reveal_default(template, configmap);
    assert!(rendered.contains("0.0.0.0:${{system:tdk_apiserver_public_port}}"));
}

#[test]
fn secret_without_values_shows_token() {
    let template = "password: ${{secret}}\n";
    let configmap = "password: ${{secret}}\n";
    let rendered = reveal_default(template, configmap);
    assert!(rendered.contains("password: ${{secret}}"));
}

#[test]
fn secret_with_values_shows_resolved_secret() {
    let template = "password: ${{secret}}\n";
    let configmap = "password: ${{secret}}\n";
    let mut secret_values = HashMap::new();
    let key = encode_secret_key(SECRET_CONSUMER, &["password".to_owned()]).expect("key");
    secret_values.insert(key, "topsecret".to_owned());
    let rendered =
        reveal(template, configmap, &Schema::default(), true, &secret_values).expect("reveal");
    assert!(rendered.contains("password: topsecret"));
}

#[test]
fn secret_with_values_but_missing_entry_falls_back_to_token() {
    let template = "password: ${{secret}}\n";
    let configmap = "password: ${{secret}}\n";
    let rendered =
        reveal(template, configmap, &Schema::default(), true, &HashMap::new()).expect("reveal");
    assert!(rendered.contains("password: ${{secret}}"));
}

#[test]
fn schema_secret_leaf_is_shown_as_token_without_values() {
    let schema_text = "directives:\n  - at: options.token\n    treat: secret\n";
    let schema = Schema::parse("x.yamls", schema_text).expect("schema");
    let template = "options:\n  token: baked-in-value\n";
    let configmap = "options:\n  token: baked-in-value\n";
    let rendered = reveal(template, configmap, &schema, false, &HashMap::new()).expect("reveal");
    assert!(rendered.contains("token: ${{secret}}"));
}

#[test]
fn empty_configmap_renders_empty_string() {
    let template = "a: ${{literal:x}}\n";
    let configmap = "";
    let rendered = reveal_default(template, configmap);
    assert_eq!(rendered, "");
}

#[test]
fn value_not_in_template_or_schema_passes_through() {
    let template = "known: ${{literal:x}}\n";
    let configmap = "known: value\nextra: untouched\n";
    let rendered = reveal_default(template, configmap);
    assert!(rendered.contains("extra: untouched"));
}

#[test]
fn prefill_replaces_secret_token_with_server_value() {
    let input = "password: ${{secret}}\n";
    let mut server_values = HashMap::new();
    let key = encode_secret_key(SECRET_CONSUMER, &["password".to_owned()]).expect("key");
    server_values.insert(key, "from-server".to_owned());
    let rendered = prefill_secrets(input, &server_values).expect("prefill");
    assert!(rendered.contains("password: from-server"));
}

#[test]
fn prefill_leaves_non_secret_placeholders_untouched() {
    let input = "port: ${{system:tdk_apiserver_public_port}}\n";
    let rendered = prefill_secrets(input, &HashMap::new()).expect("prefill");
    assert!(rendered.contains("${{system:tdk_apiserver_public_port}}"));
}

#[test]
fn prefill_leaves_secret_token_when_no_server_value() {
    let input = "password: ${{secret}}\n";
    let rendered = prefill_secrets(input, &HashMap::new()).expect("prefill");
    assert!(rendered.contains("password: ${{secret}}"));
}

#[test]
fn prefill_handles_keyed_sequence_identity_paths() {
    let input = "\
mounts:
- id: S3
  options:
    key: ${{secret}}
";
    let mut server_values = HashMap::new();
    let key = encode_secret_key(
        SECRET_CONSUMER,
        &[
            "mounts".to_owned(),
            "S3".to_owned(),
            "options".to_owned(),
            "key".to_owned(),
        ],
    )
    .expect("key");
    server_values.insert(key, "AKIA".to_owned());
    let rendered = prefill_secrets(input, &server_values).expect("prefill");
    assert!(rendered.contains("key: AKIA"));
}
