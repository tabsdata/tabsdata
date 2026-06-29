//
// Copyright 2026 Tabs Data Inc.
//

use super::*;
use crate::runtime::gateway::resolver::placeholder::SystemValues;

#[test]
fn secret_auto_collects_generated_value_and_retains_token() {
    let config = "\
database:
  system:
    username: ${{secretAuto:postgres_system_user}}
    password: ${{secretAuto:postgres_password}}
";
    let system = SystemValues::default();
    let auto = HashMap::new();
    let outcome = resolve_config_file(config, config, &system, &auto).expect("resolve");

    assert!(
        outcome
            .rendered
            .contains("${{secretAuto:postgres_system_user}}")
    );

    let by_key: std::collections::BTreeMap<_, _> = outcome
        .secrets
        .iter()
        .map(|entry| (entry.key.as_str(), entry.value.as_str()))
        .collect();
    let user = by_key
        .get("DATABASE__SYSTEM__USERNAME")
        .expect("username secret collected");
    assert_eq!(*user, "postgres");
    let password = by_key
        .get("DATABASE__SYSTEM__PASSWORD")
        .expect("password secret collected");
    assert!(
        !password.contains("${"),
        "password must be a generated value, not the token: {password}"
    );
}

#[test]
fn schema_baked_template_harvests_value_from_init() {
    let template = "\
storage:
  mounts:
  - id: S3
    options:
      aws_region: ${{secret}}
";
    let init = "\
storage:
  mounts:
  - id: S3
    options:
      aws_region: eu-west-1
";
    let system = SystemValues::default();
    let auto = HashMap::new();
    let outcome = resolve_config_file(template, init, &system, &auto).expect("resolve");

    let by_key: std::collections::BTreeMap<_, _> = outcome
        .secrets
        .iter()
        .map(|entry| (entry.key.as_str(), entry.value.as_str()))
        .collect();
    let region = by_key
        .get("STORAGE__MOUNTS__S3__OPTIONS__AWS_REGION")
        .expect("aws_region secret harvested from init");
    assert_eq!(*region, "eu-west-1");
    assert!(outcome.rendered.contains("${{secret}}"));
}

#[test]
fn connection_envfrom_secret_ref_resolves_without_identity_error() {
    let config = "\
envFrom:
  - secretRef:
      name: ${{system:tdk_connection_secret_name}}
";
    let system = SystemValues {
        connection_secret_name: Some("td-conn-abc123".to_owned()),
        ..SystemValues::default()
    };
    let auto = HashMap::new();

    let outcome =
        resolve_config_file(config, config, &system, &auto).expect("envFrom must resolve");

    assert!(
        outcome.rendered.contains("td-conn-abc123"),
        "connection secret name must be rendered: {}",
        outcome.rendered
    );
    assert!(
        !outcome.rendered.contains("${"),
        "no placeholder token must remain: {}",
        outcome.rendered
    );
}
