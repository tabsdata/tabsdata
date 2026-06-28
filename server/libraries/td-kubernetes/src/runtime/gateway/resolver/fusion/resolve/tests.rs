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
