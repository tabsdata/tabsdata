//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use crate::runtime::gateway::resolver::placeholder::SystemValues;

#[test]
fn secret_auto_collects_generated_value_and_retains_token() {
    let config = "\
database_server:
  username: ${{secretAuto:postgres_system_user}}
  password: ${{secretAuto:postgres_password}}
";
    let system = SystemValues::default();
    let auto = HashMap::new();
    let outcome = resolve_config_file(
        config,
        config,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
    )
    .expect("resolve");

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
        .get("DATABASE_SERVER__USERNAME")
        .expect("username secret collected");
    assert_eq!(*user, "postgres");
    let password = by_key
        .get("DATABASE_SERVER__PASSWORD")
        .expect("password secret collected");
    assert!(
        !password.contains("${"),
        "password must be a generated value, not the token: {password}"
    );
}

#[test]
fn secret_auto_same_category_shares_one_value_across_configs() {
    let first = "\
database:
  apiserver:
    password: ${{secretAuto:postgres_password}}
  aiagent:
    password: ${{secretAuto:postgres_password}}
";
    let second = "\
database_server:
  password: ${{secretAuto:postgres_password}}
";
    let system = SystemValues::default();
    let auto = HashMap::new();
    let vault = RefCell::new(HashMap::new());
    let literals = RefCell::new(HashMap::new());
    let first_outcome =
        resolve_config_file(first, first, &system, &auto, &vault, &literals).expect("first");
    let second_outcome =
        resolve_config_file(second, second, &system, &auto, &vault, &literals).expect("second");

    let values: std::collections::BTreeSet<_> = first_outcome
        .secrets
        .iter()
        .chain(second_outcome.secrets.iter())
        .map(|entry| entry.value.clone())
        .collect();
    assert_eq!(
        values.len(),
        1,
        "all postgres_password occurrences must share one value: {values:?}"
    );
    assert!(!values.iter().next().expect("value").is_empty());
}

#[test]
fn literal_same_category_reuses_value_across_configs() {
    let first_template = "database:\n  host: ${{literal:dbhost}}\n";
    let first_init = "database:\n  host: pg.internal\n";
    let second_template = "dbmanager:\n  host: ${{literal:dbhost}}\n";
    let second_init = "dbmanager:\n  host: ${{literal:dbhost}}\n";
    let system = SystemValues::default();
    let auto = HashMap::new();
    let vault = RefCell::new(HashMap::new());
    let literals = RefCell::new(HashMap::new());

    let first = resolve_config_file(
        first_template,
        first_init,
        &system,
        &auto,
        &vault,
        &literals,
    )
    .expect("first");
    let second = resolve_config_file(
        second_template,
        second_init,
        &system,
        &auto,
        &vault,
        &literals,
    )
    .expect("second");

    assert!(
        first.rendered.contains("pg.internal"),
        "the providing config must render the value: {}",
        first.rendered
    );
    assert!(
        second.rendered.contains("pg.internal"),
        "a later config must reuse the value provided elsewhere: {}",
        second.rendered
    );
}

#[test]
fn literal_and_secret_of_same_category_do_not_share_stores() {
    let literal_template = "plain:\n  name: ${{literal:postgres_password}}\n";
    let literal_init = "plain:\n  name: from-literal\n";
    let secret_template = "guarded:\n  password: ${{secretAuto:postgres_password}}\n";
    let system = SystemValues::default();
    let auto = HashMap::new();
    let vault = RefCell::new(HashMap::new());
    let literals = RefCell::new(HashMap::new());

    let literal_outcome = resolve_config_file(
        literal_template,
        literal_init,
        &system,
        &auto,
        &vault,
        &literals,
    )
    .expect("literal");
    let secret_outcome = resolve_config_file(
        secret_template,
        secret_template,
        &system,
        &auto,
        &vault,
        &literals,
    )
    .expect("secret");

    assert!(literal_outcome.rendered.contains("from-literal"));
    let generated = secret_outcome
        .secrets
        .first()
        .expect("secret collected")
        .value
        .clone();
    assert_ne!(
        generated, "from-literal",
        "a secret must not pick up the literal value of the same category"
    );
}

#[test]
fn secret_auto_prefers_caller_provided_auto_value() {
    let config = "\
database_server:
  password: ${{secretAuto:postgres_password}}
";
    let system = SystemValues::default();
    let auto: HashMap<String, String> =
        HashMap::from([("postgres_password".to_owned(), "from-auto".to_owned())]);
    let outcome = resolve_config_file(
        config,
        config,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
    )
    .expect("resolve");
    assert_eq!(outcome.secrets[0].value, "from-auto");
}

/// `${embedded}` is not a resolver placeholder — it passes through as a plain
/// scalar and reaches td-database untouched.
#[test]
fn embedded_credential_passes_through_untouched() {
    let config = "\
database:
  apiserver:
    password: ${embedded}
";
    let system = SystemValues::default();
    let auto = HashMap::new();
    let outcome = resolve_config_file(
        config,
        config,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
    )
    .expect("resolve");
    assert!(outcome.rendered.contains("${embedded}"));
    assert!(outcome.secrets.is_empty());
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
    let outcome = resolve_config_file(
        template,
        init,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
    )
    .expect("resolve");

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

    let outcome = resolve_config_file(
        config,
        config,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
    )
    .expect("envFrom must resolve");

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

fn resolve(template: &str, init: &str) -> ResolveOutcome {
    let system = SystemValues::default();
    let auto = HashMap::new();
    resolve_config_file(
        template,
        init,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
    )
    .expect("resolve")
}

fn resolve_error(template: &str, init: &str) -> ResolverError {
    let system = SystemValues::default();
    let auto = HashMap::new();
    match resolve_config_file(
        template,
        init,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
    ) {
        Ok(_) => panic!("expected resolve error"),
        Err(error) => error,
    }
}

#[test]
fn literal_inlines_the_init_value() {
    let template = "name: ${{literal:label}}\n";
    let init = "name: production\n";
    let outcome = resolve(template, init);
    assert!(outcome.rendered.contains("name: production"));
    assert!(outcome.secrets.is_empty());
}

#[test]
fn literal_quoted_numeric_init_coerces_to_number() {
    let template = "count: ${{literal:n}}\n";
    let init = "count: \"7\"\n";
    let outcome = resolve(template, init);
    let value: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    assert_eq!(value["count"], Value::Number(7.into()));
}

#[test]
fn non_string_init_scalar_is_structure_mismatch() {
    let template = "count: ${{literal:n}}\n";
    let init = "count: 7\n";
    let error = resolve_error(template, init);
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

#[test]
fn literal_required_missing_value_errors() {
    let template = "name: ${{literal:label}}\n";
    let init = "name: ${{literal:label}}\n";
    let error = resolve_error(template, init);
    assert!(matches!(error, ResolverError::MissingRequiredValue { .. }));
}

#[test]
fn literal_optional_missing_value_renders_empty() {
    let template = "name: ${{literal:label?}}\n";
    let init = "name: ${{literal:label?}}\n";
    let outcome = resolve(template, init);
    let value: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    assert_eq!(value["name"], Value::String(String::new()));
}

#[test]
fn env_placeholder_retains_token_at_creation() {
    let template = "home: ${{env:HOME}}\n";
    let init = "home: ${{env:HOME}}\n";
    let outcome = resolve(template, init);
    assert!(outcome.rendered.contains("${{env:HOME}}"));
}

#[test]
fn embedded_system_placeholder_splices_into_surrounding_text() {
    let template = "address: 0.0.0.0:${{system:tdk_apiserver_public_port}}\n";
    let init = "address: 0.0.0.0:${{system:tdk_apiserver_public_port}}\n";
    let system = SystemValues {
        apiserver_public_port: Some("2457".to_owned()),
        ..SystemValues::default()
    };
    let auto = HashMap::new();
    let outcome = resolve_config_file(
        template,
        init,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
    )
    .expect("resolve embedded");
    assert!(outcome.rendered.contains("0.0.0.0:2457"));
}

#[test]
fn non_editable_placeholder_modified_in_init_errors() {
    let template = "port: ${{system:tdk_apiserver_public_port}}\n";
    let init = "port: \"9999\"\n";
    let system = SystemValues {
        apiserver_public_port: Some("2457".to_owned()),
        ..SystemValues::default()
    };
    let auto = HashMap::new();
    let error = match resolve_config_file(
        template,
        init,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
    ) {
        Ok(_) => panic!("modified system value must error"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        ResolverError::NonEditablePlaceholderModified { .. }
    ));
}

#[test]
fn structure_mismatch_when_init_shape_differs() {
    let template = "database:\n  password: ${{literal:pw}}\n";
    let init = "database: scalar\n";
    let error = resolve_error(template, init);
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

#[test]
fn missing_key_in_init_is_structure_mismatch() {
    let template = "a: ${{literal:x}}\nb: ${{literal:y}}\n";
    let init = "a: one\n";
    let error = resolve_error(template, init);
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

#[test]
fn positional_list_length_mismatch_errors() {
    let template = "hosts:\n- ${{literal:a}}\n- ${{literal:b}}\n";
    let init = "hosts:\n- one\n";
    let error = resolve_error(template, init);
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

#[test]
fn positional_scalar_list_inlines_each_item() {
    let template = "hosts:\n- ${{literal:a}}\n- ${{literal:b}}\n";
    let init = "hosts:\n- alpha\n- beta\n";
    let outcome = resolve(template, init);
    let value: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    let hosts = value["hosts"].as_sequence().expect("hosts");
    assert_eq!(hosts[0], Value::String("alpha".to_owned()));
    assert_eq!(hosts[1], Value::String("beta".to_owned()));
}

#[test]
fn keyed_sequence_matches_items_by_identity() {
    let template = "\
mounts:
- id: ROOT
  uri: ${{literal:root}}
- id: DATA
  uri: ${{literal:data}}
";
    let init = "\
mounts:
- id: DATA
  uri: /data
- id: ROOT
  uri: /
";
    let outcome = resolve(template, init);
    let value: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    let mounts = value["mounts"].as_sequence().expect("mounts");
    let data = mounts
        .iter()
        .find(|m| m["id"].as_str() == Some("DATA"))
        .expect("DATA");
    let root = mounts
        .iter()
        .find(|m| m["id"].as_str() == Some("ROOT"))
        .expect("ROOT");
    assert_eq!(data["uri"].as_str(), Some("/data"));
    assert_eq!(root["uri"].as_str(), Some("/"));
}

#[test]
fn keyed_sequence_duplicate_identity_in_init_errors() {
    let template = "\
mounts:
- id: ROOT
  uri: ${{literal:root}}
";
    let init = "\
mounts:
- id: ROOT
  uri: /
- id: ROOT
  uri: /other
";
    let error = resolve_error(template, init);
    assert!(matches!(
        error,
        ResolverError::SequenceDuplicateFirstValues { .. }
    ));
}

#[test]
fn diverging_value_for_same_placeholder_identity_errors() {
    let template = "a: ${{literal:shared}}\nb: ${{literal:shared}}\n";
    let init = "a: one\nb: two\n";
    let error = resolve_error(template, init);
    assert!(matches!(
        error,
        ResolverError::DivergingPlaceholderValue { .. }
    ));
}

#[test]
fn same_value_for_same_placeholder_identity_is_accepted() {
    let template = "a: ${{literal:shared}}\nb: ${{literal:shared}}\n";
    let init = "a: same\nb: same\n";
    let outcome = resolve(template, init);
    let value: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    assert_eq!(value["a"], Value::String("same".to_owned()));
    assert_eq!(value["b"], Value::String("same".to_owned()));
}

#[test]
fn embedded_retained_placeholder_is_rejected() {
    let template = "uri: prefix-${{secret}}\n";
    let init = "uri: prefix-topsecret\n";
    let error = resolve_error(template, init);
    assert!(matches!(
        error,
        ResolverError::EmbeddedRetainedPlaceholder { .. }
    ));
}

#[test]
fn plain_scalar_without_placeholder_passes_through() {
    let template = "name: literal-value\n";
    let init = "name: literal-value\n";
    let outcome = resolve(template, init);
    assert!(outcome.rendered.contains("name: literal-value"));
    assert!(outcome.secrets.is_empty());
}

#[test]
fn system_null_renders_as_a_yaml_null() {
    let template = "postgres:\n  ssl_root_cert: ${{system:tdk_database_ssl_root_cert}}\n";
    let system = SystemValues {
        database_ssl_root_cert: Some("null".to_owned()),
        ..SystemValues::default()
    };
    let auto = HashMap::new();
    let outcome = resolve_config_file(
        template,
        template,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
    )
    .expect("resolve");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&outcome.rendered).expect("parse");
    assert!(
        parsed["postgres"]["ssl_root_cert"].is_null(),
        "an unset system value must render as a yaml null, got: {}",
        outcome.rendered
    );
}
