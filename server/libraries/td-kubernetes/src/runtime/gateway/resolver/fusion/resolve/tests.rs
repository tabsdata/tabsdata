//
// Copyright 2026 Tabsdata Inc.
//

use indoc::indoc;

use super::*;
use crate::runtime::gateway::resolver::placeholder::SystemValues;

#[test]
fn secret_auto_collects_generated_value_and_retains_token() {
    let config = "\
database_server:
  username: ${{secretAuto:postgres_system_user}}
  password: ${{secretAuto:postgres_system_password}}
";
    let system = SystemValues::default();
    let auto = HashMap::new();
    let outcome = resolve_config_file(
        "config.yaml",
        config,
        config,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
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
    password: ${{secretAuto:postgres_system_password}}
  aiagent:
    password: ${{secretAuto:postgres_system_password}}
";
    let second = "\
database_server:
  password: ${{secretAuto:postgres_system_password}}
";
    let system = SystemValues::default();
    let auto = HashMap::new();
    let vault = RefCell::new(HashMap::new());
    let literals = RefCell::new(HashMap::new());
    let values = RefCell::new(HashMap::new());
    let first_outcome = resolve_config_file(
        "config.yaml",
        first,
        first,
        &system,
        &auto,
        &vault,
        &literals,
        &values,
        None,
    )
    .expect("first");
    let second_outcome = resolve_config_file(
        "config.yaml",
        second,
        second,
        &system,
        &auto,
        &vault,
        &literals,
        &values,
        None,
    )
    .expect("second");

    let values: std::collections::BTreeSet<_> = first_outcome
        .secrets
        .iter()
        .chain(second_outcome.secrets.iter())
        .map(|entry| entry.value.clone())
        .collect();
    assert_eq!(
        values.len(),
        1,
        "all postgres_system_password occurrences must share one value: {values:?}"
    );
    assert!(!values.iter().next().expect("value").is_empty());
}

#[test]
fn literal_same_category_reuses_value_across_configs() {
    let first_template = indoc! {"
        database:
          host: ${{literal:dbhost}}
    "};
    let first_init = indoc! {"
        database:
          host: pg.internal
    "};
    let second_template = indoc! {"
        dbmanager:
          host: ${{literal:dbhost}}
    "};
    let second_init = indoc! {"
        dbmanager:
          host: ${{literal:dbhost}}
    "};
    let system = SystemValues::default();
    let auto = HashMap::new();
    let vault = RefCell::new(HashMap::new());
    let literals = RefCell::new(HashMap::new());
    let values = RefCell::new(HashMap::new());

    let first = resolve_config_file(
        "config.yaml",
        first_template,
        first_init,
        &system,
        &auto,
        &vault,
        &literals,
        &values,
        None,
    )
    .expect("first");
    let second = resolve_config_file(
        "config.yaml",
        second_template,
        second_init,
        &system,
        &auto,
        &vault,
        &literals,
        &values,
        None,
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
    let literal_template = indoc! {"
        plain:
          name: ${{literal:postgres_system_password}}
    "};
    let literal_init = indoc! {"
        plain:
          name: from-literal
    "};
    let secret_template = indoc! {"
        guarded:
          password: ${{secretAuto:postgres_system_password}}
    "};
    let system = SystemValues::default();
    let auto = HashMap::new();
    let vault = RefCell::new(HashMap::new());
    let literals = RefCell::new(HashMap::new());
    let values = RefCell::new(HashMap::new());

    let literal_outcome = resolve_config_file(
        "config.yaml",
        literal_template,
        literal_init,
        &system,
        &auto,
        &vault,
        &literals,
        &values,
        None,
    )
    .expect("literal");
    let secret_outcome = resolve_config_file(
        "config.yaml",
        secret_template,
        secret_template,
        &system,
        &auto,
        &vault,
        &literals,
        &values,
        None,
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
  password: ${{secretAuto:postgres_system_password}}
";
    let system = SystemValues::default();
    let auto: HashMap<String, String> = HashMap::from([(
        "postgres_system_password".to_owned(),
        "from-auto".to_owned(),
    )]);
    let outcome = resolve_config_file(
        "config.yaml",
        config,
        config,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
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
        "config.yaml",
        config,
        config,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
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
        "config.yaml",
        template,
        init,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
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
        "config.yaml",
        config,
        config,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
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
        "config.yaml",
        template,
        init,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
    )
    .expect("resolve")
}

fn resolve_error(template: &str, init: &str) -> ResolverError {
    let system = SystemValues::default();
    let auto = HashMap::new();
    match resolve_config_file(
        "config.yaml",
        template,
        init,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
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
fn literal_bare_numeric_init_stays_a_number() {
    let template = "count: ${{literal:n}}\n";
    let init = "count: 7\n";
    let outcome = resolve(template, init);
    let value: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    assert_eq!(value["count"], Value::Number(7.into()));
}

#[test]
fn literal_bare_boolean_init_stays_a_boolean() {
    let template = "enabled: ${{literal:flag}}\n";
    let init = "enabled: true\n";
    let outcome = resolve(template, init);
    let value: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    assert_eq!(value["enabled"], Value::Bool(true));
}

#[test]
fn non_scalar_init_value_is_structure_mismatch() {
    let template = "count: ${{literal:n}}\n";
    let init = indoc! {"
        count:
          nested: 7
    "};
    let error = resolve_error(template, init);
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

#[test]
fn an_empty_template_mapping_takes_the_keys_the_user_supplies() {
    let template = indoc! {"
        service_account:
          annotations: {}
    "};
    let init = indoc! {"
        service_account:
          annotations:
            eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/apiserver
    "};
    let value: Value = serde_yaml::from_str(&resolve(template, init).rendered).expect("yaml");
    assert_eq!(
        value["service_account"]["annotations"]["eks.amazonaws.com/role-arn"],
        Value::String("arn:aws:iam::123456789012:role/apiserver".to_owned())
    );
}

#[test]
fn an_empty_template_mapping_left_empty_stays_empty() {
    let template = "selectors: {}\n";
    let value: Value = serde_yaml::from_str(&resolve(template, template).rendered).expect("yaml");
    assert_eq!(
        value["selectors"],
        Value::Mapping(serde_yaml::Mapping::new())
    );
}

#[test]
fn keys_beyond_a_non_empty_template_mapping_are_still_refused() {
    let template = indoc! {"
        resources:
          cpu: ${{literal:cpu}}
    "};
    let init = indoc! {"
        resources:
          cpu: \"2\"
          gpu: \"1\"
    "};
    let error = resolve_error(template, init);
    assert!(matches!(error, ResolverError::UnknownConfigKeys { .. }));
}

#[test]
fn an_empty_template_mapping_given_a_scalar_is_structure_mismatch() {
    let template = "selectors: {}\n";
    let init = "selectors: zone-a\n";
    let error = resolve_error(template, init);
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

#[test]
fn the_shipped_eks_config_keeps_the_apiserver_role_annotation() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../variant/resources/kprofile/configs/tabsdata_eks.yaml");
    let template = std::fs::read_to_string(&path)
        .expect("shipped eks config")
        .replace("\r\n", "\n");
    let annotated = template.replacen(
        "      service_account:\n        annotations: {}",
        "      service_account:\n        annotations:\n          \
         eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/apiserver",
        1,
    );
    assert_ne!(
        annotated, template,
        "the eks config declares the apiserver annotations"
    );
    let rendered = resolve(&template, &annotated).rendered;
    let config = crate::runtime::instance::tabsdata::TabsdataConfig::parse(&rendered)
        .expect("rendered config parses");
    assert_eq!(
        config
            .component("apiserver", Some("authorization"))
            .service_account
            .annotations
            .get("eks.amazonaws.com/role-arn")
            .map(String::as_str),
        Some("arn:aws:iam::123456789012:role/apiserver")
    );
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
        "config.yaml",
        template,
        init,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
    )
    .expect("resolve embedded");
    assert!(outcome.rendered.contains("0.0.0.0:2457"));
}

#[test]
fn the_version_label_resolves_from_the_tabsdata_version() {
    let template = indoc! {r#"
        metadata:
          labels:
            app.kubernetes.io/version: "${{system:tdk_tabsdata_version}}"
    "#};
    let system = SystemValues {
        tabsdata_version: Some("2.0.1".to_owned()),
        ..SystemValues::default()
    };
    let auto = HashMap::new();
    let outcome = resolve_config_file(
        "pod.yaml",
        template,
        template,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
    )
    .expect("resolve the version label");
    assert!(
        outcome.rendered.contains("2.0.1"),
        "rendered was: {}",
        outcome.rendered
    );
}

#[test]
fn the_dbmanager_mode_resolves_in_the_name_and_the_argument() {
    let template = indoc! {"
        name: 1000-db${{system:tdk_dbmanager_mode}}
        args:
          - db
          - ${{system:tdk_dbmanager_mode}}
    "};
    for (mode, container) in [("create", "1000-dbcreate"), ("upgrade", "1000-dbupgrade")] {
        let system = SystemValues {
            dbmanager_mode: Some(mode.to_owned()),
            ..SystemValues::default()
        };
        let auto = HashMap::new();
        let outcome = resolve_config_file(
            "dbmanager.yaml",
            template,
            template,
            &system,
            &auto,
            &RefCell::new(HashMap::new()),
            &RefCell::new(HashMap::new()),
            &RefCell::new(HashMap::new()),
            None,
        )
        .expect("resolve the dbmanager mode");
        assert!(
            outcome.rendered.contains(container),
            "rendered was: {}",
            outcome.rendered
        );
        assert!(outcome.rendered.contains(&format!("- {mode}")));
    }
}

#[test]
fn venv_archive_name_splices_cluster_and_namespace() {
    let template =
        "image: tdvenvs-${{system:tdk_cluster_name}}-${{system:tdk_namespace_name}}.tar.zst\n";
    let system = SystemValues {
        cluster_name: Some("tabsdata-dev".to_owned()),
        namespace_name: Some("tabsdatao15".to_owned()),
        ..SystemValues::default()
    };
    let auto = HashMap::new();
    let outcome = resolve_config_file(
        "config.yaml",
        template,
        template,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
    )
    .expect("resolve archive name");
    assert!(
        outcome
            .rendered
            .contains("tdvenvs-tabsdata-dev-tabsdatao15.tar.zst")
    );
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
        "config.yaml",
        template,
        init,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
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
    let template = indoc! {"
        database:
          password: ${{literal:pw}}
    "};
    let init = "database: scalar\n";
    let error = resolve_error(template, init);
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

#[test]
fn missing_key_in_init_is_structure_mismatch() {
    let template = indoc! {"
        a: ${{literal:x}}
        b: ${{literal:y}}
    "};
    let init = "a: one\n";
    let error = resolve_error(template, init);
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

#[test]
fn positional_list_length_mismatch_errors() {
    let template = indoc! {"
        hosts:
        - ${{literal:a}}
        - ${{literal:b}}
    "};
    let init = indoc! {"
        hosts:
        - one
    "};
    let error = resolve_error(template, init);
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

#[test]
fn positional_scalar_list_inlines_each_item() {
    let template = indoc! {"
        hosts:
        - ${{literal:a}}
        - ${{literal:b}}
    "};
    let init = indoc! {"
        hosts:
        - alpha
        - beta
    "};
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
    let template = indoc! {"
        a: ${{literal:shared}}
        b: ${{literal:shared}}
    "};
    let init = indoc! {"
        a: one
        b: two
    "};
    let error = resolve_error(template, init);
    assert!(matches!(
        error,
        ResolverError::DivergingPlaceholderValue { .. }
    ));
}

#[test]
fn same_value_for_same_placeholder_identity_is_accepted() {
    let template = indoc! {"
        a: ${{literal:shared}}
        b: ${{literal:shared}}
    "};
    let init = indoc! {"
        a: same
        b: same
    "};
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
    let template = indoc! {"
        postgres:
          ssl_root_cert: ${{system:tdk_database_ssl_root_cert}}
    "};
    let system = SystemValues {
        database_ssl_root_cert: Some("null".to_owned()),
        ..SystemValues::default()
    };
    let auto = HashMap::new();
    let outcome = resolve_config_file(
        "config.yaml",
        template,
        template,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
    )
    .expect("resolve");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&outcome.rendered).expect("parse");
    assert!(
        parsed["postgres"]["ssl_root_cert"].is_null(),
        "an unset system value must render as a yaml null, got: {}",
        outcome.rendered
    );
}

#[test]
fn tabsdata_version_splices_into_a_pinned_requirement() {
    let template = "package: tabsdata[all]==${{system:tdk_tabsdata_version}}\n";
    let system = SystemValues {
        tabsdata_version: Some("2.0.0".to_owned()),
        ..SystemValues::default()
    };
    let auto = HashMap::new();
    let outcome = resolve_config_file(
        "config.yaml",
        template,
        template,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
    )
    .expect("resolve version");
    assert!(outcome.rendered.contains("tabsdata[all]==2.0.0"));
}
fn resolve_pair(
    first: (&str, &str, &str),
    second: (&str, &str, &str),
) -> (ResolveOutcome, ResolveOutcome) {
    let system = SystemValues::default();
    let auto = HashMap::new();
    let vault = RefCell::new(HashMap::new());
    let literals = RefCell::new(HashMap::new());
    let values = RefCell::new(HashMap::new());
    let first_outcome = resolve_config_file(
        first.0, first.1, first.2, &system, &auto, &vault, &literals, &values, None,
    )
    .expect("first file");
    let second_outcome = resolve_config_file(
        second.0, second.1, second.2, &system, &auto, &vault, &literals, &values, None,
    )
    .expect("second file");
    (first_outcome, second_outcome)
}

#[test]
fn a_supplied_secret_settles_the_same_category_in_another_file() {
    let template = indoc! {"
        server:
          username: ${{secret:postgres_system_user}}
    "};
    let supplied = indoc! {"
        server:
          username: dbadmin
    "};
    let (first, second) = resolve_pair(
        ("dbmanager.database.yaml", template, supplied),
        ("apiserver.database.yaml", template, template),
    );
    assert_eq!(first.secrets[0].value, "dbadmin");
    assert_eq!(
        second.secrets[0].value, "dbadmin",
        "the second file must reuse the value supplied by the first"
    );
}

#[test]
fn a_supplied_literal_settles_the_same_category_in_another_file() {
    let template = indoc! {"
        server:
          host: ${{literal:tdk_database_host}}
    "};
    let supplied = indoc! {"
        server:
          host: dimax
    "};
    let (first, second) = resolve_pair(
        ("dbmanager.database.yaml", template, supplied),
        ("apiserver.database.yaml", template, template),
    );
    assert!(first.rendered.contains("host: dimax"));
    assert!(
        second.rendered.contains("host: dimax"),
        "the second file must reuse the literal supplied by the first"
    );
}

#[test]
fn a_mandatory_secret_missing_everywhere_still_fails() {
    let template = indoc! {"
        server:
          username: ${{secret:postgres_system_user}}
    "};
    let system = SystemValues::default();
    let auto = HashMap::new();
    let error = resolve_config_file(
        "apiserver.database.yaml",
        template,
        template,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
    )
    .expect_err("must fail");
    assert!(matches!(error, ResolverError::MissingRequiredValue { .. }));
}

#[test]
fn an_error_names_the_offending_file() {
    let template = indoc! {"
        server:
          username: ${{secret:postgres_system_user}}
    "};
    let system = SystemValues::default();
    let auto = HashMap::new();
    let error = resolve_config_file(
        "apiserver.database.yaml",
        template,
        template,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
    )
    .expect_err("must fail");
    let message = error.to_string();
    assert!(
        message.contains("apiserver.database.yaml:server.username"),
        "the error must name the file and the node, got: {message}"
    );
}

#[test]
fn an_optional_secret_stays_empty_when_no_file_supplies_it() {
    let template = indoc! {"
        agent:
          key: ${{secret:openai-key?}}
    "};
    let (first, second) = resolve_pair(
        ("aiagent.yaml", template, template),
        ("other.yaml", template, template),
    );
    assert_eq!(first.secrets[0].value, "");
    assert_eq!(second.secrets[0].value, "");
}

#[test]
fn an_optional_secret_supplied_once_settles_the_other_file() {
    let template = indoc! {"
        agent:
          key: ${{secret:openai-key?}}
    "};
    let (first, second) = resolve_pair(
        (
            "aiagent.yaml",
            template,
            indoc! {"
            agent:
              key: sk-real
        "},
        ),
        ("other.yaml", template, template),
    );
    assert_eq!(first.secrets[0].value, "sk-real");
    assert_eq!(
        second.secrets[0].value, "sk-real",
        "an optional secret supplied once must settle the rest"
    );
}

#[test]
fn two_files_giving_the_same_value_is_accepted() {
    let template = indoc! {"
        server:
          host: ${{literal:tdk_database_host}}
    "};
    let supplied = indoc! {"
        server:
          host: dimax
    "};
    let (first, second) = resolve_pair(
        ("dbmanager.database.yaml", template, supplied),
        ("apiserver.database.yaml", template, supplied),
    );
    assert!(first.rendered.contains("host: dimax"));
    assert!(second.rendered.contains("host: dimax"));
}

#[test]
fn diverging_values_across_files_error() {
    let template = indoc! {"
        server:
          host: ${{literal:tdk_database_host}}
    "};
    let system = SystemValues::default();
    let auto = HashMap::new();
    let vault = RefCell::new(HashMap::new());
    let literals = RefCell::new(HashMap::new());
    let values = RefCell::new(HashMap::new());
    resolve_config_file(
        "dbmanager.database.yaml",
        template,
        indoc! {"
            server:
              host: first
        "},
        &system,
        &auto,
        &vault,
        &literals,
        &values,
        None,
    )
    .expect("first file");
    let error = resolve_config_file(
        "apiserver.database.yaml",
        template,
        indoc! {"
            server:
              host: second
        "},
        &system,
        &auto,
        &vault,
        &literals,
        &values,
        None,
    )
    .expect_err("a second, different value must be rejected");
    assert!(matches!(
        error,
        ResolverError::DivergingPlaceholderValue { .. }
    ));
    assert!(
        error.to_string().contains("apiserver.database.yaml"),
        "the error must name the offending file, got: {error}"
    );
}

#[test]
fn diverging_secret_values_across_files_error() {
    let template = indoc! {"
        server:
          username: ${{secret:postgres_system_user}}
    "};
    let system = SystemValues::default();
    let auto = HashMap::new();
    let vault = RefCell::new(HashMap::new());
    let literals = RefCell::new(HashMap::new());
    let values = RefCell::new(HashMap::new());
    resolve_config_file(
        "dbmanager.database.yaml",
        template,
        indoc! {"
            server:
              username: dbadmin
        "},
        &system,
        &auto,
        &vault,
        &literals,
        &values,
        None,
    )
    .expect("first file");
    let error = resolve_config_file(
        "apiserver.database.yaml",
        template,
        indoc! {"
            server:
              username: someoneelse
        "},
        &system,
        &auto,
        &vault,
        &literals,
        &values,
        None,
    )
    .expect_err("a second, different value must be rejected");
    assert!(matches!(
        error,
        ResolverError::DivergingPlaceholderValue { .. }
    ));
    let message = error.to_string();
    assert!(
        !message.contains("dbadmin") && !message.contains("someoneelse"),
        "a divergence report must not leak the secret values, got: {message}"
    );
}

#[test]
fn unqualified_placeholders_are_independent_of_each_other() {
    let template = "\
storage:
  mounts:
    ROOT:
      options:
        aws_region: ${{secret}}
        aws_access_key: ${{secret}}
";
    let init = "\
storage:
  mounts:
    ROOT:
      options:
        aws_region: eu-west-1
        aws_access_key: AKIAEXAMPLE
";
    let outcome = resolve(template, init);
    let values: Vec<String> = outcome
        .secrets
        .iter()
        .map(|entry| entry.value.clone())
        .collect();
    assert!(values.contains(&"eu-west-1".to_owned()));
    assert!(values.contains(&"AKIAEXAMPLE".to_owned()));
}

#[test]
fn unqualified_placeholders_across_files_are_independent() {
    let template = indoc! {"
        options:
          token: ${{secret}}
    "};
    let (first, second) = resolve_pair(
        (
            "apiserver.storage.yaml",
            template,
            indoc! {"
                options:
                  token: one
            "},
        ),
        (
            "aiagent.yaml",
            template,
            indoc! {"
            options:
              token: two
        "},
        ),
    );
    assert_eq!(first.secrets[0].value, "one");
    assert_eq!(
        second.secrets[0].value, "two",
        "an unqualified placeholder names nothing, so occurrences stand alone"
    );
}

#[test]
fn unqualified_literals_are_independent_of_each_other() {
    let template = indoc! {"
        a: ${{literal}}
        b: ${{literal}}
    "};
    let init = indoc! {"
        a: one
        b: two
    "};
    let outcome = resolve(template, init);
    let value: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    assert_eq!(value["a"], Value::String("one".to_owned()));
    assert_eq!(value["b"], Value::String("two".to_owned()));
}

#[test]
fn an_optional_mapping_lets_the_initialization_omit_keys() {
    let template = concat!(
        "storage:\n",
        "  mounts:\n",
        "  - id: ROOT\n",
        "    options:\n",
        "      aws_region: ${{secret:storage_region}}\n",
        "      google_service_account_key: ${{secret:storage_account_key}}\n",
    );
    let supplied = concat!(
        "storage:\n",
        "  mounts:\n",
        "  - id: ROOT\n",
        "    options:\n",
        "      google_service_account_key: a-key\n",
    );
    let schema = Schema::parse(
        "apiserver.storage.yaml",
        indoc! {"
            directives:
              - at: storage.mounts[*].options
                treat: optional
        "},
    )
    .expect("schema");
    let system = SystemValues::default();
    let auto = HashMap::new();
    let outcome = resolve_config_file(
        "apiserver.storage.yaml",
        template,
        supplied,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        Some(&schema),
    )
    .expect("resolve");
    assert!(outcome.rendered.contains("google_service_account_key"));
    assert!(
        !outcome.rendered.contains("aws_region"),
        "an omitted key must not be rendered"
    );
    assert!(
        outcome.secrets.iter().any(|secret| secret.value == "a-key"),
        "the supplied value must still be collected as a secret"
    );
}

#[test]
fn an_optional_mapping_still_rejects_a_missing_key_elsewhere() {
    let template = indoc! {"
        storage:
          mounts:
          - id: ROOT
            uri: gs://bucket/
    "};
    let supplied = indoc! {"
        storage:
          mounts:
          - id: ROOT
    "};
    let schema = Schema::parse(
        "apiserver.storage.yaml",
        indoc! {"
            directives:
              - at: storage.mounts[*].options
                treat: optional
        "},
    )
    .expect("schema");
    let system = SystemValues::default();
    let auto = HashMap::new();
    let error = resolve_config_file(
        "apiserver.storage.yaml",
        template,
        supplied,
        &system,
        &auto,
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        Some(&schema),
    )
    .expect_err("uri is not covered by the directive");
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

fn resolve_with_schema(
    template: &str,
    supplied: &str,
    directives: &str,
) -> Result<ResolveOutcome, ResolverError> {
    let schema = Schema::parse("apiserver.storage.yaml", directives).expect("schema");
    resolve_config_file(
        "apiserver.storage.yaml",
        template,
        supplied,
        &SystemValues::default(),
        &HashMap::new(),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        Some(&schema),
    )
}

const OPEN_SECRET_OPTIONS: &str = indoc! {"
    directives:
      - at: storage.mounts[*].options.*
        treat: secret
      - at: storage.mounts[*].options
        treat: open
"};

#[test]
fn an_open_mapping_keeps_a_key_the_template_lacks_as_a_secret() {
    let template = indoc! {"
        storage:
          mounts:
          - id: ROOT
            uri: ${{secret}}
            options: {}
    "};
    let supplied = indoc! {"
        storage:
          mounts:
          - id: ROOT
            uri: s3://bucket/root/
            options:
              aws_access_key_id: an-id
    "};
    let outcome = resolve_with_schema(template, supplied, OPEN_SECRET_OPTIONS).expect("resolve");
    let value: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    assert_eq!(
        value["storage"]["mounts"][0]["options"]["aws_access_key_id"],
        Value::String("${{secret}}".to_owned())
    );
    assert!(
        outcome.secrets.iter().any(|secret| {
            secret.key == "STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ACCESS_KEY_ID"
                && secret.value == "an-id"
        }),
        "the added key must be collected as a secret"
    );
}

#[test]
fn an_open_mapping_drops_a_template_key_the_initialization_omits() {
    let template = indoc! {"
        storage:
          mounts:
          - id: ROOT
            options:
              aws_region: ${{secret}}
    "};
    let supplied = indoc! {"
        storage:
          mounts:
          - id: ROOT
            options:
              azure_storage_account_name: an-account
    "};
    let outcome = resolve_with_schema(template, supplied, OPEN_SECRET_OPTIONS).expect("resolve");
    assert!(!outcome.rendered.contains("aws_region"));
    assert!(outcome.rendered.contains("azure_storage_account_name"));
    assert_eq!(outcome.secrets.len(), 1);
}

#[test]
fn an_open_mapping_keeps_an_empty_initialization_empty() {
    let template = indoc! {"
        storage:
          mounts:
          - id: ROOT
            options: {}
    "};
    let outcome = resolve_with_schema(template, template, OPEN_SECRET_OPTIONS).expect("resolve");
    let value: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    assert_eq!(
        value["storage"]["mounts"][0]["options"],
        Value::Mapping(Mapping::new())
    );
    assert!(outcome.secrets.is_empty());
}

#[test]
fn an_open_mapping_keeps_a_key_no_secret_rule_covers_as_given() {
    let template = indoc! {"
        storage:
          mounts:
          - id: ROOT
            options: {}
    "};
    let supplied = indoc! {"
        storage:
          mounts:
          - id: ROOT
            options:
              allow_http: 'true'
    "};
    let directives = indoc! {"
        directives:
          - at: storage.mounts[*].options
            treat: open
    "};
    let outcome = resolve_with_schema(template, supplied, directives).expect("resolve");
    let value: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    assert_eq!(
        value["storage"]["mounts"][0]["options"]["allow_http"],
        Value::String("true".to_owned())
    );
    assert!(outcome.secrets.is_empty());
}

#[test]
fn an_open_mapping_still_rejects_a_missing_key_elsewhere() {
    let template = indoc! {"
        storage:
          mounts:
          - id: ROOT
            uri: gs://bucket/
            options: {}
    "};
    let supplied = indoc! {"
        storage:
          mounts:
          - id: ROOT
            options: {}
    "};
    let error = resolve_with_schema(template, supplied, OPEN_SECRET_OPTIONS)
        .expect_err("uri is not covered by the directive");
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

#[test]
fn a_list_the_schema_reaches_names_its_items_by_first_value_without_placeholders() {
    let template = indoc! {"
        storage:
          mounts:
          - id: ROOT
            options: {}
    "};
    let supplied = indoc! {"
        storage:
          mounts:
          - id: ROOT
            options:
              aws_access_key_id: an-id
    "};
    let outcome = resolve_with_schema(template, supplied, OPEN_SECRET_OPTIONS).expect("resolve");
    let keys: Vec<&str> = outcome
        .secrets
        .iter()
        .map(|secret| secret.key.as_str())
        .collect();
    assert_eq!(keys, ["STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ACCESS_KEY_ID"]);
}

#[test]
fn a_list_the_schema_does_not_reach_still_pairs_its_items_by_position() {
    let template = indoc! {"
        tolerations:
        - key: a
        - key: b
    "};
    let supplied = indoc! {"
        tolerations:
        - key: a
    "};
    let error = resolve_with_schema(template, supplied, OPEN_SECRET_OPTIONS)
        .expect_err("a positional list keeps its length");
    assert!(matches!(error, ResolverError::StructureMismatch { .. }));
}

const SECRET_URI_TEMPLATE: &str = indoc! {"
    storage:
      mounts:
      - id: ROOT
        uri: ${{secret}}
"};

fn resolve_secret_uri(supplied_uri: &str) -> Result<ResolveOutcome, ResolverError> {
    let supplied = format!("storage:\n  mounts:\n  - id: ROOT\n    uri: '{supplied_uri}'\n");
    let system = SystemValues {
        storage_url: Some("file:///home/td/storage/".to_owned()),
        ..SystemValues::default()
    };
    resolve_config_file(
        "apiserver.storage.yaml",
        SECRET_URI_TEMPLATE,
        &supplied,
        &system,
        &HashMap::new(),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        None,
    )
}

fn secret_uri(outcome: &ResolveOutcome) -> &str {
    outcome
        .secrets
        .iter()
        .find(|secret| secret.key == "STORAGE__MOUNTS__ROOT__URI")
        .map(|secret| secret.value.as_str())
        .expect("the uri secret")
}

#[test]
fn a_secret_value_embedding_a_system_placeholder_is_stored_resolved() {
    let outcome = resolve_secret_uri("${{system:tdk_storage_url}}data/").expect("resolve");
    assert_eq!(secret_uri(&outcome), "file:///home/td/storage/data/");
    assert!(outcome.rendered.contains("uri: ${{secret}}"));
}

#[test]
fn a_plain_secret_value_is_stored_as_given() {
    let outcome = resolve_secret_uri("s3://bucket/root/").expect("resolve");
    assert_eq!(secret_uri(&outcome), "s3://bucket/root/");
}

#[test]
fn a_secret_value_embedding_a_placeholder_resolved_later_is_rejected() {
    let error = resolve_secret_uri("${{env:HOME}}/data/").expect_err("env resolves in the pod");
    assert!(matches!(
        error,
        ResolverError::SecretValuePlaceholderUnsupported { .. }
    ));
}

#[test]
fn the_secret_token_itself_still_counts_as_not_supplied() {
    let error = resolve_secret_uri("${{secret}}").expect_err("nothing supplied");
    assert!(matches!(error, ResolverError::MissingRequiredValue { .. }));
}
