//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::io::Write;

use indoc::indoc;

use super::*;

const TABSDATA: &str = "\
# Global settings.
pods:
  global:
    envs:
      UV_DEFAULT_INDEX: https://pypi.org/simple/
      UV_INDEX:
      UV_INDEX_STRATEGY: unsafe-best-match
  service:
    resources:
      cpu: \"2\" # Cores.
      memory: 4Gi
      replicas: 1
      enabled: true
    placement:
      selectors: {}
      tolerations:
        - key: virtual-kubelet.io/provider
          operator: Exists
";

const DATABASE: &str = "\
database_server:
  postgres:
    # Where the server listens.
    host: ${{literal:tdk_database_host}}
    port: ${{literal:tdk_database_port}}
    username: ${{secret:postgres_system_user}}
    ssl_mode: ${{system:tdk_database_ssl_mode}}
";

const APISERVER: &str = "\
database:
  host: ${{literal:tdk_database_host}}
  password: ${{secretAuto:postgres_apiserver_password}}
  shadow: ${{literal:shadowed}}
  other: ${{secret:shadowed}}
storage:
  mounts:
    - id: ROOT
      uri: ${{secret}}
    - id: EXTRA
      uri: file:///tmp/extra
addresses:
  - 127.0.0.1:${{system:tdk_private_port}}
";

fn configs() -> RenderedConfigmap {
    BTreeMap::from([
        ("tabsdata.yaml".to_owned(), TABSDATA.to_owned()),
        ("dbmanager.database.yaml".to_owned(), DATABASE.to_owned()),
        ("apiserver.database.yaml".to_owned(), APISERVER.to_owned()),
        (
            "apiserver.storage.yamls".to_owned(),
            "directives: []\n".to_owned(),
        ),
        ("aiagent.req.txt".to_owned(), "anthropic\n".to_owned()),
    ])
}

fn settings(arguments: &[&str]) -> Vec<Setting> {
    let arguments: Vec<String> = arguments.iter().map(|text| (*text).to_owned()).collect();
    parse_settings(&arguments).expect("valid settings")
}

fn injected(arguments: &[&str]) -> BTreeMap<String, String> {
    plan(&configs(), &settings(arguments)).expect("injected")
}

fn failure(arguments: &[&str]) -> InjectorError {
    plan(&configs(), &settings(arguments)).expect_err("rejected")
}

fn read(content: &str) -> Value {
    serde_yaml::from_str(content).expect("valid yaml")
}

#[test]
fn a_category_fills_every_file_that_uses_it() {
    let files = injected(&["tdk_database_host=db.internal"]);
    assert_eq!(files.len(), 2);
    assert!(files["dbmanager.database.yaml"].contains("    host: db.internal\n"));
    assert!(files["apiserver.database.yaml"].contains("  host: db.internal\n"));
}

#[test]
fn comments_and_untouched_lines_survive() {
    let files = injected(&["tdk_database_host=db.internal"]);
    let database = &files["dbmanager.database.yaml"];
    assert!(database.contains("    # Where the server listens.\n"));
    assert_eq!(
        database.replace("db.internal", "${{literal:tdk_database_host}}"),
        DATABASE
    );
}

#[test]
fn a_qualified_category_resolves_the_shared_name() {
    let files = injected(&["secret:shadowed=hidden", "literal:shadowed=visible"]);
    let apiserver = &files["apiserver.database.yaml"];
    assert!(apiserver.contains("  shadow: visible\n"));
    assert!(apiserver.contains("  other: hidden\n"));
}

#[test]
fn an_unqualified_shared_category_is_ambiguous() {
    assert!(matches!(
        failure(&["shadowed=value"]),
        InjectorError::AmbiguousCategory { count: 2, .. }
    ));
}

#[test]
fn a_numeric_text_for_a_placeholder_is_quoted() {
    let files = injected(&["tdk_database_port=5432"]);
    assert!(files["dbmanager.database.yaml"].contains("    port: '5432'\n"));
}

#[test]
fn a_secret_auto_placeholder_takes_a_value() {
    let files = injected(&["postgres_apiserver_password=s3cr#t value"]);
    let apiserver = read(&files["apiserver.database.yaml"]);
    assert_eq!(apiserver["database"]["password"], "s3cr#t value");
}

#[test]
fn a_system_placeholder_cannot_be_set() {
    assert!(matches!(
        failure(&["tdk_database_ssl_mode=require"]),
        InjectorError::NonEditablePlaceholder { .. }
    ));
}

#[test]
fn an_embedded_system_placeholder_cannot_be_set() {
    assert!(matches!(
        failure(&["tdk_private_port=1"]),
        InjectorError::NonEditablePlaceholder { .. }
    ));
}

#[test]
fn an_unknown_category_suggests_the_closest_one() {
    match failure(&["tdk_database_hots=x"]) {
        InjectorError::UnknownCategory { suggestion, .. } => {
            assert!(suggestion.contains("tdk_database_host"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn an_empty_value_for_a_mandatory_placeholder_is_rejected() {
    assert!(matches!(
        failure(&["tdk_database_host="]),
        InjectorError::EmptyMandatoryValue { .. }
    ));
}

#[test]
fn a_string_leaf_keeps_its_type() {
    let files = injected(&["tabsdata:pods.service.resources.cpu=4"]);
    assert!(files["tabsdata.yaml"].contains("      cpu: '4' # Cores.\n"));
}

#[test]
fn a_number_leaf_takes_only_numbers() {
    let files = injected(&["tabsdata.yaml:pods.service.resources.replicas=3"]);
    assert!(files["tabsdata.yaml"].contains("      replicas: 3\n"));
    assert!(matches!(
        failure(&["tabsdata.yaml:pods.service.resources.replicas=three"]),
        InjectorError::ValueTypeMismatch { .. }
    ));
}

#[test]
fn a_boolean_leaf_takes_only_booleans() {
    let files = injected(&["tabsdata.yaml:pods.service.resources.enabled=false"]);
    assert!(files["tabsdata.yaml"].contains("      enabled: false\n"));
    assert!(matches!(
        failure(&["tabsdata.yaml:pods.service.resources.enabled=maybe"]),
        InjectorError::ValueTypeMismatch { .. }
    ));
}

#[test]
fn an_empty_leaf_takes_a_value_after_its_colon() {
    let index = "http://localhost:8000/simple # main";
    let argument = format!("tabsdata.yaml:pods.global.envs.UV_INDEX={index}");
    let files = injected(&[argument.as_str()]);
    let tabsdata = read(&files["tabsdata.yaml"]);
    assert_eq!(tabsdata["pods"]["global"]["envs"]["UV_INDEX"], index);
    assert!(files["tabsdata.yaml"].contains("      UV_INDEX_STRATEGY: unsafe-best-match\n"));
}

#[test]
fn an_empty_leaf_infers_scalar_types() {
    let files = injected(&["tabsdata.yaml:pods.global.envs.UV_INDEX=8"]);
    assert!(files["tabsdata.yaml"].contains("      UV_INDEX: 8\n"));
}

#[test]
fn a_list_item_is_selected_by_its_identity_or_position() {
    let by_identity = injected(&["apiserver.database.yaml:storage.mounts[id=EXTRA].uri=s3://b"]);
    let by_position = injected(&["apiserver.database.yaml:storage.mounts[1].uri=s3://b"]);
    assert_eq!(by_identity, by_position);
    let apiserver = read(&by_identity["apiserver.database.yaml"]);
    assert_eq!(apiserver["storage"]["mounts"][1]["uri"], "s3://b");
}

#[test]
fn an_uncategorized_placeholder_is_set_by_its_key() {
    let files = injected(&["apiserver.database.yaml:storage.mounts[id=ROOT].uri=s3://root"]);
    let apiserver = read(&files["apiserver.database.yaml"]);
    assert_eq!(apiserver["storage"]["mounts"][0]["uri"], "s3://root");
}

#[test]
fn a_categorized_placeholder_is_not_set_by_its_key() {
    match failure(&["dbmanager.database:database_server.postgres.host=x"]) {
        InjectorError::PlaceholderAtKey { identity, .. } => {
            assert_eq!(identity, "literal:tdk_database_host");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn a_selector_must_match_exactly_one_item() {
    assert!(matches!(
        failure(&["apiserver.database.yaml:storage.mounts[id=NONE].uri=x"]),
        InjectorError::SelectorMismatch { count: 0, .. }
    ));
}

#[test]
fn only_final_nodes_can_be_set() {
    assert!(matches!(
        failure(&["tabsdata.yaml:pods.service.resources=x"]),
        InjectorError::NotALeaf { .. }
    ));
    assert!(matches!(
        failure(&["tabsdata.yaml:pods.service.placement.selectors=x"]),
        InjectorError::NotALeaf { .. }
    ));
}

#[test]
fn new_keys_cannot_be_added() {
    assert!(matches!(
        failure(&["tabsdata.yaml:pods.service.resources.gpu=1"]),
        InjectorError::KeyNotFound { .. }
    ));
    assert!(matches!(
        failure(&["tabsdata.yaml:pods.global.NEW_SECTION=x"]),
        InjectorError::KeyNotFound { .. }
    ));
    assert!(matches!(
        failure(&["apiserver.database:database.port=1"]),
        InjectorError::KeyNotFound { .. }
    ));
}

#[test]
fn a_global_env_can_be_added() {
    let content =
        &injected(&["tabsdata:pods.global.envs.TYPESAFE_API_KEY=secret-value"])["tabsdata.yaml"];
    assert!(content.contains(
        "      UV_INDEX_STRATEGY: unsafe-best-match\n      TYPESAFE_API_KEY: secret-value\n  service:"
    ));
    let config =
        crate::runtime::instance::tabsdata::TabsdataConfig::parse(content).expect("parses");
    let value = config.pods.global.envs["TYPESAFE_API_KEY"].render();
    assert_eq!(value.as_deref(), Some("secret-value"));
}

#[test]
fn an_added_global_env_is_always_a_string() {
    let content = &injected(&["tabsdata:pods.global.envs.RETRIES=3"])["tabsdata.yaml"];
    assert!(content.contains("      RETRIES: '3'\n"));
}

#[test]
fn several_global_envs_can_be_added() {
    let content = &injected(&[
        "tabsdata:pods.global.envs.FIRST=one",
        "tabsdata:pods.global.envs.SECOND=two",
    ])["tabsdata.yaml"];
    assert!(content.contains("FIRST: one\n"));
    assert!(content.contains("SECOND: two\n"));
}

#[test]
fn an_added_global_env_needs_a_valid_name() {
    assert!(matches!(
        failure(&["tabsdata:pods.global.envs.9LIVES=x"]),
        InjectorError::InvalidEnvName { .. }
    ));
    assert!(matches!(
        failure(&["tabsdata:pods.global.envs.\"MY-KEY\"=x"]),
        InjectorError::InvalidEnvName { .. }
    ));
}

#[test]
fn an_existing_global_env_is_still_replaced() {
    let content =
        &injected(&["tabsdata:pods.global.envs.UV_INDEX_STRATEGY=first-index"])["tabsdata.yaml"];
    assert!(content.contains("      UV_INDEX_STRATEGY: first-index\n"));
    assert!(!content.contains("unsafe-best-match"));
}

#[test]
fn only_editable_config_files_take_values() {
    assert!(matches!(
        failure(&["aiagent.req:requirements=x"]),
        InjectorError::UnknownFile { .. }
    ));
    assert!(matches!(
        failure(&["apiserver.storage.yamls:directives=x"]),
        InjectorError::UnknownFile { .. }
    ));
}

#[test]
fn a_value_cannot_be_specified_twice_even_when_equal() {
    assert!(matches!(
        failure(&["tdk_database_host=a", "tdk_database_host=a"]),
        InjectorError::DuplicateSetting { .. }
    ));
}

#[test]
fn different_spellings_of_one_target_are_duplicates() {
    assert!(matches!(
        failure(&["tdk_database_host=a", "literal:tdk_database_host=b"]),
        InjectorError::DuplicateSetting { .. }
    ));
    assert!(matches!(
        failure(&[
            "tabsdata:pods.service.resources.memory=8Gi",
            "tabsdata.yaml:pods.service.resources.memory=8Gi",
        ]),
        InjectorError::DuplicateSetting { .. }
    ));
    assert!(matches!(
        failure(&[
            "apiserver.database.yaml:storage.mounts[1].uri=a",
            "apiserver.database.yaml:storage.mounts[id=EXTRA].uri=b",
        ]),
        InjectorError::DuplicateSetting { .. }
    ));
}

#[test]
fn a_value_is_read_from_this_machine_environment() {
    let path = std::env::var("PATH").expect("PATH is set");
    let files = injected(&["tdk_database_host=env:PATH"]);
    let database = read(&files["dbmanager.database.yaml"]);
    assert_eq!(
        database["database_server"]["postgres"]["host"],
        path.as_str()
    );
}

#[test]
fn a_missing_environment_variable_is_rejected() {
    let arguments = vec!["tdk_database_host=env:TD_INJECTOR_TEST_SURELY_UNSET".to_owned()];
    assert!(matches!(
        parse_settings(&arguments),
        Err(InjectorError::EnvironmentVariableMissing { .. })
    ));
}

#[test]
fn braced_references_are_expanded_and_the_text_around_them_is_kept() {
    let path = std::env::var("PATH").expect("PATH is set");
    let home = std::env::var("HOME").expect("HOME is set");
    let files = injected(&["tdk_database_host=env:${PATH}/root:${ HOME }"]);
    let database = read(&files["dbmanager.database.yaml"]);
    assert_eq!(
        database["database_server"]["postgres"]["host"],
        format!("{path}/root:{home}").as_str()
    );
}

#[test]
fn a_missing_variable_in_a_braced_reference_is_rejected() {
    let arguments = vec!["tdk_database_host=env:${TD_INJECTOR_TEST_SURELY_UNSET}/root".to_owned()];
    assert!(matches!(
        parse_settings(&arguments),
        Err(InjectorError::EnvironmentVariableMissing { variable, .. })
            if variable == "TD_INJECTOR_TEST_SURELY_UNSET"
    ));
}

#[test]
fn an_unclosed_or_empty_braced_reference_is_rejected() {
    for value in ["env:${PATH/root", "env:${}/root"] {
        let arguments = vec![format!("tdk_database_host={value}")];
        assert!(matches!(
            parse_settings(&arguments),
            Err(InjectorError::InvalidSetting { .. })
        ));
    }
}

#[test]
fn a_literal_prefix_keeps_an_environment_lookalike() {
    let files = injected(&["tdk_database_host=val:env:HOST"]);
    let database = read(&files["dbmanager.database.yaml"]);
    assert_eq!(database["database_server"]["postgres"]["host"], "env:HOST");
}

#[test]
fn an_assignment_needs_an_equals_sign() {
    let arguments = vec!["tdk_database_host".to_owned()];
    assert!(matches!(
        parse_settings(&arguments),
        Err(InjectorError::InvalidSetting { .. })
    ));
}

fn settings_file(content: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().expect("temporary file");
    file.write_all(content.as_bytes()).expect("written");
    file
}

fn from_file(content: &str) -> Result<Vec<Setting>, InjectorError> {
    let file = settings_file(content);
    parse_settings(&[format!("@{}", file.path().display())])
}

#[test]
fn a_compact_settings_file_uses_the_flag_addresses() {
    let settings = from_file(indoc! {"
            tdk_database_host: db.internal
            tabsdata:pods.service.resources.replicas: 2
        "})
    .expect("valid file");
    let files = plan(&configs(), &settings).expect("injected");
    assert!(files["tabsdata.yaml"].contains("      replicas: 2\n"));
    assert!(files["dbmanager.database.yaml"].contains("    host: db.internal\n"));
}

#[test]
fn an_empty_settings_file_sets_nothing() {
    for content in ["# Nothing to set.\n", "{}\n", "[]\n"] {
        let settings = from_file(content).expect("valid file");
        assert!(settings.is_empty());
        assert!(plan(&configs(), &settings).expect("planned").is_empty());
    }
}

#[test]
fn an_expanded_settings_file_nests_keys_per_file() {
    let settings = from_file(
        "\
values:
  tdk_database_host: db.internal
files:
  tabsdata.yaml:
    pods:
      service.resources:
        memory: 8Gi
  apiserver.database:
    storage:
      mounts[id=EXTRA]:
        uri: s3://bucket
",
    )
    .expect("valid file");
    let files = plan(&configs(), &settings).expect("injected");
    assert!(files["tabsdata.yaml"].contains("      memory: 8Gi\n"));
    let apiserver = read(&files["apiserver.database.yaml"]);
    assert_eq!(apiserver["storage"]["mounts"][1]["uri"], "s3://bucket");
    assert_eq!(apiserver["database"]["host"], "db.internal");
}

#[test]
fn a_settings_file_may_list_assignments() {
    let settings = from_file("- tdk_database_host=db.internal\n").expect("valid file");
    assert!(plan(&configs(), &settings).is_ok());
}

#[test]
fn a_settings_file_and_a_flag_cannot_repeat_a_value() {
    let file = settings_file("tdk_database_host: a\n");
    let arguments = vec![
        format!("@{}", file.path().display()),
        "tdk_database_host=a".to_owned(),
    ];
    let settings = parse_settings(&arguments).expect("valid settings");
    assert!(matches!(
        plan(&configs(), &settings),
        Err(InjectorError::DuplicateSetting { .. })
    ));
}

#[test]
fn a_settings_file_cannot_mix_sections_and_compact_entries() {
    assert!(matches!(
        from_file(indoc! {"
            values: {}
            tdk_database_host: a
        "}),
        Err(InjectorError::InvalidSettingsFile { .. })
    ));
}

#[test]
fn a_settings_file_holds_only_final_values() {
    assert!(matches!(
        from_file(indoc! {"
            files:
              tabsdata.yaml:
                pods: {}
        "}),
        Err(InjectorError::InvalidSetting { .. })
    ));
    assert!(matches!(
        from_file("tdk_database_host: [a, b]\n"),
        Err(InjectorError::ValueNotScalar { .. })
    ));
}

#[test]
fn a_flow_collection_value_is_quoted_safely() {
    let configs = BTreeMap::from([(
        "flow.yaml".to_owned(),
        "envs: {A: one, B: two}\n".to_owned(),
    )]);
    let settings = settings(&["flow:envs.A=x, y]"]);
    let files = plan(&configs, &settings).expect("injected");
    let flow = read(&files["flow.yaml"]);
    assert_eq!(flow["envs"]["A"], "x, y]");
    assert_eq!(flow["envs"]["B"], "two");
}

#[test]
fn a_block_scalar_cannot_be_edited_in_place() {
    let configs = BTreeMap::from([(
        "block.yaml".to_owned(),
        indoc! {"
            text: |
              first
              second
        "}
        .to_owned(),
    )]);
    let settings = settings(&["block:text=x"]);
    assert!(matches!(
        plan(&configs, &settings),
        Err(InjectorError::UnsupportedLayout { .. })
    ));
}

#[test]
fn a_key_with_dots_is_quoted_in_the_path() {
    let configs = BTreeMap::from([(
        "labels.yaml".to_owned(),
        indoc! {"
            selectors:
              kubernetes.io/os: linux
        "}
        .to_owned(),
    )]);
    let settings = settings(&["labels:selectors.\"kubernetes.io/os\"=windows"]);
    let files = plan(&configs, &settings).expect("injected");
    assert_eq!(
        files["labels.yaml"],
        indoc! {"
            selectors:
              kubernetes.io/os: windows
        "}
    );
}

#[test]
fn non_ascii_text_keeps_offsets_aligned() {
    let configs = BTreeMap::from([(
        "text.yaml".to_owned(),
        indoc! {"
            name: ñandú # bird
            next: value
        "}
        .to_owned(),
    )]);
    let settings = settings(&["text:next=other"]);
    let files = plan(&configs, &settings).expect("injected");
    assert_eq!(
        files["text.yaml"],
        indoc! {"
        name: ñandú # bird
        next: other
    "}
    );
}

const KMINUS_STORAGE: &str = "\
storage:
  mounts:
  - id: ROOT
    path: /
    uri: ${{system:tdk_storage_url}}data/
    options: {}
  - id: BUNDLES
    path: /bundles
    uri: ${{system:tdk_storage_url}}bundles/
    options: {}
";

const KMINUS_STORAGE_SCHEMA: &str = "\
directives:
  - at: storage.mounts[*].uri
    treat: secret
  - at: storage.mounts[*].options.*
    treat: secret
  - at: storage.mounts[*].options
    treat: open
";

const OPTIONS_ONLY_SCHEMA: &str = "\
directives:
  - at: storage.mounts[*].options.*
    treat: secret
  - at: storage.mounts[*].options
    treat: open
";

fn storage_plan(
    schema: &str,
    arguments: &[&str],
) -> Result<BTreeMap<String, String>, InjectorError> {
    let configs = BTreeMap::from([
        (
            "apiserver.storage.yaml".to_owned(),
            KMINUS_STORAGE.to_owned(),
        ),
        ("apiserver.storage.yamls".to_owned(), schema.to_owned()),
    ]);
    plan(&configs, &settings(arguments))
}

#[test]
fn a_uri_the_schema_declares_secret_can_be_set_over_a_system_placeholder() {
    let files = storage_plan(
        KMINUS_STORAGE_SCHEMA,
        &["apiserver.storage:storage.mounts[id=ROOT].uri=s3://bucket/root/"],
    )
    .expect("injected");
    let storage = read(&files["apiserver.storage.yaml"]);
    assert_eq!(storage["storage"]["mounts"][0]["uri"], "s3://bucket/root/");
    assert_eq!(
        storage["storage"]["mounts"][1]["uri"],
        "${{system:tdk_storage_url}}bundles/"
    );
}

#[test]
fn a_uri_the_schema_does_not_declare_secret_stays_system_controlled() {
    let error = storage_plan(
        OPTIONS_ONLY_SCHEMA,
        &["apiserver.storage:storage.mounts[id=ROOT].uri=s3://bucket/root/"],
    )
    .expect_err("rejected");
    assert!(matches!(error, InjectorError::NonEditableKey { .. }));
}

#[test]
fn a_secret_uri_cannot_be_set_empty() {
    let error = storage_plan(
        KMINUS_STORAGE_SCHEMA,
        &["apiserver.storage:storage.mounts[id=ROOT].uri="],
    )
    .expect_err("rejected");
    assert!(matches!(error, InjectorError::EmptyMandatoryValue { .. }));
}

#[test]
fn keys_can_be_added_to_empty_open_mappings() {
    let files = storage_plan(
        KMINUS_STORAGE_SCHEMA,
        &[
            "apiserver.storage:storage.mounts[id=ROOT].options.aws_region=eu-west-1",
            "apiserver.storage:storage.mounts[id=ROOT].options.aws_access_key_id=an-id",
            "apiserver.storage:storage.mounts[id=BUNDLES].options.aws_region=eu-west-1",
        ],
    )
    .expect("injected");
    let storage = read(&files["apiserver.storage.yaml"]);
    let root = &storage["storage"]["mounts"][0]["options"];
    assert_eq!(root["aws_region"], "eu-west-1");
    assert_eq!(root["aws_access_key_id"], "an-id");
    let bundles = &storage["storage"]["mounts"][1]["options"];
    assert_eq!(bundles["aws_region"], "eu-west-1");
    assert_eq!(bundles.as_mapping().map(serde_yaml::Mapping::len), Some(1));
    assert_eq!(
        storage["storage"]["mounts"][1]["uri"],
        "${{system:tdk_storage_url}}bundles/"
    );
}

#[test]
fn a_key_added_to_an_open_mapping_twice_is_a_duplicate() {
    let error = storage_plan(
        KMINUS_STORAGE_SCHEMA,
        &[
            "apiserver.storage:storage.mounts[id=ROOT].options.aws_region=eu-west-1",
            "apiserver.storage:storage.mounts[id=ROOT].options.aws_region=us-east-1",
        ],
    )
    .expect_err("rejected");
    assert!(matches!(error, InjectorError::DuplicateSetting { .. }));
}

#[test]
fn a_key_cannot_be_added_to_a_mapping_the_schema_does_not_open() {
    let error = storage_plan(
        KMINUS_STORAGE_SCHEMA,
        &["apiserver.storage:storage.mounts[id=ROOT].extra=value"],
    )
    .expect_err("rejected");
    assert!(matches!(error, InjectorError::KeyNotFound { .. }));
}

#[test]
fn a_storage_stub_sets_uris_and_options_together() {
    let stub = indoc! {"
        files:
          apiserver.storage:
            storage.mounts[id=ROOT]:
              uri: s3://bucket/root/
              options.aws_region: eu-west-1
              options.aws_endpoint_url: http://127.0.0.1:9000
            storage.mounts[id=BUNDLES]:
              uri: s3://bucket/bundles/
              options.aws_region: eu-west-1
    "};
    let mut file = tempfile::NamedTempFile::new().expect("a stub file");
    file.write_all(stub.as_bytes()).expect("written");
    let argument = format!("@{}", file.path().display());
    let files = storage_plan(KMINUS_STORAGE_SCHEMA, &[argument.as_str()]).expect("injected");
    let storage = read(&files["apiserver.storage.yaml"]);
    let root = &storage["storage"]["mounts"][0];
    assert_eq!(root["uri"], "s3://bucket/root/");
    assert_eq!(root["options"]["aws_region"], "eu-west-1");
    assert_eq!(root["options"]["aws_endpoint_url"], "http://127.0.0.1:9000");
    assert_eq!(
        storage["storage"]["mounts"][1]["uri"],
        "s3://bucket/bundles/"
    );
}

const TABSDATA_SCHEMA: &str =
    include_str!("../../../../../../../variant/resources/kprofile/configs/tabsdata.schema.yaml");
const TABSDATA_EKS: &str =
    include_str!("../../../../../../../variant/resources/kprofile/configs/tabsdata_eks.yaml");
const TABSDATA_GKE: &str =
    include_str!("../../../../../../../variant/resources/kprofile/configs/tabsdata_gke.yaml");
const TABSDATA_AKS: &str =
    include_str!("../../../../../../../variant/resources/kprofile/configs/tabsdata_aks.yaml");

fn identity_plan(
    tabsdata: &str,
    arguments: &[&str],
) -> Result<BTreeMap<String, String>, InjectorError> {
    let configs = BTreeMap::from([
        ("tabsdata.yaml".to_owned(), tabsdata.to_owned()),
        ("tabsdata.yamls".to_owned(), TABSDATA_SCHEMA.to_owned()),
    ]);
    plan(&configs, &settings(arguments))
}

#[test]
fn an_eks_apiserver_takes_its_iam_role() {
    let files = identity_plan(
        TABSDATA_EKS,
        &[
            "tabsdata:pods.service.apiserver.service_account.annotations.\"eks.amazonaws.com/role-arn\"=arn:aws:iam::123456789012:role/apiserver",
        ],
    )
    .expect("injected");
    let annotations = &read(&files["tabsdata.yaml"])["pods"]["service"]["apiserver"]["service_account"]
        ["annotations"];
    assert_eq!(
        annotations["eks.amazonaws.com/role-arn"],
        "arn:aws:iam::123456789012:role/apiserver"
    );
    assert_eq!(
        annotations.as_mapping().map(serde_yaml::Mapping::len),
        Some(1)
    );
}

#[test]
fn a_gke_apiserver_takes_its_google_service_account_and_worker_logs_bucket() {
    let files = identity_plan(
        TABSDATA_GKE,
        &[
            "tabsdata:pods.service.apiserver.service_account.annotations.\"iam.gke.io/gcp-service-account\"=apiserver@project.iam.gserviceaccount.com",
            "tabsdata:pods.service.apiserver.worker_logs.bucket=tabsdata-logs",
            "tabsdata:pods.service.apiserver.worker_logs.location=europe-west1",
        ],
    )
    .expect("injected");
    let apiserver = &read(&files["tabsdata.yaml"])["pods"]["service"]["apiserver"];
    assert_eq!(
        apiserver["service_account"]["annotations"]["iam.gke.io/gcp-service-account"],
        "apiserver@project.iam.gserviceaccount.com"
    );
    assert_eq!(apiserver["worker_logs"]["bucket"], "tabsdata-logs");
    assert_eq!(apiserver["worker_logs"]["location"], "europe-west1");
}

#[test]
fn an_aks_apiserver_takes_its_workload_identity_and_worker_logs_workspace() {
    let files = identity_plan(
        TABSDATA_AKS,
        &[
            "tabsdata:pods.service.apiserver.service_account.annotations.\"azure.workload.identity/client-id\"=af088ea7-37cd-4fb6-bdb7-cc50392a3384",
            "tabsdata:pods.service.apiserver.worker_logs.workspace=fccdfef1-d7b9-4f1b-86a8-78fc72277906",
        ],
    )
    .expect("injected");
    let apiserver = &read(&files["tabsdata.yaml"])["pods"]["service"]["apiserver"];
    assert_eq!(
        apiserver["service_account"]["annotations"]["azure.workload.identity/client-id"],
        "af088ea7-37cd-4fb6-bdb7-cc50392a3384"
    );
    assert_eq!(
        apiserver["worker_logs"]["workspace"],
        "fccdfef1-d7b9-4f1b-86a8-78fc72277906"
    );
}

#[test]
fn the_tabsdata_schema_opens_only_the_annotations() {
    let error = identity_plan(
        TABSDATA_EKS,
        &["tabsdata:pods.service.apiserver.service_account.name=other"],
    )
    .expect_err("rejected");
    assert!(matches!(error, InjectorError::KeyNotFound { .. }));
}
