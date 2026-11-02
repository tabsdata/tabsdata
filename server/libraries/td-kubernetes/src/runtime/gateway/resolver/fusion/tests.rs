//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use indoc::indoc;

use super::*;
use crate::base::constants::{REQUIREMENTS_TD_ASSET, TABSDATA_REQUIREMENTS_LOCK_ASSET};
use crate::runtime::gateway::resolver::fusion::resolve::ResolveOutcome;
use crate::runtime::instance::manifest::Manifest;

fn configs(entries: &[(&str, &str)]) -> RenderedConfigmap {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

#[test]
fn bake_schema_secrets_rewrites_matched_leaves_to_tokens() {
    let schema = indoc! {"
        directives:
          - at: options.token
            treat: secret
    "};
    let config = indoc! {"
        options:
          token: baked
          other: keep
    "};
    let configs = configs(&[("apiserver.yamls", schema)]);
    let baked = bake_schema_secrets(
        "apiserver.yaml",
        config,
        schema_of("apiserver.yaml", &configs)
            .expect("schema")
            .as_ref(),
    )
    .expect("bake");
    assert!(baked.contains("token: ${{secret}}"));
    assert!(baked.contains("other: keep"));
}

#[test]
fn bake_schema_secrets_is_noop_without_schema_file() {
    let config = indoc! {"
        options:
          token: baked
    "};
    let configs = configs(&[]);
    let baked = bake_schema_secrets(
        "apiserver.yaml",
        config,
        schema_of("apiserver.yaml", &configs)
            .expect("schema")
            .as_ref(),
    )
    .expect("bake");
    assert_eq!(baked, config);
}

#[test]
fn bake_schema_secrets_is_noop_for_empty_schema() {
    let schema = "other: 1\n";
    let config = indoc! {"
        options:
          token: baked
    "};
    let configs = configs(&[("apiserver.yamls", schema)]);
    let baked = bake_schema_secrets(
        "apiserver.yaml",
        config,
        schema_of("apiserver.yaml", &configs)
            .expect("schema")
            .as_ref(),
    )
    .expect("bake");
    assert_eq!(baked, config);
}

#[test]
fn bake_schema_secrets_ignores_non_yaml_stem() {
    let config = "plain content";
    let configs = configs(&[]);
    let baked = bake_schema_secrets(
        "notes.txt",
        config,
        schema_of("notes.txt", &configs).expect("schema").as_ref(),
    )
    .expect("bake");
    assert_eq!(baked, config);
}

fn write_family(root: &Path, family: &str, files: &[(&str, &str)]) {
    let folder = root.join(family);
    fs::create_dir_all(&folder).expect("create family folder");
    for (name, content) in files {
        fs::write(folder.join(name), content).expect("write family file");
    }
}

#[test]
fn compare_family_accepts_exact_match() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "configs", &[("a.yaml", "x"), ("b.yaml", "y")]);
    let canonical = configs(&[("a.yaml", "x"), ("b.yaml", "y")]);
    let result = compare_family(temp.path(), "configs", &canonical, false, &[], &[]);
    assert!(result.is_ok());
}

#[test]
fn compare_family_rejects_extra_file() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(
        temp.path(),
        "configs",
        &[("a.yaml", "x"), ("extra.yaml", "z")],
    );
    let canonical = configs(&[("a.yaml", "x")]);
    let error = compare_family(temp.path(), "configs", &canonical, false, &[], &[])
        .expect_err("extra file must be rejected");
    assert!(matches!(
        error,
        ResolverError::InitializationExtraFile { .. }
    ));
}

#[test]
fn compare_family_rejects_missing_file() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "configs", &[("a.yaml", "x")]);
    let canonical = configs(&[("a.yaml", "x"), ("b.yaml", "y")]);
    let error = compare_family(temp.path(), "configs", &canonical, false, &[], &[])
        .expect_err("missing file must be rejected");
    assert!(matches!(
        error,
        ResolverError::InitializationMissingFile { .. }
    ));
}

#[test]
fn compare_family_strict_rejects_modified_pod() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "pods", &[("worker.yaml", "changed")]);
    let canonical = configs(&[("worker.yaml", "canonical")]);
    let error = compare_family(temp.path(), "pods", &canonical, true, &[], &[])
        .expect_err("modified pod must be rejected");
    assert!(matches!(
        error,
        ResolverError::InitializationTemplateModified { .. }
    ));
}

#[test]
fn compare_family_strict_accepts_unmodified_pod() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "pods", &[("worker.yaml", "canonical")]);
    let canonical = configs(&[("worker.yaml", "canonical")]);
    let result = compare_family(temp.path(), "pods", &canonical, true, &[], &[]);
    assert!(result.is_ok());
}

#[test]
fn compare_family_strict_accepts_modified_customizable_asset() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(
        temp.path(),
        "assets",
        &[(REQUIREMENTS_TD_ASSET, "mimesis==19.1.0")],
    );
    let canonical = configs(&[(REQUIREMENTS_TD_ASSET, "")]);
    let result = compare_family(
        temp.path(),
        "assets",
        &canonical,
        true,
        CUSTOMIZABLE_ASSETS,
        &[],
    );
    assert!(result.is_ok());
}

#[test]
fn compare_family_strict_rejects_modified_asset_outside_the_list() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "assets", &[("venvloader.sh", "changed")]);
    let canonical = configs(&[("venvloader.sh", "canonical")]);
    let error = compare_family(
        temp.path(),
        "assets",
        &canonical,
        true,
        CUSTOMIZABLE_ASSETS,
        &[],
    )
    .expect_err("a non customizable asset must be rejected");
    assert!(matches!(
        error,
        ResolverError::InitializationTemplateModified { .. }
    ));
}

#[test]
fn read_flat_family_rejects_nested_subfolder() {
    let temp = tempfile::tempdir().expect("tempdir");
    let folder = temp.path().join("configs");
    fs::create_dir_all(folder.join("nested")).expect("create nested");
    let error =
        read_flat_family(&folder, "configs").expect_err("nested subfolder must be rejected");
    assert!(matches!(error, ResolverError::InitializationNotFlat { .. }));
}

#[test]
fn read_flat_family_lists_files() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "configs", &[("a.yaml", "x"), ("b.yaml", "y")]);
    let files = read_flat_family(&temp.path().join("configs"), "configs").expect("read");
    assert!(files.contains("a.yaml"));
    assert!(files.contains("b.yaml"));
    assert_eq!(files.len(), 2);
}

#[test]
fn seed_vault_takes_existing_generated_values_by_category() {
    let template = "\
database_server:
  username: ${{secretAuto:postgres_system_user}}
  password: ${{secretAuto:postgres_system_password}}
  host: ${{literal:host}}
";
    let configs = configs(&[("dbmanager.database.yaml", template)]);
    let server_secrets: HashMap<String, HashMap<String, String>> = HashMap::from([(
        "dbmanager.database.yaml".to_owned(),
        HashMap::from([
            (
                "DATABASE_SERVER__PASSWORD".to_owned(),
                "stored-password".to_owned(),
            ),
            ("DATABASE_SERVER__USERNAME".to_owned(), String::new()),
        ]),
    )]);
    let vault: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    seed_vault_from_server_secrets(&vault, &configs, &server_secrets);

    let vault = vault.borrow();
    assert_eq!(
        vault.get("postgres_system_password").map(String::as_str),
        Some("stored-password")
    );
    assert!(!vault.contains_key("postgres_system_user"));
}

#[test]
fn seed_vault_keeps_first_seen_value_per_category() {
    let template = "password: ${{secretAuto:postgres_system_password}}\n";
    let configs = configs(&[
        ("a.yaml", template),
        ("b.yaml", template),
        ("c.yamls", "directives: []\n"),
    ]);
    let server_secrets: HashMap<String, HashMap<String, String>> = HashMap::from([
        (
            "a.yaml".to_owned(),
            HashMap::from([("PASSWORD".to_owned(), "value-a".to_owned())]),
        ),
        (
            "b.yaml".to_owned(),
            HashMap::from([("PASSWORD".to_owned(), "value-b".to_owned())]),
        ),
    ]);
    let vault: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    seed_vault_from_server_secrets(&vault, &configs, &server_secrets);

    assert_eq!(
        vault
            .borrow()
            .get("postgres_system_password")
            .map(String::as_str),
        Some("value-a")
    );
}

#[test]
fn compare_family_accepts_a_foreign_asset_absent_from_the_kprofile() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(
        temp.path(),
        "assets",
        &[
            ("venvloader.sh", "canonical"),
            (TABSDATA_REQUIREMENTS_LOCK_ASSET, "polars==1.0.0"),
        ],
    );
    let canonical = configs(&[("venvloader.sh", "canonical")]);
    let result = compare_family(
        temp.path(),
        "assets",
        &canonical,
        true,
        CUSTOMIZABLE_ASSETS,
        FOREIGN_ASSETS,
    );
    assert!(result.is_ok());
}

#[test]
fn compare_family_rejects_a_missing_foreign_asset() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "assets", &[("venvloader.sh", "canonical")]);
    let canonical = configs(&[("venvloader.sh", "canonical")]);
    let error = compare_family(
        temp.path(),
        "assets",
        &canonical,
        true,
        CUSTOMIZABLE_ASSETS,
        FOREIGN_ASSETS,
    )
    .expect_err("a missing foreign asset must be rejected");
    assert!(matches!(
        error,
        ResolverError::InitializationMissingFile { .. }
    ));
}

#[test]
fn compare_family_still_rejects_an_unknown_extra_asset() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(
        temp.path(),
        "assets",
        &[
            ("venvloader.sh", "canonical"),
            (TABSDATA_REQUIREMENTS_LOCK_ASSET, ""),
            ("stranger.txt", "unexpected"),
        ],
    );
    let canonical = configs(&[("venvloader.sh", "canonical")]);
    let error = compare_family(
        temp.path(),
        "assets",
        &canonical,
        true,
        CUSTOMIZABLE_ASSETS,
        FOREIGN_ASSETS,
    )
    .expect_err("an unknown extra asset must be rejected");
    assert!(matches!(
        error,
        ResolverError::InitializationExtraFile { .. }
    ));
}

#[test]
fn stage_assets_files_stages_the_foreign_asset_absent_from_the_kprofile() {
    let temp = tempfile::tempdir().expect("tempdir");
    let init = temp.path().join("init");
    let stage = temp.path().join("stage");
    write_family(
        &init,
        "assets",
        &[
            ("venvloader.sh", "canonical"),
            (TABSDATA_REQUIREMENTS_LOCK_ASSET, "polars==1.0.0"),
        ],
    );
    let profile = Profile {
        pods: configs(&[]),
        assets: configs(&[("venvloader.sh", "canonical")]),
        configs: configs(&[]),
        manifest: Manifest::initial_install("0.0.0", chrono::Utc::now(), Vec::new()),
    };
    stage_assets_files(&profile, &init, &stage).expect("staging failed");

    let staged = stage.join("assets").join(TABSDATA_REQUIREMENTS_LOCK_ASSET);
    assert!(staged.is_file(), "the foreign asset was not staged");
    assert_eq!(
        std::fs::read_to_string(&staged).expect("read"),
        "polars==1.0.0"
    );
    assert!(stage.join("assets").join("venvloader.sh").is_file());
}

fn seeded(
    name: &str,
    templates: &[(&str, &str)],
    inputs: &[(&str, &str)],
) -> (HashMap<String, LiteralOrigin>, HashMap<String, String>) {
    let folder = std::env::temp_dir().join(format!("tdk_seed_{name}"));
    let _ = std::fs::remove_dir_all(&folder);
    std::fs::create_dir_all(&folder).expect("folder");
    for (file, content) in inputs {
        std::fs::write(folder.join(file), content).expect("write input");
    }
    let literals: RefCell<HashMap<String, LiteralOrigin>> = RefCell::new(HashMap::new());
    let vault: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    seed_literals_from_input(&literals, &vault, &configs(templates), &folder);
    let _ = std::fs::remove_dir_all(&folder);
    (literals.into_inner(), vault.into_inner())
}

const LITERAL_HOST: &str = indoc! {"
        database:
          apiserver:
            postgres:
              host: ${{literal:tdk_database_host}}
    "};
const SECRET_USER: &str = indoc! {"
        database_server:
          postgres:
            username: ${{secret:postgres_system_user}}
    "};

#[test]
fn a_literal_supplied_in_a_later_file_reaches_an_earlier_one() {
    let (literals, _) = seeded(
        "literal_later",
        &[
            ("apiserver.database.yaml", LITERAL_HOST),
            (
                "dbmanager.database.yaml",
                indoc! {"
                    database_server:
                      postgres:
                        host: ${{literal:tdk_database_host}}
                "},
            ),
        ],
        &[
            ("apiserver.database.yaml", LITERAL_HOST),
            (
                "dbmanager.database.yaml",
                indoc! {"
                    database_server:
                      postgres:
                        host: dimax
                "},
            ),
        ],
    );
    assert_eq!(
        literals.get("tdk_database_host").map(|o| o.value.as_str()),
        Some("dimax")
    );
}

#[test]
fn a_literal_supplied_in_an_earlier_file_reaches_a_later_one() {
    let (literals, _) = seeded(
        "literal_earlier",
        &[
            ("apiserver.database.yaml", LITERAL_HOST),
            (
                "dbmanager.database.yaml",
                indoc! {"
                    database_server:
                      postgres:
                        host: ${{literal:tdk_database_host}}
                "},
            ),
        ],
        &[
            (
                "apiserver.database.yaml",
                indoc! {"
                    database:
                      apiserver:
                        postgres:
                          host: earlyhost
                "},
            ),
            (
                "dbmanager.database.yaml",
                indoc! {"
                    database_server:
                      postgres:
                        host: ${{literal:tdk_database_host}}
                "},
            ),
        ],
    );
    assert_eq!(
        literals.get("tdk_database_host").map(|o| o.value.as_str()),
        Some("earlyhost")
    );
}

#[test]
fn a_secret_supplied_in_a_later_file_reaches_an_earlier_one() {
    let (_, vault) = seeded(
        "secret_later",
        &[
            (
                "apiserver.database.yaml",
                indoc! {"
                    database:
                      apiserver:
                        postgres:
                          username: ${{secret:postgres_system_user}}
                "},
            ),
            ("dbmanager.database.yaml", SECRET_USER),
        ],
        &[
            (
                "apiserver.database.yaml",
                indoc! {"
                    database:
                      apiserver:
                        postgres:
                          username: ${{secret:postgres_system_user}}
                "},
            ),
            (
                "dbmanager.database.yaml",
                indoc! {"
                    database_server:
                      postgres:
                        username: dbadmin
                "},
            ),
        ],
    );
    assert_eq!(
        vault.get("postgres_system_user").map(String::as_str),
        Some("dbadmin")
    );
}

#[test]
fn the_first_file_in_order_wins_when_two_supply_the_same_category() {
    let (literals, _) = seeded(
        "literal_conflict",
        &[
            ("apiserver.database.yaml", LITERAL_HOST),
            (
                "dbmanager.database.yaml",
                indoc! {"
                    database_server:
                      postgres:
                        host: ${{literal:tdk_database_host}}
                "},
            ),
        ],
        &[
            (
                "apiserver.database.yaml",
                indoc! {"
                    database:
                      apiserver:
                        postgres:
                          host: first
                "},
            ),
            (
                "dbmanager.database.yaml",
                indoc! {"
                    database_server:
                      postgres:
                        host: second
                "},
            ),
        ],
    );
    assert_eq!(
        literals.get("tdk_database_host").map(|o| o.value.as_str()),
        Some("first")
    );
}

#[test]
fn an_unsupplied_token_is_not_seeded() {
    let (literals, vault) = seeded(
        "nothing_supplied",
        &[
            ("apiserver.database.yaml", LITERAL_HOST),
            ("dbmanager.database.yaml", SECRET_USER),
        ],
        &[
            ("apiserver.database.yaml", LITERAL_HOST),
            ("dbmanager.database.yaml", SECRET_USER),
        ],
    );
    assert!(literals.is_empty());
    assert!(vault.is_empty());
}

#[test]
fn an_empty_value_is_not_seeded() {
    let (literals, _) = seeded(
        "empty_value",
        &[("apiserver.database.yaml", LITERAL_HOST)],
        &[(
            "apiserver.database.yaml",
            indoc! {r#"
                database:
                  apiserver:
                    postgres:
                      host: ""
            "#},
        )],
    );
    assert!(literals.is_empty());
}

#[test]
fn non_string_scalars_are_seeded_as_text() {
    let (literals, _) = seeded(
        "numeric_value",
        &[(
            "apiserver.database.yaml",
            indoc! {"
                database:
                  apiserver:
                    postgres:
                      port: ${{literal:tdk_database_port}}
            "},
        )],
        &[(
            "apiserver.database.yaml",
            indoc! {"
                database:
                  apiserver:
                    postgres:
                      port: 5432
            "},
        )],
    );
    assert_eq!(
        literals.get("tdk_database_port").map(|o| o.value.as_str()),
        Some("5432")
    );
}

#[test]
fn generated_secrets_are_not_seeded_from_input() {
    let (_, vault) = seeded(
        "auto_secret",
        &[(
            "apiserver.service.yaml",
            indoc! {"
                jwt:
                  secret: ${{secretAuto:jwt_signing_key}}
            "},
        )],
        &[(
            "apiserver.service.yaml",
            indoc! {"
            jwt:
              secret: leaked
        "},
        )],
    );
    assert!(
        vault.is_empty(),
        "a generated secret owns its value; the input must not seed it"
    );
}

#[test]
fn system_placeholders_are_not_seeded() {
    let (literals, vault) = seeded(
        "system_placeholder",
        &[(
            "apiserver.database.yaml",
            indoc! {"
                database:
                  apiserver:
                    postgres:
                      host: ${{system:tdk_database_host}}
            "},
        )],
        &[(
            "apiserver.database.yaml",
            indoc! {"
                database:
                  apiserver:
                    postgres:
                      host: 127.0.0.1
            "},
        )],
    );
    assert!(literals.is_empty());
    assert!(vault.is_empty());
}

#[test]
fn a_value_inside_a_sequence_is_seeded() {
    let (literals, _) = seeded(
        "sequence_value",
        &[(
            "apiserver.service.yaml",
            indoc! {"
                addresses:
                  - ${{literal:tdk_bind_address}}
            "},
        )],
        &[(
            "apiserver.service.yaml",
            indoc! {"
            addresses:
              - 0.0.0.0:2457
        "},
        )],
    );
    assert_eq!(
        literals.get("tdk_bind_address").map(|o| o.value.as_str()),
        Some("0.0.0.0:2457")
    );
}

#[test]
fn literals_and_secrets_are_seeded_into_their_own_stores() {
    let (literals, vault) = seeded(
        "both_kinds",
        &[(
            "dbmanager.database.yaml",
            indoc! {"
                database_server:
                  postgres:
                    host: ${{literal:tdk_database_host}}
                    username: ${{secret:postgres_system_user}}
            "},
        )],
        &[(
            "dbmanager.database.yaml",
            indoc! {"
                database_server:
                  postgres:
                    host: dimax
                    username: dbadmin
            "},
        )],
    );
    assert_eq!(
        literals.get("tdk_database_host").map(|o| o.value.as_str()),
        Some("dimax")
    );
    assert_eq!(
        vault.get("postgres_system_user").map(String::as_str),
        Some("dbadmin")
    );
    assert!(!vault.contains_key("tdk_database_host"));
    assert!(!literals.contains_key("postgres_system_user"));
}

#[test]
fn a_missing_input_file_is_skipped_without_failing() {
    let (literals, _) = seeded(
        "missing_input",
        &[
            ("apiserver.database.yaml", LITERAL_HOST),
            (
                "dbmanager.database.yaml",
                indoc! {"
                    database_server:
                      postgres:
                        host: ${{literal:tdk_database_host}}
                "},
            ),
        ],
        &[(
            "dbmanager.database.yaml",
            indoc! {"
                database_server:
                  postgres:
                    host: dimax
            "},
        )],
    );
    assert_eq!(
        literals.get("tdk_database_host").map(|o| o.value.as_str()),
        Some("dimax")
    );
}

#[test]
fn schema_files_are_not_seeded() {
    let (literals, _) = seeded(
        "schema_skipped",
        &[(
            "apiserver.storage.yamls",
            indoc! {"
                directives:
                  - at: ${{literal:tdk_ignored}}
            "},
        )],
        &[(
            "apiserver.storage.yamls",
            indoc! {"
                directives:
                  - at: something
            "},
        )],
    );
    assert!(literals.is_empty());
}

const OPEN_STORAGE_TEMPLATE: &str = indoc! {"
    storage:
      mounts:
      - id: ROOT
        path: /
        uri: file:///data/
        options: {}
"};

const OPEN_STORAGE_SCHEMA: &str = indoc! {"
    directives:
      - at: storage.mounts[*].options.*
        treat: secret
      - at: storage.mounts[*].options
        treat: open
"};

#[test]
fn config_put_keeps_added_options_and_drops_removed_ones() {
    let edited = indoc! {"
        storage:
          mounts:
          - id: ROOT
            path: /
            uri: file:///data/
            options:
              aws_access_key_id: ${{secret}}
              aws_endpoint_url: http://localhost:9000
    "};
    let input = tempfile::tempdir().expect("an edit folder");
    write_family(
        input.path(),
        KPROFILE_CONFIGS_FAMILY.folder,
        &[
            ("apiserver.storage.yaml", edited),
            ("apiserver.storage.yamls", OPEN_STORAGE_SCHEMA),
        ],
    );
    let profile = Profile {
        pods: RenderedConfigmap::new(),
        assets: RenderedConfigmap::new(),
        configs: configs(&[
            ("apiserver.storage.yaml", OPEN_STORAGE_TEMPLATE),
            ("apiserver.storage.yamls", OPEN_STORAGE_SCHEMA),
        ]),
        manifest: Manifest::initial_install("2.3.0", chrono::Utc::now(), Vec::new()),
    };
    let server_secrets = HashMap::from([(
        "apiserver.storage.yaml".to_owned(),
        HashMap::from([
            (
                "STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ACCESS_KEY_ID".to_owned(),
                "an-id".to_owned(),
            ),
            (
                "STORAGE__MOUNTS__ROOT__OPTIONS__AWS_REGION".to_owned(),
                "eu-west-1".to_owned(),
            ),
        ]),
    )]);

    let merged = merge(
        &profile,
        &SystemValues::default(),
        input.path(),
        &server_secrets,
    )
    .expect("merges");

    assert_eq!(merged.len(), 1);
    let secrets: BTreeMap<&str, &str> = merged[0]
        .secrets
        .iter()
        .map(|secret| (secret.key.as_str(), secret.value.as_str()))
        .collect();
    assert_eq!(
        secrets,
        BTreeMap::from([
            ("STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ACCESS_KEY_ID", "an-id"),
            (
                "STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ENDPOINT_URL",
                "http://localhost:9000"
            ),
        ])
    );
    let rendered: Value = serde_yaml::from_str(&merged[0].rendered).expect("yaml");
    let options = &rendered["storage"]["mounts"][0]["options"];
    assert_eq!(
        options["aws_access_key_id"],
        Value::String("${{secret}}".to_owned())
    );
    assert_eq!(
        options["aws_endpoint_url"],
        Value::String("${{secret}}".to_owned())
    );
    assert!(options.get("aws_region").is_none());
}

fn shipped(relative: &str) -> String {
    let path = std::path::PathBuf::from(tm_workspace::workspace_root!()).join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

const KMINUS_STORAGE_FILE: &str = "apiserver.storage.yaml";

fn kminus_storage_fused(arguments: &[&str]) -> (Value, BTreeMap<String, String>) {
    let configs = configs(&[
        (
            KMINUS_STORAGE_FILE,
            &shipped("variant/resources/kprofile/configs/apiserver.storage_kminus.yaml"),
        ),
        (
            "apiserver.storage.yamls",
            &shipped("variant/resources/kprofile/configs/apiserver.storage.schema_kminus.yaml"),
        ),
    ]);
    let settings: Vec<String> = arguments.iter().map(|text| (*text).to_owned()).collect();
    let settings = crate::client::tdkserver::injector::parse_settings(&settings).expect("settings");
    let injected = crate::client::tdkserver::injector::plan(&configs, &settings).expect("injects");
    let template = &configs[KMINUS_STORAGE_FILE];
    let input = injected
        .get(KMINUS_STORAGE_FILE)
        .cloned()
        .unwrap_or_else(|| template.clone());
    let baked = bake_config(KMINUS_STORAGE_FILE, template, &configs).expect("bakes");
    let schema = schema_of(KMINUS_STORAGE_FILE, &configs)
        .expect("parses")
        .expect("kminus has a storage schema");
    let system = SystemValues {
        storage_url: Some("file:///home/td/storage/".to_owned()),
        ..SystemValues::default()
    };
    let outcome = resolve_config_file(
        KMINUS_STORAGE_FILE,
        &baked,
        &input,
        &system,
        &HashMap::new(),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        Some(&schema),
    )
    .expect("fuses");
    let rendered: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    let secrets = outcome
        .secrets
        .into_iter()
        .map(|secret| (secret.key, secret.value))
        .collect();
    (rendered, secrets)
}

#[test]
fn kminus_storage_defaults_to_local_files_held_as_secrets() {
    let (rendered, secrets) = kminus_storage_fused(&[]);
    assert_eq!(
        secrets,
        BTreeMap::from([
            (
                "STORAGE__MOUNTS__BUNDLES__URI".to_owned(),
                "file:///home/td/storage/bundles/".to_owned()
            ),
            (
                "STORAGE__MOUNTS__IMAGES__URI".to_owned(),
                "file:///home/td/storage/images/".to_owned()
            ),
            (
                "STORAGE__MOUNTS__ROOT__URI".to_owned(),
                "file:///home/td/storage/data/".to_owned()
            ),
        ])
    );
    for mount in rendered["storage"]["mounts"].as_sequence().expect("mounts") {
        assert_eq!(mount["uri"], Value::String("${{secret}}".to_owned()));
        assert_eq!(mount["options"], Value::Mapping(serde_yaml::Mapping::new()));
    }
}

#[test]
fn kminus_storage_goes_cloud_through_a_stub() {
    let (rendered, secrets) = kminus_storage_fused(&[
        "apiserver.storage:storage.mounts[id=ROOT].uri=s3://bucket/root/",
        "apiserver.storage:storage.mounts[id=ROOT].options.aws_region=eu-west-1",
        "apiserver.storage:storage.mounts[id=ROOT].options.aws_endpoint_url=http://127.0.0.1:9000",
        "apiserver.storage:storage.mounts[id=BUNDLES].uri=s3://bucket/bundles/",
    ]);
    assert_eq!(secrets["STORAGE__MOUNTS__ROOT__URI"], "s3://bucket/root/");
    assert_eq!(
        secrets["STORAGE__MOUNTS__ROOT__OPTIONS__AWS_REGION"],
        "eu-west-1"
    );
    assert_eq!(
        secrets["STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ENDPOINT_URL"],
        "http://127.0.0.1:9000"
    );
    assert_eq!(
        secrets["STORAGE__MOUNTS__BUNDLES__URI"],
        "s3://bucket/bundles/"
    );
    assert_eq!(
        secrets["STORAGE__MOUNTS__IMAGES__URI"],
        "file:///home/td/storage/images/"
    );
    assert_eq!(secrets.len(), 5);
    let images = &rendered["storage"]["mounts"][2];
    assert_eq!(
        images["options"],
        Value::Mapping(serde_yaml::Mapping::new())
    );
}

fn resolved(
    template: &str,
    init: &str,
    schema: Option<&str>,
) -> Result<ResolveOutcome, ResolverError> {
    let schema = schema.map(|text| Schema::parse("x.yamls", text).expect("schema"));
    let template = bake_schema_secrets("x.yaml", template, schema.as_ref()).expect("bakes");
    resolve_config_file(
        "x.yaml",
        &template,
        init,
        &SystemValues::default(),
        &HashMap::new(),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        &RefCell::new(HashMap::new()),
        schema.as_ref(),
    )
}

#[test]
fn a_key_the_template_lacks_at_the_root_is_refused() {
    let error = resolved("kept: 1\n", "kept: 1\nadded: 2\n", None).expect_err("refused");
    assert!(
        matches!(&error, ResolverError::UnknownConfigKeys { path, keys }
            if path == "x.yaml" && keys == "'added'"),
        "{error}"
    );
}

#[test]
fn a_key_the_template_lacks_in_a_nested_mapping_is_refused() {
    let template = indoc! {"
        service:
          mode: plain
    "};
    let init = indoc! {"
        service:
          mode: remote
          endpoint: e
          retries: 3
    "};
    let error = resolved(template, init, None).expect_err("refused");
    assert!(
        matches!(&error, ResolverError::UnknownConfigKeys { path, keys }
            if path == "x.yaml:service" && keys == "'endpoint', 'retries'"),
        "{error}"
    );
}

#[test]
fn an_open_mapping_keeps_the_keys_the_template_lacks() {
    let schema = indoc! {"
        directives:
          - at: options
            treat: open
    "};
    let outcome = resolved(
        "options: {}\n",
        "options:\n  batch: 4\n  ratio: 0.4\n",
        Some(schema),
    )
    .expect("resolves");
    let rendered: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    assert_eq!(rendered["options"]["batch"], Value::from(4));
    assert_eq!(rendered["options"]["ratio"], Value::from(0.4));
    assert!(outcome.secrets.is_empty());
}

#[test]
fn a_secret_leaf_added_under_an_open_mapping_becomes_a_secret() {
    let schema = indoc! {"
        directives:
          - at: service.api_key
            treat: secret
          - at: service
            treat: open
    "};
    let outcome = resolved(
        "service:\n  mode: plain\n",
        "service:\n  mode: remote\n  endpoint: e\n  api_key: k-1\n",
        Some(schema),
    )
    .expect("resolves");
    let rendered: Value = serde_yaml::from_str(&outcome.rendered).expect("yaml");
    assert_eq!(rendered["service"]["mode"], Value::from("remote"));
    assert_eq!(rendered["service"]["endpoint"], Value::from("e"));
    assert_eq!(
        rendered["service"]["api_key"],
        Value::String("${{secret}}".to_owned())
    );
    let secrets: Vec<(String, String)> = outcome
        .secrets
        .into_iter()
        .map(|secret| (secret.key, secret.value))
        .collect();
    assert_eq!(
        secrets,
        vec![("SERVICE__API_KEY".to_owned(), "k-1".to_owned())]
    );
}
