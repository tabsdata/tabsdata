//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use crate::base::constants::{REQUIREMENTS_TD_ASSET, TABSDATA_REQUIREMENTS_LOCK_ASSET};
use crate::runtime::instance::manifest::Manifest;

fn configs(entries: &[(&str, &str)]) -> RenderedConfigmap {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

#[test]
fn bake_schema_secrets_rewrites_matched_leaves_to_tokens() {
    let schema = "directives:\n  - at: options.token\n    treat: secret\n";
    let config = "options:\n  token: baked\n  other: keep\n";
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
    let config = "options:\n  token: baked\n";
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
    let config = "options:\n  token: baked\n";
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

const LITERAL_HOST: &str =
    "database:\n  apiserver:\n    postgres:\n      host: ${{literal:tdk_database_host}}\n";
const SECRET_USER: &str =
    "database_server:\n  postgres:\n    username: ${{secret:postgres_system_user}}\n";

#[test]
fn a_literal_supplied_in_a_later_file_reaches_an_earlier_one() {
    let (literals, _) = seeded(
        "literal_later",
        &[
            ("apiserver.database.yaml", LITERAL_HOST),
            (
                "dbmanager.database.yaml",
                "database_server:\n  postgres:\n    host: ${{literal:tdk_database_host}}\n",
            ),
        ],
        &[
            ("apiserver.database.yaml", LITERAL_HOST),
            (
                "dbmanager.database.yaml",
                "database_server:\n  postgres:\n    host: dimax\n",
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
                "database_server:\n  postgres:\n    host: ${{literal:tdk_database_host}}\n",
            ),
        ],
        &[
            (
                "apiserver.database.yaml",
                "database:\n  apiserver:\n    postgres:\n      host: earlyhost\n",
            ),
            (
                "dbmanager.database.yaml",
                "database_server:\n  postgres:\n    host: ${{literal:tdk_database_host}}\n",
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
                "database:\n  apiserver:\n    postgres:\n      username: ${{secret:postgres_system_user}}\n",
            ),
            ("dbmanager.database.yaml", SECRET_USER),
        ],
        &[
            (
                "apiserver.database.yaml",
                "database:\n  apiserver:\n    postgres:\n      username: ${{secret:postgres_system_user}}\n",
            ),
            (
                "dbmanager.database.yaml",
                "database_server:\n  postgres:\n    username: dbadmin\n",
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
                "database_server:\n  postgres:\n    host: ${{literal:tdk_database_host}}\n",
            ),
        ],
        &[
            (
                "apiserver.database.yaml",
                "database:\n  apiserver:\n    postgres:\n      host: first\n",
            ),
            (
                "dbmanager.database.yaml",
                "database_server:\n  postgres:\n    host: second\n",
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
            "database:\n  apiserver:\n    postgres:\n      host: \"\"\n",
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
            "database:\n  apiserver:\n    postgres:\n      port: ${{literal:tdk_database_port}}\n",
        )],
        &[(
            "apiserver.database.yaml",
            "database:\n  apiserver:\n    postgres:\n      port: 5432\n",
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
            "jwt:\n  secret: ${{secretAuto:jwt_signing_key}}\n",
        )],
        &[("apiserver.service.yaml", "jwt:\n  secret: leaked\n")],
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
            "database:\n  apiserver:\n    postgres:\n      host: ${{system:tdk_database_host}}\n",
        )],
        &[(
            "apiserver.database.yaml",
            "database:\n  apiserver:\n    postgres:\n      host: 127.0.0.1\n",
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
            "addresses:\n  - ${{literal:tdk_bind_address}}\n",
        )],
        &[("apiserver.service.yaml", "addresses:\n  - 0.0.0.0:2457\n")],
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
            "database_server:\n  postgres:\n    host: ${{literal:tdk_database_host}}\n    username: ${{secret:postgres_system_user}}\n",
        )],
        &[(
            "dbmanager.database.yaml",
            "database_server:\n  postgres:\n    host: dimax\n    username: dbadmin\n",
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
                "database_server:\n  postgres:\n    host: ${{literal:tdk_database_host}}\n",
            ),
        ],
        &[(
            "dbmanager.database.yaml",
            "database_server:\n  postgres:\n    host: dimax\n",
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
            "directives:\n  - at: ${{literal:tdk_ignored}}\n",
        )],
        &[(
            "apiserver.storage.yamls",
            "directives:\n  - at: something\n",
        )],
    );
    assert!(literals.is_empty());
}
