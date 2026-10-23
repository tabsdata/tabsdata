//
// Copyright 2026 Tabsdata Inc.
//

use indoc::indoc;

use super::*;

const APISERVER_DATABASE: &str = indoc! {"
    database:
      apiserver:
        postgres:
          password: apiserver_secret
"};

const DBMANAGER_DATABASE: &str = indoc! {"
    database_server:
      postgres:
        host: rds.example.com
        port: 5433
        database: tabsdata
        username: admin
        password: admin_secret
"};

fn loaded(files: &[&str]) -> TdManagerConfig {
    files
        .iter()
        .fold(config::Config::builder(), |builder, file| {
            builder.add_source(config::File::from_str(file, config::FileFormat::Yaml))
        })
        .build()
        .expect("the files build a configuration")
        .try_deserialize()
        .expect("the configuration deserializes")
}

#[test]
fn the_server_entry_supplies_the_target_of_every_component() {
    let config = loaded(&[APISERVER_DATABASE, DBMANAGER_DATABASE]);
    let components = config.components();
    let postgres = &components
        .get("apiserver")
        .expect("the apiserver component is configured")
        .postgres;
    assert_eq!(postgres.host, "rds.example.com");
    assert_eq!(postgres.port, 5433);
    assert_eq!(postgres.database.as_deref(), Some("tabsdata"));
    assert_eq!(postgres.password.as_deref(), Some("apiserver_secret"));
}

#[test]
fn without_a_server_entry_the_components_keep_their_own_target() {
    let config = loaded(&[APISERVER_DATABASE]);
    let components = config.components();
    let postgres = &components
        .get("apiserver")
        .expect("the apiserver component is configured")
        .postgres;
    assert_eq!(postgres.database, None);
}
