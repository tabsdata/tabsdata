//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

const APISERVER_DATABASE: &str = "\
database:
  apiserver:
    postgres:
      host: 127.0.0.1
      port: 2460
      database: tabsdata
      password: secret
    pool:
      regular:
        min_connections: 5
        max_connections: 50
";

#[test]
fn derived_schema_and_username_are_added() {
    let amended = amend(APISERVER_DATABASE_CONFIG, APISERVER_DATABASE, None).expect("amend");
    assert!(amended.contains("schema: tabsdata_apiserver"));
    assert!(amended.contains("username: tabsdata_apiserver"));
}

#[test]
fn derived_schema_follows_the_configured_database() {
    let config = APISERVER_DATABASE.replace("database: tabsdata", "database: production");
    let amended = amend(APISERVER_DATABASE_CONFIG, &config, None).expect("amend");
    assert!(amended.contains("schema: production_apiserver"));
    assert!(amended.contains("username: production_apiserver"));
}

#[test]
fn derived_username_sits_inside_the_backend() {
    let amended = amend(APISERVER_DATABASE_CONFIG, APISERVER_DATABASE, None).expect("amend");
    assert!(amended.contains("      username: tabsdata_apiserver"));
}

#[test]
fn derived_schema_sits_beside_the_backend() {
    let amended = amend(APISERVER_DATABASE_CONFIG, APISERVER_DATABASE, None).expect("amend");
    assert!(amended.contains("    schema: tabsdata_apiserver"));
    assert!(!amended.contains("      schema: tabsdata_apiserver"));
}

#[test]
fn derived_properties_are_placed_next_to_their_source() {
    let amended = amend(APISERVER_DATABASE_CONFIG, APISERVER_DATABASE, None).expect("amend");
    let username = amended
        .find("username: tabsdata_apiserver")
        .expect("username");
    let password = amended.find("password: secret").expect("password");
    let schema = amended.find("schema: tabsdata_apiserver").expect("schema");
    let pool = amended.find("pool:").expect("pool");
    assert!(username < password);
    assert!(schema < pool);
}

#[test]
fn explicit_schema_is_not_overridden_and_drives_the_username() {
    let config = APISERVER_DATABASE.replace("    pool:", "    schema: authored\n    pool:");
    let amended = amend(APISERVER_DATABASE_CONFIG, &config, None).expect("amend");
    assert!(amended.contains("schema: authored"));
    assert!(!amended.contains("schema: tabsdata_apiserver"));
    assert!(amended.contains("username: authored"));
}

#[test]
fn explicit_username_is_not_overridden() {
    let config = APISERVER_DATABASE.replace(
        "      database: tabsdata",
        "      database: tabsdata\n      username: authored",
    );
    let amended = amend(APISERVER_DATABASE_CONFIG, &config, None).expect("amend");
    assert!(amended.contains("username: authored"));
    assert!(amended.contains("schema: tabsdata_apiserver"));
}

#[test]
fn unlisted_config_is_left_untouched() {
    let config = "service:\n  port: 2457\n";
    let amended = amend("apiserver.service.yaml", config, None).expect("amend");
    assert_eq!(amended, config);
}

#[test]
fn every_component_in_the_section_is_derived() {
    let config = "\
database:
  apiserver:
    postgres:
      database: tabsdata
  aiagent:
    postgres:
      database: tabsdata
";
    let amended = amend(APISERVER_DATABASE_CONFIG, config, None).expect("amend");
    assert!(amended.contains("schema: tabsdata_apiserver"));
    assert!(amended.contains("username: tabsdata_apiserver"));
    assert!(amended.contains("schema: tabsdata_aiagent"));
    assert!(amended.contains("username: tabsdata_aiagent"));
}

#[test]
fn missing_section_is_skipped() {
    let config = "other:\n  apiserver:\n    postgres:\n      database: tabsdata\n";
    let amended = amend(APISERVER_DATABASE_CONFIG, config, None).expect("amend");
    assert!(!amended.contains("schema:"));
    assert!(!amended.contains("username:"));
}

#[test]
fn derived_schema_is_rejected_when_too_long_for_postgres() {
    let database = "d".repeat(60);
    let config = APISERVER_DATABASE.replace("database: tabsdata", &format!("database: {database}"));
    let amended = amend(APISERVER_DATABASE_CONFIG, &config, None).expect("amend");
    assert!(!amended.contains("schema:"));
    assert!(!amended.contains("username:"));
}

#[test]
fn empty_config_is_left_untouched() {
    let amended = amend(APISERVER_DATABASE_CONFIG, "", None).expect("amend");
    assert_eq!(amended, "");
}

#[test]
fn other_properties_are_preserved() {
    let amended = amend(APISERVER_DATABASE_CONFIG, APISERVER_DATABASE, None).expect("amend");
    assert!(amended.contains("host: 127.0.0.1"));
    assert!(amended.contains("port: 2460"));
    assert!(amended.contains("password: secret"));
    assert!(amended.contains("min_connections: 5"));
}

#[test]
fn show_unknown_component() {
    let config = "database:\n  other:\n    postgres:\n      database: tabsdata\n";
    let amended = amend(APISERVER_DATABASE_CONFIG, config, None).expect("amend");
    println!("---OUT---\n{amended}---END---");
}

#[test]
fn the_schema_comes_from_the_server_database_when_the_component_has_none() {
    let config = "database:\n  apiserver:\n    postgres:\n      password: p\n";
    let server = "database_server:\n  postgres:\n    database: tabsdata\n";
    let amended = amend(APISERVER_DATABASE_CONFIG, config, Some(server)).expect("amend");
    assert!(
        amended.contains("schema: tabsdata_apiserver"),
        "got: {amended}"
    );
    assert!(
        amended.contains("username: tabsdata_apiserver"),
        "got: {amended}"
    );
}

#[test]
fn without_a_server_config_the_static_default_is_used() {
    let config = "database:\n  apiserver:\n    postgres:\n      password: p\n";
    let amended = amend(APISERVER_DATABASE_CONFIG, config, None).expect("amend");
    assert!(amended.contains("schema: td_apiserver"), "got: {amended}");
}
