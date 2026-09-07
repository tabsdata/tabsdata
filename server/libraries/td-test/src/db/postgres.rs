//
// Copyright 2026 Tabsdata Inc.
//

//! Postgres provider: the per-test schemas land in a **tdx-provisioned domain's
//! database**. Nothing here starts a server — `cargo make domains_create` does
//! that before the run, with `--force`, so each provisioning drops and recreates
//! the database and no schema outlives it.
//!
//! The domain descriptor carries the owning system's superuser
//! (`SYS_ADMIN_USER` / `SYS_ADMIN_PASSWORD`) alongside the domain's own owner
//! role, and the superuser is what the per-test path needs: it creates and drops
//! schemas in the database and ensures the cluster-global RLS app role, neither
//! of which the domain owner can do.

use crate::domain::{PostgresDomain, resolve_domain};
use td_database::components::SchemaComponent;
use td_database::config::{PostgresConfig, PostgresConnect};
use td_database::test_utils::MultiTestDb;

/// The domain the db tests carve their per-test schemas out of. Declared in
/// `variant/config/pytest/domains_rs.yaml`, which the `domains_create --rust`
/// task provisions with `--force` on every run.
pub const DEFAULT_DOMAIN: &str = "apiserver_postgres";

/// Points the db tests at a different domain — for a developer running against
/// one of their own.
pub const DOMAIN_ENV: &str = "TD_TEST_DB_DOMAIN";

fn domain_name() -> String {
    std::env::var(DOMAIN_ENV)
        .map(|name| name.trim().to_string())
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| DEFAULT_DOMAIN.to_string())
}

/// The superuser connect for `domain`'s own database.
fn admin_connect(domain: &PostgresDomain) -> Result<PostgresConnect, String> {
    Ok(PostgresConnect {
        options: PostgresConfig {
            host: domain.host.clone(),
            port: domain.port()?,
            database: Some(domain.db.clone()),
            ..Default::default()
        },
        username: domain.sys_admin_user.clone(),
        password: domain.sys_admin_password.clone(),
    })
}

/// Provision the given components — each on its own schema of the resolved
/// domain's database — and return a serving pool per component. `None`
/// declines the test: the domain is out of reach and the policy says skip
/// rather than fail. `test_name` only labels the diagnostic.
pub async fn provision(
    test_name: &'static str,
    components: &'static [&'static dyn SchemaComponent],
) -> Option<MultiTestDb> {
    let name = domain_name();
    let domain = resolve_domain::<PostgresDomain>(&name, test_name)?;
    let admin = admin_connect(&domain).unwrap_or_else(|why| {
        panic!("Database not usable, test: {test_name}. domain '{name}': {why}")
    });
    Some(td_database::test_utils::postgres::pools(admin, components).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domain_name_defaults_and_overrides() {
        temp_env::with_var_unset(DOMAIN_ENV, || {
            assert_eq!(domain_name(), DEFAULT_DOMAIN);
        });
        temp_env::with_var(DOMAIN_ENV, Some("  mine  "), || {
            assert_eq!(domain_name(), "mine");
        });
        // A blank override is an unset one, not a nameless domain.
        temp_env::with_var(DOMAIN_ENV, Some("  "), || {
            assert_eq!(domain_name(), DEFAULT_DOMAIN);
        });
    }

    #[test]
    fn test_admin_connect_targets_the_domain_database_as_the_superuser() {
        use crate::domain::descriptor::{DomainInfo, Info};
        use crate::domain::{Attrs, Backend};
        use std::collections::BTreeMap;

        let meta = BTreeMap::from([
            ("SYSTEM_KIND".to_string(), "sql".to_string()),
            ("IMAGE".to_string(), "postgres:17".to_string()),
            ("HOST".to_string(), "localhost".to_string()),
            ("PORT".to_string(), "61553".to_string()),
            (
                "USER".to_string(),
                "d_tucu_0_apiserver_postgres".to_string(),
            ),
            ("PASSWORD".to_string(), "owner-pw".to_string()),
            ("DB".to_string(), "d_tucu_0_apiserver_postgres".to_string()),
            (
                "SCHEMA".to_string(),
                "d_tucu_0_apiserver_postgres".to_string(),
            ),
            ("URI".to_string(), "postgresql+psycopg2://x".to_string()),
            (
                "SQLALCHEMY_URI".to_string(),
                "postgresql+psycopg2://x".to_string(),
            ),
            ("SYS_ADMIN_USER".to_string(), "postgres".to_string()),
            ("SYS_ADMIN_PASSWORD".to_string(), "super-pw".to_string()),
        ]);
        let domain = PostgresDomain::build(&Attrs::of(&Info::Domain(DomainInfo {
            system: "postgres".into(),
            name: "apiserver_postgres".into(),
            uuid: String::new(),
            started: String::new(),
            sys_name: "postgres".into(),
            external_name: "d_tucu_0_apiserver_postgres".into(),
            meta,
            model: None,
            data: None,
            entities: BTreeMap::new(),
            seed: 0,
        })))
        .unwrap();

        let admin = admin_connect(&domain).unwrap();
        assert_eq!(admin.username, "postgres", "schemas need the superuser");
        assert_eq!(admin.password, "super-pw");
        assert_eq!(admin.options.host, "localhost");
        assert_eq!(admin.options.port, 61553);
        assert_eq!(
            admin.options.database.as_deref(),
            Some("d_tucu_0_apiserver_postgres"),
            "the schemas go in the domain's own database, not the maintenance one"
        );
    }
}
