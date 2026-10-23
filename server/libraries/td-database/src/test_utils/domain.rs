//
// Copyright 2026 Tabsdata Inc.
//

//! The tdd domain td-database's own internal tests (`components/tests/*`,
//! `pool::connect`, `pool::options`) run against. Like the
//! `#[td_test::test(db)]` macro path, nothing here starts a server: the
//! database is provisioned by `cargo make domains_create --rust` before the
//! run, and each test isolates itself on its own uniquely-named schema (see
//! [`super::postgres`]).
//!
//! A separate domain from the macro path's `apiserver_postgres`, because this
//! suite needs the owning system's superuser for cluster-global work a
//! domain's own owner role cannot do: creating and dropping roles, and setting
//! `BYPASSRLS`.
//!
//! Resolved like td-test's backends: a complete `<NAME>__<KEY>` environment
//! tier (what `cargo make test_rs` exports) wins, so no tdd process is started;
//! otherwise the descriptor is read from tdd with `$_TDD_CRYPTO_KEY`. An
//! incomplete tier is ignored as a whole, never mixed with the descriptor.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use td_tdd::Role;

use crate::config::{PostgresConfig, PostgresConnect};

/// The tdd domain this crate's internals suite runs against, declared in
/// `variant/config/pytest/domains_rs.yaml`.
pub const INTERNALS_DOMAIN: &str = "apiserver_postgres_internals";

const KEYS: [&str; 5] = [
    "EXTERNAL_HOST",
    "PORT",
    "DATABASE",
    "SYS_ADMIN_USER",
    "SYS_ADMIN_PASSWORD",
];

/// Superuser connect target, for building per-schema admin and serving
/// connectables. Panics when the domain cannot be resolved: the suite has no
/// database to run against, which is a failure, not a pass.
pub fn admin_base() -> PostgresConnect {
    static ADMIN: OnceLock<PostgresConnect> = OnceLock::new();
    ADMIN
        .get_or_init(|| {
            let attributes = from_environment(INTERNALS_DOMAIN)
                .map(Ok)
                .unwrap_or_else(|| from_tdd(INTERNALS_DOMAIN))
                .unwrap_or_else(|why| panic!("domain '{INTERNALS_DOMAIN}': {why}"));
            identity(&attributes).unwrap_or_else(|why| panic!("domain '{INTERNALS_DOMAIN}': {why}"))
        })
        .clone()
}

/// The domain's `<NAME>__<KEY>` environment tier, or `None` unless every key
/// is set.
fn from_environment(domain: &str) -> Option<BTreeMap<String, String>> {
    let prefix = domain.to_uppercase();
    KEYS.iter()
        .map(|key| {
            std::env::var(format!("{prefix}__{key}"))
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .map(|value| (key.to_string(), value))
        })
        .collect()
}

fn from_tdd(domain: &str) -> Result<BTreeMap<String, String>, String> {
    td_tdd::descriptor(Role::Domain, domain)
        .map(|descriptor| descriptor.meta)
        .map_err(|why| {
            format!(
                "{why}. Provision it with `cargo make domains_create --rust`, with \
                 `${}` set so tdd can read its descriptor",
                td_tdd::ENV_CRYPTO_KEY
            )
        })
}

fn identity(attributes: &BTreeMap<String, String>) -> Result<PostgresConnect, String> {
    let attribute = |key: &str| {
        attributes
            .get(key)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("the descriptor has no '{key}'"))
    };
    let port = attribute("PORT")?;
    Ok(PostgresConnect {
        options: PostgresConfig {
            host: attribute("EXTERNAL_HOST")?,
            port: port
                .parse()
                .map_err(|_| format!("'PORT' is not a port: '{port}'"))?,
            database: Some(attribute("DATABASE")?),
            ..Default::default()
        },
        username: attribute("SYS_ADMIN_USER")?,
        password: attribute("SYS_ADMIN_PASSWORD")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOMAIN: &str = "a_test_domain";

    fn complete() -> Vec<(String, Option<&'static str>)> {
        [
            ("EXTERNAL_HOST", "db.example"),
            ("PORT", "5433"),
            ("DATABASE", "d_x"),
            ("SYS_ADMIN_USER", "postgres"),
            ("SYS_ADMIN_PASSWORD", "super-pw"),
        ]
        .into_iter()
        .map(|(key, value)| (format!("A_TEST_DOMAIN__{key}"), Some(value)))
        .collect()
    }

    #[test]
    fn test_a_complete_environment_tier_is_the_identity() {
        temp_env::with_vars(complete(), || {
            let admin = identity(&from_environment(DOMAIN).unwrap()).unwrap();
            assert_eq!(admin.options.host, "db.example");
            assert_eq!(admin.options.port, 5433);
            assert_eq!(admin.options.database.as_deref(), Some("d_x"));
            assert_eq!(admin.username, "postgres");
            assert_eq!(admin.password, "super-pw");
        });
    }

    /// An incomplete tier falls through to tdd as a whole, so its values are
    /// never mixed with the descriptor's.
    #[test]
    fn test_an_incomplete_environment_tier_is_ignored() {
        let mut vars = complete();
        vars[4].1 = None;
        temp_env::with_vars(vars, || {
            assert!(from_environment(DOMAIN).is_none());
        });
    }

    #[test]
    fn test_a_descriptor_missing_a_key_names_it() {
        let mut attributes: BTreeMap<String, String> = KEYS
            .iter()
            .map(|key| (key.to_string(), "1".to_string()))
            .collect();
        attributes.remove("DATABASE");
        assert!(identity(&attributes).unwrap_err().contains("'DATABASE'"));
    }
}
