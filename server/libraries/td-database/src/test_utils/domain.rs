//
// Copyright 2026 Tabsdata Inc.
//

//! The tdx domain td-database's own internal tests (`components/tests/*`,
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

use crate::config::{PostgresConfig, PostgresConnect};

/// The tdx domain this crate's internals suite runs against, declared in
/// `variant/config/pytest/domains_rs.yaml`.
pub const INTERNALS_DOMAIN: &str = "APISERVER_POSTGRES_INTERNALS";

/// Superuser connect target, for building per-schema admin and serving
/// connectables. Panics when the domain's environment tier is absent: the
/// suite has no database to run against, which is a failure, not a pass.
pub fn admin_base() -> PostgresConnect {
    domain_identity(INTERNALS_DOMAIN).unwrap_or_else(|| {
        panic!(
            "domain '{INTERNALS_DOMAIN}' is not in the environment — provision it with \
             `cargo make domains_create --rust` and load the tdx secrets (the descriptor \
             is encrypted, so `TD_BACKENDS_CRYPTO_KEY` has to be set)"
        )
    })
}

/// Superuser identity from a domain's `<NAME>__*` environment tier, or `None`
/// when the tier is absent or incomplete.
fn domain_identity(domain: &str) -> Option<PostgresConnect> {
    let attribute = |key: &str| {
        std::env::var(format!("{domain}__{key}"))
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    };
    Some(PostgresConnect {
        options: PostgresConfig {
            host: attribute("HOST")?,
            port: attribute("PORT")?.parse().ok()?,
            database: Some(attribute("DB")?),
            ..Default::default()
        },
        username: attribute("SYS_ADMIN_USER")?,
        password: attribute("SYS_ADMIN_PASSWORD")?,
    })
}
