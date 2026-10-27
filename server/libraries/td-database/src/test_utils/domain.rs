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
//! Each test declares it like any other backend —
//! `#[td_test::domain("apiserver_postgres_internals")]` above its test
//! attribute, taking `domain_apiserver_postgres_internals: PostgresDomain` — so
//! td-test resolves it, skips or fails per its policy, and the test hands it
//! down to [`admin_base`] and [`super::postgres::pool`].

pub use td_test::domain::PostgresDomain;

use crate::config::{PostgresConfig, PostgresConnect};

/// Superuser connect target into `domain`'s database, for building per-schema
/// admin and serving connectables.
pub fn admin_base(domain: &PostgresDomain) -> PostgresConnect {
    PostgresConnect {
        options: PostgresConfig {
            host: domain.external_host.clone(),
            port: domain
                .port()
                .unwrap_or_else(|why| panic!("domain '{}': {why}", domain.name)),
            database: Some(domain.database.clone()),
            ..Default::default()
        },
        username: domain.sys_admin_user.clone(),
        password: domain.sys_admin_password.clone(),
    }
}
