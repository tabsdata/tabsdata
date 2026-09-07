//
// Copyright 2026 Tabsdata Inc.
//

//! Mapping a resolved config onto sqlx's native connect options, field by
//! field — pure translation, no pools and no I/O.

use crate::config::PostgresConnect;
use crate::config::StatementLogConfig;
use sqlx::ConnectOptions;
use sqlx::postgres::PgConnectOptions;

/// Statement logging — the only `ConnectOptions`-trait knobs this module sets.
pub(crate) fn statement_logging<O: ConnectOptions>(options: O, log: &StatementLogConfig) -> O {
    options
        .log_statements(log.log_statements)
        .log_slow_statements(log.log_slow_statements, log.log_slow_statements_threshold)
}

/// Map the resolved postgres config onto sqlx's native options, field by
/// field. The constructor seeds fields from `PG*` env vars (and `new` would
/// additionally consult `~/.pgpass`), so the connection identity — host, port,
/// user, password, ssl mode, database — is always set from the config;
/// remaining unset fields keep the libpq-style env layering.
pub fn pg_connect_options(c: &PostgresConnect) -> PgConnectOptions {
    let o = &c.options;
    let mut options = PgConnectOptions::new_without_pgpass()
        .host(&o.host)
        .port(o.port)
        .username(&c.username)
        .password(&c.password)
        .ssl_mode(o.ssl_mode.into())
        .database(o.database.as_deref().unwrap_or(&c.username));
    if let Some(socket) = &o.socket {
        options = options.socket(socket);
    }
    if let Some(cert) = &o.ssl_root_cert {
        options = options.ssl_root_cert(cert);
    }
    if let Some(pem) = &o.ssl_root_cert_pem {
        options = options.ssl_root_cert_from_pem(pem.clone().into_bytes());
    }
    if let Some(cert) = &o.ssl_client_cert {
        options = options.ssl_client_cert(cert);
    }
    if let Some(pem) = &o.ssl_client_cert_pem {
        options = options.ssl_client_cert_from_pem(pem);
    }
    if let Some(key) = &o.ssl_client_key {
        options = options.ssl_client_key(key);
    }
    if let Some(pem) = &o.ssl_client_key_pem {
        options = options.ssl_client_key_from_pem(pem);
    }
    if let Some(capacity) = o.statement_cache_capacity {
        options = options.statement_cache_capacity(capacity);
    }
    if let Some(name) = &o.application_name {
        options = options.application_name(name);
    }
    if let Some(digits) = o.extra_float_digits {
        options = options.extra_float_digits(digits);
    }
    if !o.options.is_empty() {
        options = options.options(&o.options);
    }
    statement_logging(options, &o.log)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PostgresConfig;
    use crate::test_utils::server;
    use sqlx::postgres::PgPoolOptions;
    use std::collections::BTreeMap;

    #[test]
    fn pg_connect_options_mapping() {
        let connect = PostgresConnect {
            options: PostgresConfig {
                host: "db.example.com".to_string(),
                port: 6432,
                database: Some("postgres".to_string()),
                ..Default::default()
            },
            username: "admin".to_string(),
            password: "secret".to_string(),
        };
        let options = pg_connect_options(&connect);
        assert_eq!(options.get_host(), "db.example.com");
        assert_eq!(options.get_port(), 6432);
        assert_eq!(options.get_database(), Some("postgres"));
        assert_eq!(options.get_username(), "admin");
    }

    /// With `database:` unset the pool targets the username's database — set
    /// explicitly so an ambient `PGDATABASE` cannot redirect it.
    #[test]
    fn pg_connect_options_database_defaults_to_username() {
        let connect = PostgresConnect {
            options: PostgresConfig::default(),
            username: "admin".to_string(),
            password: "secret".to_string(),
        };
        let options = pg_connect_options(&connect);
        assert_eq!(options.get_database(), Some("admin"));
    }

    /// The session settings sqlx exposes no getters for (`application_name`,
    /// the startup `options` GUCs, `extra_float_digits`) actually reach the
    /// server — read them back over a live connection. `extra_float_digits: 1`
    /// is deliberate: sqlx sends `2` when unset, so a dropped mapping fails.
    #[tokio::test]
    async fn pg_connect_options_reach_the_server() {
        server::ensure_started().await;
        let base = server::admin_base();
        let connect = PostgresConnect {
            options: PostgresConfig {
                application_name: Some("td-connect-options-test".to_string()),
                extra_float_digits: Some(1),
                options: BTreeMap::from([(
                    "search_path".to_string(),
                    "information_schema".to_string(),
                )]),
                ..base.options
            },
            ..base
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pg_connect_options(&connect))
            .await
            .unwrap();
        let (application_name, search_path, extra_float_digits): (String, String, String) =
            sqlx::query_as(
                "SELECT current_setting('application_name'), \
                 current_setting('search_path'), \
                 current_setting('extra_float_digits')",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(application_name, "td-connect-options-test");
        assert_eq!(search_path, "information_schema");
        assert_eq!(extra_float_digits, "1");
        pool.close().await;
    }
}
