//
// Copyright 2026 Tabsdata Inc.
//

//! Gates on the `database_server:` entry — the privileged admin connection
//! and boot target, read only by `td_dbmanager`. It reuses the
//! [`ComponentConfig`] shape: a dedicated schema-less struct would let
//! `flatten` silently swallow a stray `schema:`; [`validate_server`] reports
//! it instead.

use super::credentials::{EmbeddedPassword, resolve_password};
use crate::config::{
    ComponentConfig, ConnectConfig, DatabaseConfig, EMBEDDED_CREDENTIAL, PostgresConfig,
    PostgresConnect,
};
use crate::error::DbError;
use std::net::IpAddr;

/// The superuser the embedded boot always bakes in.
pub(crate) const DEFAULT_SUPERUSER: &str = "postgres";

/// Shape rules for the server entry: a PostgreSQL backend, no home schema.
/// Returns the entry's postgres options as the evidence.
pub fn validate_server(server: &ComponentConfig) -> Result<&PostgresConfig, DbError> {
    let pg = server
        .conn
        .postgres()
        .ok_or(DbError::ServerBackendNotPostgres)?;
    if server.schema.is_some() {
        return Err(DbError::ServerSchemaNotAllowed);
    }
    Ok(pg)
}

/// The boot-path locality rule: loopback or `localhost` counts; no other host
/// is inherently local (the config default is `localhost`).
pub fn is_local(server: &ComponentConfig) -> bool {
    let Some(pg) = server.conn.postgres() else {
        return false;
    };
    match pg.host.parse::<IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => pg.host.eq_ignore_ascii_case("localhost"),
    }
}

/// The single boot gate: present, well-shaped, local, and a bootable superuser
/// name (`initdb` always bakes `postgres`, so any other explicit name could
/// never log in). `serve` treats every error as fatal; `start`/`stop`
/// downgrade the missing/non-local cases to logged no-ops.
pub fn bootable_server(server: Option<&ComponentConfig>) -> Result<&ComponentConfig, DbError> {
    let server = server.ok_or(DbError::MissingServerEntry)?;
    let pg = validate_server(server)?;
    if !is_local(server) {
        return Err(DbError::NonLocalServerHost(pg.host.clone()));
    }
    if let Some(username) = pg.username.as_deref()
        && username != EMBEDDED_CREDENTIAL
        && username != DEFAULT_SUPERUSER
    {
        return Err(DbError::ServerUsernameNotBootable(username.to_owned()));
    }
    Ok(server)
}

/// The privileged connectable: the server entry with no home schema.
/// `${embedded}` resolves to the boot superuser / the password persisted at
/// first boot; absent credentials are an error.
pub fn server_connectable(server: &ComponentConfig) -> Result<DatabaseConfig, DbError> {
    let pg = validate_server(server)?;
    let username = match pg.username.as_deref() {
        Some(EMBEDDED_CREDENTIAL) => DEFAULT_SUPERUSER.to_owned(),
        Some(username) => username.to_owned(),
        None => return Err(DbError::MissingCredentials),
    };
    let password = resolve_password(pg.password.as_deref(), EmbeddedPassword::Superuser)?
        .ok_or(DbError::MissingCredentials)?;
    Ok(DatabaseConfig {
        conn: ConnectConfig::Postgres(PostgresConnect {
            options: pg.clone(),
            username,
            password,
        }),
        schema: None,
        app_role: server.app_role.clone(),
        pool: server.pool.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(yaml: &str) -> ComponentConfig {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[cfg(postgres)]
    #[test]
    fn server_connectable_explicit_credentials() {
        let entry = server(
            "
postgres:
  host: db.example.com
  port: 5432
  database: postgres
  username: admin
  password: secret
pool:
  max_connections: 2
",
        );
        let config = server_connectable(&entry).unwrap();
        #[allow(irrefutable_let_patterns)]
        let ConnectConfig::Postgres(pg) = &config.conn else {
            panic!("expected postgres connect");
        };
        assert_eq!(pg.username, "admin");
        assert_eq!(pg.password, "secret");
        assert!(config.schema.is_none());
        assert_eq!(config.pool.max_connections, 2);
    }

    #[cfg(postgres)]
    #[test]
    fn server_connectable_requires_credentials() {
        let entry = server(
            "
postgres:
  host: localhost
",
        );
        assert!(matches!(
            server_connectable(&entry),
            Err(DbError::MissingCredentials)
        ));
    }

    #[cfg(postgres)]
    #[test]
    fn server_connectable_embedded_username() {
        let entry = server(
            "
postgres:
  username: ${embedded}
  password: secret
",
        );
        let config = server_connectable(&entry).unwrap();
        #[allow(irrefutable_let_patterns)]
        let ConnectConfig::Postgres(pg) = &config.conn else {
            panic!("expected postgres connect");
        };
        assert_eq!(pg.username, DEFAULT_SUPERUSER);
    }

    #[cfg(all(postgres, sqlite))]
    #[test]
    fn server_shape_rules() {
        let entry = server(
            "
sqlite:
  path: /tmp/server.db
",
        );
        assert!(matches!(
            validate_server(&entry),
            Err(DbError::ServerBackendNotPostgres)
        ));

        let entry = server(
            "
postgres:
  host: localhost
schema: td_system
",
        );
        assert!(matches!(
            validate_server(&entry),
            Err(DbError::ServerSchemaNotAllowed)
        ));
    }

    #[cfg(postgres)]
    #[test]
    fn server_locality() {
        for host in ["localhost", "LOCALHOST", "127.0.0.1", "127.0.0.2", "::1"] {
            assert!(is_local(&server(&format!(
                "
postgres:
  host: '{host}'
  port: 7432
"
            ))));
        }
        for host in ["db.example.com", "10.0.0.5", "2001:db8::1"] {
            assert!(!is_local(&server(&format!(
                "
postgres:
  host: '{host}'
"
            ))));
        }
        // The default host is localhost, so an empty block is local.
        assert!(is_local(&server("postgres: {}")));
    }

    #[cfg(postgres)]
    #[test]
    fn bootable_server_gate() {
        assert!(matches!(
            bootable_server(None),
            Err(DbError::MissingServerEntry)
        ));
        assert!(matches!(
            bootable_server(Some(&server(
                "
postgres:
  host: db.example.com
"
            ))),
            Err(DbError::NonLocalServerHost(host)) if host == "db.example.com"
        ));
        assert!(matches!(
            bootable_server(Some(&server(
                "
postgres:
  host: localhost
  username: admin
"
            ))),
            Err(DbError::ServerUsernameNotBootable(name)) if name == "admin"
        ));
        for username in ["postgres", "${embedded}"] {
            bootable_server(Some(&server(&format!(
                "
postgres:
  host: localhost
  username: {username}
"
            ))))
            .unwrap();
        }
    }
}
