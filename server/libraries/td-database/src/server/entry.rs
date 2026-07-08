//
// Copyright 2026 Tabs Data Inc.
//

//! Gates on the `database_server:` entry — the privileged admin connection
//! and boot target, read only by `td_dbmanager`. It reuses the
//! [`ComponentConfig`] shape: a dedicated schema-less struct would let
//! `flatten` silently swallow a stray `schema:`; [`validate_server`] reports
//! it instead.

use super::credentials::{EmbeddedPassword, resolve_password};
use crate::config::{ComponentConfig, DatabaseConfig, EMBEDDED_CREDENTIAL};
use crate::error::DbError;
use std::net::IpAddr;
use url::Host;

/// The superuser the embedded boot always bakes in.
pub(crate) const DEFAULT_SUPERUSER: &str = "postgres";

/// Shape rules for the server entry: a PostgreSQL URL, no home schema.
pub fn validate_server(server: &ComponentConfig) -> Result<(), DbError> {
    if !server.conn.is_postgres() {
        return Err(DbError::ServerBackendNotPostgres(
            server.conn.url().scheme().to_owned(),
        ));
    }
    if server.schema.is_some() {
        return Err(DbError::ServerSchemaNotAllowed);
    }
    Ok(())
}

/// The boot-path locality rule: loopback or `localhost` counts; no host is
/// inherently local. `postgres://` is not a "special" scheme for the url
/// crate, so IP hosts arrive as `Host::Domain` — hence the IP parse there.
pub fn is_local(server: &ComponentConfig) -> bool {
    match server.conn.url().host() {
        None => true,
        Some(Host::Domain(domain)) => match domain.parse::<IpAddr>() {
            Ok(ip) => ip.is_loopback(),
            Err(_) => domain.eq_ignore_ascii_case("localhost"),
        },
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
    }
}

/// The single boot gate: present, well-shaped, local, and a bootable superuser
/// name (`initdb` always bakes `postgres`, so any other explicit name could
/// never log in). `serve` treats every error as fatal; `start`/`stop`
/// downgrade the missing/non-local cases to logged no-ops.
pub fn bootable_server(server: Option<&ComponentConfig>) -> Result<&ComponentConfig, DbError> {
    let server = server.ok_or(DbError::MissingServerEntry)?;
    validate_server(server)?;
    if !is_local(server) {
        return Err(DbError::NonLocalServerUrl(
            server.conn.url().host_str().unwrap_or_default().to_owned(),
        ));
    }
    if let Some(username) = server.username.as_deref()
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
    validate_server(server)?;
    let username = match server.username.as_deref() {
        Some(EMBEDDED_CREDENTIAL) => DEFAULT_SUPERUSER.to_owned(),
        Some(username) => username.to_owned(),
        None => return Err(DbError::MissingCredentials),
    };
    let password = resolve_password(server.password.as_deref(), EmbeddedPassword::Superuser)?
        .ok_or(DbError::MissingCredentials)?;
    Ok(DatabaseConfig {
        conn: server.conn.clone(),
        schema: None,
        username: Some(username),
        password: Some(password),
        app_role: None,
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
url: postgres://db.example.com:5432/postgres
username: admin
password: secret
max_connections: 2
",
        );
        let config = server_connectable(&entry).unwrap();
        assert_eq!(config.username.as_deref(), Some("admin"));
        assert_eq!(config.password.as_deref(), Some("secret"));
        assert!(config.schema.is_none());
        assert_eq!(config.pool.max_connections, 2);
    }

    #[cfg(postgres)]
    #[test]
    fn server_connectable_requires_credentials() {
        let entry = server("url: postgres://localhost/postgres");
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
url: postgres://localhost/postgres
username: ${embedded}
password: secret
",
        );
        let config = server_connectable(&entry).unwrap();
        assert_eq!(config.username.as_deref(), Some(DEFAULT_SUPERUSER));
    }

    #[cfg(all(postgres, sqlite))]
    #[test]
    fn server_shape_rules() {
        let entry = server("url: file:///tmp/server.db");
        assert!(matches!(
            validate_server(&entry),
            Err(DbError::ServerBackendNotPostgres(scheme)) if scheme == "file"
        ));

        let entry = server(
            "
url: postgres://localhost/postgres
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
        for host in ["localhost", "LOCALHOST", "127.0.0.1", "127.0.0.2", "[::1]"] {
            assert!(is_local(&server(&format!(
                "url: postgres://{host}:7432/postgres"
            ))));
        }
        for host in ["db.example.com", "10.0.0.5", "[2001:db8::1]"] {
            assert!(!is_local(&server(&format!(
                "url: postgres://{host}/postgres"
            ))));
        }
    }

    #[cfg(postgres)]
    #[test]
    fn bootable_server_gate() {
        assert!(matches!(
            bootable_server(None),
            Err(DbError::MissingServerEntry)
        ));
        assert!(matches!(
            bootable_server(Some(&server("url: postgres://db.example.com/postgres"))),
            Err(DbError::NonLocalServerUrl(host)) if host == "db.example.com"
        ));
        assert!(matches!(
            bootable_server(Some(&server(
                "
url: postgres://localhost/postgres
username: admin
"
            ))),
            Err(DbError::ServerUsernameNotBootable(name)) if name == "admin"
        ));
        for username in ["postgres", "${embedded}"] {
            bootable_server(Some(&server(&format!(
                "
url: postgres://localhost/postgres
username: {username}
"
            ))))
            .unwrap();
        }
    }
}
