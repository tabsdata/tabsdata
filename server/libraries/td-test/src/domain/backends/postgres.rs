//
// Copyright 2026 Tabsdata Inc.
//

//! Fixture structs for the `postgres` backend — port of
//! `tabsdatax.itest.fixture.backends.postgres`.
//!
//! A tdd `postgres` system is a container running the server; a domain is one
//! database in it, owned by a role of the same name, holding the tables
//! `create-data` seeded. Postgres calls the entity container a schema, so the
//! descriptor carries the database name as `DATABASE` and `SCHEMA` alike. The
//! postgres family names the network it is reached on: tests reach it from the
//! host, so they read the `EXTERNAL_*` host and URIs.

use std::collections::BTreeMap;

use url::Url;

/// The maintenance database every server has. `CREATE` / `DROP DATABASE` cannot
/// run from a connection to the database being changed, so admin work goes
/// through this one — see [`PostgresSystem::url`].
pub const MAINTENANCE_DB: &str = "postgres";

crate::define_backend! {
    /// A `postgres` system: the container and its superuser.
    pub struct PostgresSystem: system("postgres") {
        name: String,
        system_kind: String,
        docker_container_name: String,
        image: String,
        external_host: String,
        /// The host-side port the container publishes, as the descriptor spells
        /// it. [`PostgresSystem::port`] is the parsed form.
        port: String,
        user: String,
        password: String,
    }
}

crate::define_backend! {
    /// A `postgres` domain: one database, with its own owner role.
    pub struct PostgresDomain: domain("postgres") {
        name: String,
        system_kind: String,
        image: String,
        external_host: String,
        port: String,
        user: String,
        password: String,
        database: String,
        /// The credential-less URI, as the descriptor spells it; the owner
        /// credentials are the `user` / `password` fields.
        external_uri: String,
        /// The SQLAlchemy URI, dialect prefix and all — Python's connection
        /// string, not one a Rust client can use. See [`PostgresDomain::url`].
        external_sqlalchemy_uri: String,
        /// What postgres calls the entity container (== `database`).
        schema: String,
        /// The owning system's superuser. The domain's own `user` owns only this
        /// database; creating schemas in it and ensuring the cluster-global RLS
        /// role need the superuser. See [`PostgresDomain::admin_url`].
        sys_admin_user: String,
        sys_admin_password: String,
        model: Option<String>,
        data: Option<String>,
        /// `{table: record count}`, once loaded.
        entities: Option<BTreeMap<String, i64>>,
    }
}

/// Percent-encode a credential for a url's userinfo, escaping everything
/// outside the unreserved set.
///
/// `Url::set_username` / `set_password` escape the delimiters but leave a
/// literal `%` alone, so a credential containing one would come back out of the
/// url as something other than what went in.
fn encode_credential(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

/// `postgres://<user>:<password>@<host>:<port>/<db>`.
fn url_of(host: &str, port: &str, db: &str, user: &str, password: &str) -> Result<Url, String> {
    let authority = match port.is_empty() {
        true => host.to_string(),
        false => format!("{host}:{port}"),
    };
    let userinfo = match user.is_empty() {
        true => String::new(),
        false => format!(
            "{}:{}@",
            encode_credential(user),
            encode_credential(password)
        ),
    };
    let uri = format!("postgres://{userinfo}{authority}/{db}");
    Url::parse(&uri)
        .map_err(|e| format!("cannot build a postgres url from '{authority}/{db}': {e}"))
}

impl PostgresSystem {
    /// A superuser connection to `db`.
    pub fn url(&self, db: &str) -> Result<Url, String> {
        url_of(
            &self.external_host,
            &self.port,
            db,
            &self.user,
            &self.password,
        )
    }

    /// A superuser connection to the [`MAINTENANCE_DB`] — where `CREATE` /
    /// `DROP DATABASE` have to be issued from, a database not being droppable
    /// from within itself.
    pub fn admin_url(&self) -> Result<Url, String> {
        self.url(MAINTENANCE_DB)
    }

    /// The published port, parsed.
    pub fn port(&self) -> Result<u16, String> {
        self.port
            .parse()
            .map_err(|e| format!("'{}' is not a port: {e}", self.port))
    }
}

impl PostgresDomain {
    /// `postgres://<user>:<password>@<host>:<port>/<db>` — the owner's
    /// connection, in the libpq form sqlx and tokio-postgres take.
    ///
    /// This is the one to connect with, not the `external_sqlalchemy_uri`
    /// field: that carries the `+psycopg2` dialect prefix only SQLAlchemy
    /// understands, and the `external_uri` field carries no credentials at all.
    pub fn url(&self) -> Result<Url, String> {
        url_of(
            &self.external_host,
            &self.port,
            &self.database,
            &self.user,
            &self.password,
        )
    }

    /// The owning system's superuser, connected to this domain's own database —
    /// what a caller needs to create or drop schemas inside it.
    pub fn admin_url(&self) -> Result<Url, String> {
        url_of(
            &self.external_host,
            &self.port,
            &self.database,
            &self.sys_admin_user,
            &self.sys_admin_password,
        )
    }

    /// The published port, parsed.
    pub fn port(&self) -> Result<u16, String> {
        self.port
            .parse()
            .map_err(|e| format!("'{}' is not a port: {e}", self.port))
    }

    /// The seeded tables with their row counts. Empty until `tdd domain
    /// create-data` has run.
    pub fn tables(&self) -> Vec<(&str, i64)> {
        self.entities
            .iter()
            .flatten()
            .map(|(table, rows)| (table.as_str(), *rows))
            .collect()
    }

    /// The row count `create-data` seeded into `table`, or `None` when the
    /// domain has no such table.
    pub fn rows(&self, table: &str) -> Option<i64> {
        self.entities.as_ref()?.get(table).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Attrs, Backend, Role, parse_descriptor};
    use td_tdd::Descriptor;

    const PASSWORD: &str = "AbcdefghIJKL0123mnop";

    fn system() -> Descriptor {
        parse_descriptor(
            r#"
            {
                "role": "system",
                "system": "postgres",
                "name": "postgres",
                "meta": {
                    "SYSTEM_KIND": "sql",
                    "DOCKER_CONTAINER_NAME": "tdd_postgres",
                    "IMAGE": "postgres:17",
                    "EXTERNAL_HOST": "127.0.0.1",
                    "PORT": "15432",
                    "USER": "postgres",
                    "PASSWORD": "AbcdefghIJKL0123mnop"
                }
            }
            "#,
        )
    }

    fn landing() -> Descriptor {
        parse_descriptor(
            r#"
            {
                "role": "domain",
                "system": "postgres",
                "name": "landing",
                "sys_name": "postgres",
                "external_name": "d_tucu_0_landing",
                "meta": {
                    "SYSTEM_KIND": "sql",
                    "IMAGE": "postgres:17",
                    "EXTERNAL_HOST": "127.0.0.1",
                    "PORT": "15432",
                    "USER": "d_tucu_0_landing",
                    "PASSWORD": "AbcdefghIJKL0123mnop",
                    "DATABASE": "d_tucu_0_landing",
                    "SCHEMA": "d_tucu_0_landing",
                    "SYS_ADMIN_USER": "postgres",
                    "SYS_ADMIN_PASSWORD": "AbcdefghIJKL0123mnop",
                    "EXTERNAL_URI": "postgresql+psycopg2://127.0.0.1:15432/d_tucu_0_landing",
                    "EXTERNAL_SQLALCHEMY_URI": "postgresql+psycopg2://d_tucu_0_landing:AbcdefghIJKL0123mnop@127.0.0.1:15432/d_tucu_0_landing"
                },
                "model": "world",
                "data": "200",
                "entities": {
                    "city": 4079,
                    "country": 239,
                    "country_language": 984
                }
            }
            "#,
        )
    }

    fn domain() -> PostgresDomain {
        PostgresDomain::build(&Attrs::of(&landing())).unwrap()
    }

    #[test]
    fn test_roles_and_system_types() {
        assert_eq!(PostgresSystem::SYSTEM_TYPE, "postgres");
        assert_eq!(PostgresSystem::ROLE, Role::System);
        assert_eq!(PostgresDomain::SYSTEM_TYPE, "postgres");
        assert_eq!(PostgresDomain::ROLE, Role::Domain);
    }

    #[test]
    fn test_build_domain_from_descriptor() {
        let d = domain();
        assert_eq!(d.name, "landing");
        assert_eq!(d.system_kind, "sql");
        assert_eq!(d.image, "postgres:17");
        assert_eq!(d.external_host, "127.0.0.1");
        assert_eq!(d.port, "15432");
        assert_eq!(d.user, "d_tucu_0_landing");
        assert_eq!(d.password, PASSWORD);
        assert_eq!(d.database, "d_tucu_0_landing");
        assert_eq!(
            d.schema, d.database,
            "postgres names the entity container a schema"
        );
        assert!(
            d.external_uri
                .starts_with("postgresql+psycopg2://127.0.0.1")
        );
        assert!(d.external_sqlalchemy_uri.contains(PASSWORD));
        assert_eq!(d.model.as_deref(), Some("world"));
        assert_eq!(d.data.as_deref(), Some("200"));
        assert_eq!(d.entities.as_ref().unwrap()["city"], 4079);
        assert_eq!(d.sys_admin_user, "postgres");
        assert_eq!(d.sys_admin_password, PASSWORD);
    }

    /// The owner connects as itself; the superuser reaches the same database
    /// with the rights to add and drop schemas in it.
    #[test]
    fn test_domain_admin_url_is_the_superuser_on_the_same_database() {
        let d = domain();
        let admin = d.admin_url().unwrap();
        assert_eq!(admin.username(), "postgres");
        assert_eq!(admin.path(), d.url().unwrap().path());
        assert_eq!(admin.port(), d.url().unwrap().port());
    }

    #[test]
    fn test_build_system_from_descriptor() {
        let s = PostgresSystem::build(&Attrs::of(&system())).unwrap();
        assert_eq!(s.name, "postgres");
        assert_eq!(s.docker_container_name, "tdd_postgres");
        assert_eq!(s.user, "postgres");
        assert_eq!(s.port().unwrap(), 15432);
    }

    #[test]
    fn test_build_reports_every_missing_key() {
        let mut descriptor = landing();
        descriptor.meta.remove("DATABASE");
        descriptor.meta.remove("EXTERNAL_SQLALCHEMY_URI");
        let err = PostgresDomain::build(&Attrs::of(&descriptor)).unwrap_err();
        assert!(err.contains("DATABASE"), "got: {err}");
        assert!(err.contains("EXTERNAL_SQLALCHEMY_URI"), "got: {err}");
    }

    #[test]
    fn test_domain_url_is_a_libpq_url() {
        let url = domain().url().unwrap();
        assert_eq!(
            url.as_str(),
            format!("postgres://d_tucu_0_landing:{PASSWORD}@127.0.0.1:15432/d_tucu_0_landing")
        );
        assert_eq!(url.scheme(), "postgres");
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert_eq!(url.port(), Some(15432));
        assert_eq!(url.path(), "/d_tucu_0_landing");
    }

    /// The superuser connects to the maintenance database, never to the one it
    /// is about to create or drop.
    #[test]
    fn test_system_url_takes_the_database() {
        let s = PostgresSystem::build(&Attrs::of(&system())).unwrap();
        assert_eq!(
            s.admin_url().unwrap().as_str(),
            format!("postgres://postgres:{PASSWORD}@127.0.0.1:15432/postgres")
        );
        assert_eq!(
            s.url("other").unwrap().path(),
            "/other",
            "the database is the caller's to choose"
        );
    }

    /// A credential that needs escaping must come back out as itself. The
    /// literal `%` is the one the url crate's own setters miss.
    #[test]
    fn test_url_percent_encodes_credentials() {
        let mut descriptor = landing();
        descriptor
            .meta
            .insert("PASSWORD".to_string(), "p@ss/w%rd?".to_string());
        let d = PostgresDomain::build(&Attrs::of(&descriptor)).unwrap();
        let url = d.url().unwrap();
        assert_eq!(url.password(), Some("p%40ss%2Fw%25rd%3F"));
        assert_eq!(url.host_str(), Some("127.0.0.1"), "the host is unaffected");
        assert_eq!(url.path(), "/d_tucu_0_landing");
    }

    /// The generated credentials are alphanumeric, so the common case must pass
    /// through untouched rather than arrive escaped.
    #[test]
    fn test_encode_credential_leaves_the_unreserved_set_alone() {
        assert_eq!(encode_credential(PASSWORD), PASSWORD);
        assert_eq!(encode_credential("d_tucu_0_landing"), "d_tucu_0_landing");
        assert_eq!(encode_credential("a-b.c~d"), "a-b.c~d");
        assert_eq!(encode_credential(""), "");
        assert_eq!(encode_credential("%"), "%25");
        assert_eq!(encode_credential("ü"), "%C3%BC");
    }

    #[test]
    fn test_tables_and_rows() {
        let d = domain();
        assert_eq!(
            d.tables(),
            vec![("city", 4079), ("country", 239), ("country_language", 984)]
        );
        assert_eq!(d.rows("country"), Some(239));
        assert_eq!(d.rows("nope"), None);
    }

    #[test]
    fn test_tables_without_create_data() {
        let mut descriptor = landing();
        descriptor.entities.clear();
        let d = PostgresDomain::build(&Attrs::of(&descriptor)).unwrap();
        assert_eq!(d.entities, None);
        assert_eq!(d.tables(), vec![]);
        assert_eq!(d.rows("city"), None);
    }
}
