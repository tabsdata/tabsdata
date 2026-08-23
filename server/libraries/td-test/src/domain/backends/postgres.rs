//
// Copyright 2026 Tabsdata Inc.
//

//! Fixture structs for the `postgres` backend — port of
//! `tabsdatax.itest.fixture.backends.postgres`.
//!
//! A tdx `postgres` system is a container running the server; a domain is one
//! database in it, owned by a role of the same name, holding the tables
//! `create-data` seeded. Postgres calls the entity container a schema, so the
//! descriptor carries the database name as `DB`, `DATABASE` and `SCHEMA` alike.

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
        container: String,
        image: String,
        host: String,
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
        host: String,
        port: String,
        user: String,
        password: String,
        db: String,
        /// The credential-less URI, as the descriptor spells it; the owner
        /// credentials are the `user` / `password` fields.
        uri: String,
        /// The SQLAlchemy URI, dialect prefix and all — Python's connection
        /// string, not one a Rust client can use. See [`PostgresDomain::url`].
        sqlalchemy_uri: String,
        /// What postgres calls the entity container (== `db`).
        schema: String,
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
        url_of(&self.host, &self.port, db, &self.user, &self.password)
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
    /// This is the one to connect with, not the `sqlalchemy_uri` field: that
    /// carries the `+psycopg2` dialect prefix only SQLAlchemy understands, and
    /// the `uri` field carries no credentials at all.
    pub fn url(&self) -> Result<Url, String> {
        url_of(&self.host, &self.port, &self.db, &self.user, &self.password)
    }

    /// The published port, parsed.
    pub fn port(&self) -> Result<u16, String> {
        self.port
            .parse()
            .map_err(|e| format!("'{}' is not a port: {e}", self.port))
    }

    /// The seeded tables with their row counts. Empty until `tdx-domain
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
    use crate::domain::descriptor::{DomainInfo, Info, SystemInfo};
    use crate::domain::{Attrs, Backend, Role};

    const PASSWORD: &str = "AbcdefghIJKL0123mnop";

    fn system_info(meta: BTreeMap<String, String>) -> Info {
        Info::System(SystemInfo {
            system: "postgres".into(),
            name: "postgres".into(),
            uuid: String::new(),
            started: String::new(),
            meta,
        })
    }

    fn system_meta() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("SYSTEM_KIND".to_string(), "sql".to_string()),
            ("CONTAINER".to_string(), "tdx-postgres".to_string()),
            ("IMAGE".to_string(), "postgres:17".to_string()),
            ("HOST".to_string(), "127.0.0.1".to_string()),
            ("PORT".to_string(), "15432".to_string()),
            ("USER".to_string(), "postgres".to_string()),
            ("PASSWORD".to_string(), PASSWORD.to_string()),
        ])
    }

    fn domain_meta() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("SYSTEM_KIND".to_string(), "sql".to_string()),
            ("IMAGE".to_string(), "postgres:17".to_string()),
            ("HOST".to_string(), "127.0.0.1".to_string()),
            ("PORT".to_string(), "15432".to_string()),
            ("USER".to_string(), "d_tucu_0_landing".to_string()),
            ("PASSWORD".to_string(), PASSWORD.to_string()),
            ("DB".to_string(), "d_tucu_0_landing".to_string()),
            ("DATABASE".to_string(), "d_tucu_0_landing".to_string()),
            ("SCHEMA".to_string(), "d_tucu_0_landing".to_string()),
            (
                "URI".to_string(),
                "postgresql+psycopg2://127.0.0.1:15432/d_tucu_0_landing".to_string(),
            ),
            (
                "SQLALCHEMY_URI".to_string(),
                format!(
                    "postgresql+psycopg2://d_tucu_0_landing:{PASSWORD}@127.0.0.1:15432/d_tucu_0_landing"
                ),
            ),
        ])
    }

    fn domain_info(meta: BTreeMap<String, String>) -> Info {
        Info::Domain(DomainInfo {
            system: "postgres".into(),
            name: "landing".into(),
            uuid: String::new(),
            started: String::new(),
            sys_name: "postgres".into(),
            external_name: "d_tucu_0_landing".into(),
            meta,
            model: Some("world".into()),
            data: Some("200".into()),
            entities: BTreeMap::from([
                ("city".to_string(), 4079),
                ("country".to_string(), 239),
                ("country_language".to_string(), 984),
            ]),
            seed: 0,
        })
    }

    fn domain() -> PostgresDomain {
        PostgresDomain::build(&Attrs::of(&domain_info(domain_meta()))).unwrap()
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
        assert_eq!(d.host, "127.0.0.1");
        assert_eq!(d.port, "15432");
        assert_eq!(d.user, "d_tucu_0_landing");
        assert_eq!(d.password, PASSWORD);
        assert_eq!(d.db, "d_tucu_0_landing");
        assert_eq!(
            d.schema, d.db,
            "postgres names the entity container a schema"
        );
        assert!(d.uri.starts_with("postgresql+psycopg2://127.0.0.1"));
        assert!(d.sqlalchemy_uri.contains(PASSWORD));
        assert_eq!(d.model.as_deref(), Some("world"));
        assert_eq!(d.data.as_deref(), Some("200"));
        assert_eq!(d.entities.as_ref().unwrap()["city"], 4079);
    }

    #[test]
    fn test_build_system_from_descriptor() {
        let s = PostgresSystem::build(&Attrs::of(&system_info(system_meta()))).unwrap();
        assert_eq!(s.name, "postgres");
        assert_eq!(s.container, "tdx-postgres");
        assert_eq!(s.user, "postgres");
        assert_eq!(s.port().unwrap(), 15432);
    }

    #[test]
    fn test_build_reports_every_missing_key() {
        let mut meta = domain_meta();
        meta.remove("DB");
        meta.remove("SQLALCHEMY_URI");
        let err = PostgresDomain::build(&Attrs::of(&domain_info(meta))).unwrap_err();
        assert!(err.contains("DB"), "got: {err}");
        assert!(err.contains("SQLALCHEMY_URI"), "got: {err}");
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
        let s = PostgresSystem::build(&Attrs::of(&system_info(system_meta()))).unwrap();
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
        let mut meta = domain_meta();
        meta.insert("PASSWORD".to_string(), "p@ss/w%rd?".to_string());
        let d = PostgresDomain::build(&Attrs::of(&domain_info(meta))).unwrap();
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
        let Info::Domain(mut info) = domain_info(domain_meta()) else {
            unreachable!()
        };
        info.entities.clear();
        let d = PostgresDomain::build(&Attrs::of(&Info::Domain(info))).unwrap();
        assert_eq!(d.entities, None);
        assert_eq!(d.tables(), vec![]);
        assert_eq!(d.rows("city"), None);
    }
}
