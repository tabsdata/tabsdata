//
// Copyright 2026 Tabsdata Inc.
//

//! [`ComponentConfig`] — one named database connection — and
//! [`DatabaseComponents`], the `database:` section. The privileged
//! `database_server:` section reuses the same shape (see [`crate::server`]).

use super::Schema;
use super::connection::app_role_or_default;
use super::pool::PoolsConfig;
use super::postgres::PostgresConfig;
use crate::error::DbError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The `database:` section: the registry components' connections, keyed by
/// component name. No wrapper semantics — just named connections.
pub type DatabaseComponents = BTreeMap<String, ComponentConfig>;

/// Explicit credential placeholder: "use the managed server's generated
/// credentials". An absent field is absent — never a silent fallback to these.
pub const EMBEDDED_CREDENTIAL: &str = "${embedded}";

/// One named database connection: a block of connect options (credentials
/// included) and its own pool. Nothing is inherited between entries.
///
/// ```yaml
/// postgres:
///   host: 127.0.0.1
///   port: 5432
///   database: postgres
///   username: td_apiserver
///   password: ${embedded}
/// ```
///
/// Unknown entry-level keys are rejected, so stale keys from older config
/// shapes fail loudly instead of being silently dropped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentConfig {
    /// Connection target.
    pub postgres: PostgresConfig,
    /// Schema (and owner-role) override; defaults to `<database>_<key>` — see
    /// [`Self::schema`]. Not allowed on the `database_server:` entry.
    #[serde(default)]
    pub schema: Option<Schema>,
    /// RLS app role override; defaults to `<database>_app` — see
    /// [`Self::app_role`].
    #[serde(default)]
    pub app_role: Option<String>,
    /// Connection pool settings.
    #[serde(default)]
    pub pool: PoolsConfig,
}

/// PostgreSQL truncates longer identifiers silently (`NAMEDATALEN`), so the
/// created role/schema would not match the configured name.
const POSTGRES_IDENTIFIER_BYTES: usize = 63;

fn checked_identifier(name: String) -> Result<String, DbError> {
    if name.len() > POSTGRES_IDENTIFIER_BYTES {
        return Err(DbError::IdentifierTooLong(name));
    }
    Ok(name)
}

impl ComponentConfig {
    /// The entry's schema (and owner-role) name: the explicit `schema:`, else
    /// `<database>_<key>` — cluster-global role names derived from the
    /// database name keep instances sharing a PostgreSQL cluster from
    /// colliding — else the component's static default (no database name).
    pub fn schema(&self, key: &str, default_schema: &str) -> Result<String, DbError> {
        checked_identifier(match (&self.schema, self.postgres.database.as_deref()) {
            (Some(schema), _) => schema.to_string(),
            (None, Some(database)) => format!("{database}_{key}"),
            (None, None) => default_schema.to_owned(),
        })
    }

    /// The entry's RLS app role: the explicit `app_role:`, else
    /// `<database>_app` (see [`Self::schema`]), else the global
    /// [`APP_ROLE`](crate::APP_ROLE).
    pub(crate) fn app_role(&self) -> Result<String, DbError> {
        checked_identifier(match (&self.app_role, self.postgres.database.as_deref()) {
            (Some(app_role), _) => app_role.clone(),
            (None, Some(database)) => format!("{database}_app"),
            (None, None) => app_role_or_default(None).to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_config_shape() {
        let yaml = "
postgres:
  host: localhost
  port: 7432
  database: postgres
  password: owner_secret
schema: td_apiserver
app_role: td_app_i1
pool:
  regular:
    min_connections: 1
    max_connections: 10
";
        let entry: ComponentConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(entry.postgres.port, 7432);
        assert!(entry.postgres.username.is_none());
        assert_eq!(entry.postgres.password.as_deref(), Some("owner_secret"));
        assert_eq!(entry.schema.as_deref(), Some("td_apiserver"));
        assert_eq!(entry.app_role.as_deref(), Some("td_app_i1"));
        assert_eq!(entry.pool.regular.as_ref().unwrap().max_connections, 10);
        assert!(entry.pool.exclusive.is_none());
    }

    #[test]
    fn component_config_requires_connect_options() {
        let error = serde_yaml::from_str::<ComponentConfig>("schema: td_apiserver\n")
            .unwrap_err()
            .to_string();
        assert!(error.contains("missing field `postgres`"), "{error}");
    }

    /// Stale keys from the old flat shape (or any typo) fail loudly instead of
    /// being silently dropped.
    #[test]
    fn component_config_rejects_stale_flat_keys() {
        for stale in ["max_connections: 50", "username: td", "url: postgres://x/y"] {
            let yaml = format!(
                "
postgres:
  host: localhost
{stale}
"
            );
            let error = serde_yaml::from_str::<ComponentConfig>(&yaml)
                .unwrap_err()
                .to_string();
            assert!(error.contains("unknown field"), "{stale}: {error}");
        }
    }

    #[test]
    fn component_config_round_trip() {
        let yaml = "
postgres:
  host: db.example.com
  ssl_mode: require
  password: secret
schema: td_apiserver
pool:
  regular:
    min_connections: 1
    max_connections: 7
";
        let entry: ComponentConfig = serde_yaml::from_str(yaml).unwrap();
        let restored: ComponentConfig =
            serde_yaml::from_str(&serde_yaml::to_string(&entry).unwrap()).unwrap();
        assert_eq!(entry, restored);
    }

    /// Names derive from the database (`<db>_<key>`, `<db>_app`) so instances
    /// sharing a cluster never collide on cluster-global role names; explicit
    /// config wins; an entry with no database name keeps the static defaults.
    #[test]
    fn component_config_derived_names() {
        let entry: ComponentConfig = serde_yaml::from_str(
            "
postgres:
  host: localhost
  database: d_tenant
",
        )
        .unwrap();
        assert_eq!(
            entry.schema("apiserver", "td_apiserver").unwrap(),
            "d_tenant_apiserver"
        );
        assert_eq!(entry.app_role().unwrap(), "d_tenant_app");

        let explicit: ComponentConfig = serde_yaml::from_str(
            "
postgres:
  host: localhost
  database: d_tenant
schema: my_schema
app_role: my_app
",
        )
        .unwrap();
        assert_eq!(
            explicit.schema("apiserver", "td_apiserver").unwrap(),
            "my_schema"
        );
        assert_eq!(explicit.app_role().unwrap(), "my_app");

        let no_database: ComponentConfig = serde_yaml::from_str(
            "
postgres:
  host: localhost
",
        )
        .unwrap();
        assert_eq!(
            no_database.schema("apiserver", "td_apiserver").unwrap(),
            "td_apiserver"
        );
        assert_eq!(no_database.app_role().unwrap(), crate::APP_ROLE);

        let long: ComponentConfig = serde_yaml::from_str(&format!(
            "
postgres:
  host: localhost
  database: {}
",
            "d".repeat(60)
        ))
        .unwrap();
        assert!(matches!(
            long.schema("apiserver", "td_apiserver"),
            Err(DbError::IdentifierTooLong(_))
        ));
    }

    /// The placeholder must survive yaml as a plain string.
    #[test]
    fn embedded_placeholder_is_a_plain_string() {
        let yaml = "
postgres:
  password: ${embedded}
";
        let entry: ComponentConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            entry.postgres.password.as_deref(),
            Some(EMBEDDED_CREDENTIAL)
        );
    }
}
