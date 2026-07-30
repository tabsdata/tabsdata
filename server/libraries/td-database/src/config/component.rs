//
// Copyright 2026 Tabsdata Inc.
//

//! [`ComponentConfig`] — one named database connection — and
//! [`DatabaseComponents`], the `database:` section. The privileged
//! `database_server:` section reuses the same shape (see [`crate::server`]).

use super::Schema;
use super::backend::BackendConfig;
#[cfg(mysql)]
use super::mysql::MySqlConfig;
use super::pool::PoolConfig;
#[cfg(postgres)]
use super::postgres::PostgresConfig;
#[cfg(sqlite)]
use super::sqlite::SqliteConfig;
use crate::error::DbError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The `database:` section: the registry components' connections, keyed by
/// component name. No wrapper semantics — just named connections.
pub type DatabaseComponents = BTreeMap<String, ComponentConfig>;

/// Explicit credential placeholder: "use the managed server's generated
/// credentials". An absent field is absent — never a silent fallback to these.
pub const EMBEDDED_CREDENTIAL: &str = "${embedded}";

/// One named database connection: a backend-keyed block of connect options
/// (credentials included, where the backend has them) and its own pool.
/// Nothing is inherited between entries.
///
/// Deserialization goes through [`ComponentConfigShape`], which rejects
/// unknown entry-level keys (stale keys from older config shapes fail loudly
/// instead of being silently dropped) and requires exactly one backend key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ComponentConfigShape")]
pub struct ComponentConfig {
    /// Connection target, selected by the backend key (`postgres:`, ...).
    #[serde(flatten)]
    pub conn: BackendConfig,
    /// Schema override (default: the component's
    /// [`default_schema`](crate::components::SchemaComponent::default_schema)).
    /// Not allowed on the `database_server:` entry.
    #[serde(default)]
    pub schema: Option<Schema>,
    /// Connection pool settings.
    #[serde(default)]
    pub pool: PoolConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ComponentConfigShape {
    #[cfg(sqlite)]
    #[serde(default)]
    sqlite: Option<SqliteConfig>,
    #[cfg(postgres)]
    #[serde(default)]
    postgres: Option<PostgresConfig>,
    #[cfg(mysql)]
    #[serde(default)]
    mysql: Option<MySqlConfig>,
    #[serde(default)]
    schema: Option<Schema>,
    #[serde(default)]
    pool: PoolConfig,
}

impl TryFrom<ComponentConfigShape> for ComponentConfig {
    type Error = DbError;

    fn try_from(shape: ComponentConfigShape) -> Result<Self, DbError> {
        let mut backends = Vec::new();
        #[cfg(sqlite)]
        if let Some(c) = shape.sqlite {
            backends.push(BackendConfig::Sqlite(c));
        }
        #[cfg(postgres)]
        if let Some(c) = shape.postgres {
            backends.push(BackendConfig::Postgres(c));
        }
        #[cfg(mysql)]
        if let Some(c) = shape.mysql {
            backends.push(BackendConfig::MySql(c));
        }
        let mut backends = backends.into_iter();
        let conn = backends.next().ok_or(DbError::MissingBackend)?;
        if backends.next().is_some() {
            return Err(DbError::MultipleBackends);
        }
        Ok(ComponentConfig {
            conn,
            schema: shape.schema,
            pool: shape.pool,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(postgres)]
    #[test]
    fn component_config_shape() {
        let yaml = "
postgres:
  host: localhost
  port: 7432
  database: postgres
  password: owner_secret
schema: td_apiserver
pool:
  max_connections: 10
";
        let entry: ComponentConfig = serde_yaml::from_str(yaml).unwrap();
        let pg = entry.conn.postgres().unwrap();
        assert_eq!(pg.port, 7432);
        assert!(pg.username.is_none());
        assert_eq!(pg.password.as_deref(), Some("owner_secret"));
        assert_eq!(entry.schema.as_deref(), Some("td_apiserver"));
        assert_eq!(entry.pool.max_connections, 10);
        assert_eq!(
            entry.pool.min_connections,
            PoolConfig::default().min_connections
        );
    }

    #[test]
    fn component_config_requires_backend() {
        let yaml = "
schema: td_apiserver
";
        let error = serde_yaml::from_str::<ComponentConfig>(yaml)
            .unwrap_err()
            .to_string();
        assert!(error.contains("exactly one backend key"), "{error}");
    }

    #[cfg(all(postgres, sqlite))]
    #[test]
    fn component_config_rejects_multiple_backends() {
        let yaml = "
postgres:
  host: localhost
sqlite:
  path: /tmp/test.db
";
        let error = serde_yaml::from_str::<ComponentConfig>(yaml)
            .unwrap_err()
            .to_string();
        assert!(error.contains("more than one backend key"), "{error}");
    }

    /// Stale keys from the old flat shape (or any typo) fail loudly instead of
    /// being silently dropped.
    #[cfg(postgres)]
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

    #[cfg(sqlite)]
    #[test]
    fn component_config_sqlite_by_key() {
        let yaml = "
sqlite:
  path: /tmp/test.db
";
        let entry: ComponentConfig = serde_yaml::from_str(yaml).unwrap();
        #[allow(irrefutable_let_patterns)]
        let BackendConfig::Sqlite(c) = &entry.conn else {
            panic!("expected sqlite variant");
        };
        assert_eq!(c.path.as_deref(), Some("/tmp/test.db".as_ref()));
    }

    #[cfg(mysql)]
    #[test]
    fn component_config_mysql_by_key() {
        let yaml = "
mysql:
  host: db.example.com
";
        let entry: ComponentConfig = serde_yaml::from_str(yaml).unwrap();
        #[allow(irrefutable_let_patterns)]
        let BackendConfig::MySql(c) = &entry.conn else {
            panic!("expected mysql variant");
        };
        assert_eq!(c.host, "db.example.com");
    }

    /// Serialization keeps the single-key backend map shape the parser reads.
    #[cfg(postgres)]
    #[test]
    fn component_config_round_trip() {
        let yaml = "
postgres:
  host: db.example.com
  ssl_mode: require
  password: secret
schema: td_apiserver
pool:
  max_connections: 7
";
        let entry: ComponentConfig = serde_yaml::from_str(yaml).unwrap();
        let restored: ComponentConfig =
            serde_yaml::from_str(&serde_yaml::to_string(&entry).unwrap()).unwrap();
        assert_eq!(entry, restored);
    }

    /// The placeholder must survive yaml as a plain string.
    #[cfg(postgres)]
    #[test]
    fn embedded_placeholder_is_a_plain_string() {
        let yaml = "
postgres:
  password: ${embedded}
";
        let entry: ComponentConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            entry.conn.postgres().unwrap().password.as_deref(),
            Some(EMBEDDED_CREDENTIAL)
        );
    }
}
