//
// Copyright 2026 Tabs Data Inc.
//

//! [`ComponentConfig`] — one named database connection — and
//! [`DatabaseComponents`], the `database:` section. The privileged
//! `database_server:` section reuses the same shape (see [`crate::server`]).

use super::Schema;
use super::backend::BackendConfig;
use super::pool::PoolConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The `database:` section: the registry components' connections, keyed by
/// component name. No wrapper semantics — just named connections.
pub type DatabaseComponents = BTreeMap<String, ComponentConfig>;

/// Explicit credential placeholder: "use the managed server's generated
/// credentials". An absent field is absent — never a silent fallback to these.
pub const EMBEDDED_CREDENTIAL: &str = "${embedded}";

/// One named database connection: a URL-addressed backend, credentials, and
/// its own pool. Nothing is inherited between entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentConfig {
    /// Connection target, inferred from the URL scheme.
    #[serde(flatten)]
    pub conn: BackendConfig,
    /// Schema override (default: the component's
    /// [`default_schema`](crate::components::SchemaComponent::default_schema)).
    /// Not allowed on the `database_server:` entry.
    #[serde(default)]
    pub schema: Option<Schema>,
    /// Login username (component default: the schema-named owner role;
    /// [`EMBEDDED_CREDENTIAL`] on the server entry: the boot superuser).
    #[serde(default)]
    pub username: Option<String>,
    /// Login password; [`EMBEDDED_CREDENTIAL`] opts into the persisted one.
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default, flatten)]
    pub pool: PoolConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(postgres)]
    #[test]
    fn component_config_flatten_shape() {
        let yaml = "
url: postgres://localhost:7432/postgres
schema: td_apiserver
password: owner_secret
max_connections: 10
";
        let entry: ComponentConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(entry.conn.url().port(), Some(7432));
        assert_eq!(entry.schema.as_deref(), Some("td_apiserver"));
        assert!(entry.username.is_none());
        assert_eq!(entry.password.as_deref(), Some("owner_secret"));
        assert_eq!(entry.pool.max_connections, 10);
        assert_eq!(
            entry.pool.min_connections,
            PoolConfig::default().min_connections
        );
    }

    /// An entry without `url:` matches no backend variant and fails serde.
    #[test]
    fn component_config_requires_url() {
        let yaml = "
username: admin
password: secret
";
        assert!(serde_yaml::from_str::<ComponentConfig>(yaml).is_err());
    }

    /// The placeholder must survive yaml as a plain string.
    #[cfg(postgres)]
    #[test]
    fn embedded_placeholder_is_a_plain_string() {
        let yaml = "
url: postgres://localhost/postgres
password: ${embedded}
";
        let entry: ComponentConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(entry.password.as_deref(), Some(EMBEDDED_CREDENTIAL));
    }
}
