//
// Copyright 2026 Tabs Data Inc.
//

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Config knobs for the in-process embedded Postgres backend. Used by the
/// apiserver's no-URL fallback and by the per-test postgres factory.
///
/// Boot logic lives in [`crate::embedded::start`]; this struct is the data
/// half — what to pass to `start`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmbeddedConfig {
    /// Where the server stores its data. `None` lets `postgresql_embedded`
    /// pick a temp directory (and clean it up on drop when `persistent` is
    /// false). Tests use `None`; the apiserver passes `Some(repo/database)`.
    pub data_dir: Option<PathBuf>,
    /// Fixed listening port. `None` requests a random ephemeral port. Tests
    /// use `None`; the apiserver exposes this via a CLI flag so `psql` can
    /// attach to a known port.
    pub port: Option<u16>,
    /// When `true`, the data directory survives across runs (apiserver). When
    /// `false`, the data directory is wiped on drop (tests).
    pub persistent: bool,
    /// Superuser name. When `Some`, the value is used as-is. When `None`,
    /// `postgresql_embedded`'s built-in default applies.
    #[serde(default)]
    pub username: Option<String>,
    /// Superuser password. When `Some`, the value is used as-is. When
    /// `None`, the persistent path falls back to a sidecar password file
    /// alongside `data_dir`. Configmap-driven deployments set this from a
    /// kubernetes Secret so the password lives outside the pod filesystem.
    #[serde(default)]
    pub password: Option<String>,
}
