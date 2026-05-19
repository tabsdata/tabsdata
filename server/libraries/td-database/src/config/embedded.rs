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
}
