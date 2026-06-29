//
// Copyright 2026 Tabs Data Inc.
//

//! Incremental "last modified" cursor for `find` / `find-copy`.
//!
//! Tracks, per wildcard pattern, the newest `last_modified` already consumed and
//! the file names seen at exactly that timestamp, so re-running a request skips
//! files already returned. Serialized as base64(JSON) to travel as an opaque
//! `lastmod_info` string between calls.

use crate::transporter2::error::{Result, TransporterError};
use base64::Engine;
use base64::prelude::BASE64_STANDARD_NO_PAD;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastMod {
    initial: DateTime<Utc>,
    entries: HashMap<String, Entry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    lastmod: DateTime<Utc>,
    names: Vec<String>,
}

impl LastMod {
    pub fn new(initial: DateTime<Utc>) -> Self {
        Self {
            initial,
            entries: HashMap::new(),
        }
    }

    pub fn decode(s: &str) -> Result<Self> {
        let bytes = BASE64_STANDARD_NO_PAD
            .decode(s.as_bytes())
            .map_err(|e| TransporterError::Cursor(e.to_string()))?;
        serde_json::from_slice(&bytes).map_err(|e| TransporterError::Cursor(e.to_string()))
    }

    pub fn encode(&self) -> Result<String> {
        let json = serde_json::to_vec(self).map_err(|e| TransporterError::Cursor(e.to_string()))?;
        Ok(BASE64_STANDARD_NO_PAD.encode(json))
    }

    /// Returns `true` if the file should be processed now (and records it so it
    /// is skipped on later calls). Files older than the initial baseline, or
    /// already recorded, return `false`.
    pub fn check_and_set(&mut self, pattern: &str, filename: &str, lastmod: DateTime<Utc>) -> bool {
        match self.entries.get_mut(pattern) {
            None => {
                if lastmod >= self.initial {
                    self.entries.insert(
                        pattern.to_string(),
                        Entry {
                            lastmod,
                            names: vec![filename.to_string()],
                        },
                    );
                    true
                } else {
                    false
                }
            }
            Some(entry) => {
                if lastmod < entry.lastmod {
                    false
                } else if lastmod > entry.lastmod {
                    entry.lastmod = lastmod;
                    entry.names = vec![filename.to_string()];
                    true
                } else if entry.names.iter().any(|n| n == filename) {
                    false
                } else {
                    entry.names.push(filename.to_string());
                    true
                }
            }
        }
    }
}
