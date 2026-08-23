//
// Copyright 2026 Tabsdata Inc.
//

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use td_common::id::{Id, id_time};

use crate::base::error::upgrade::UpgradeError;

pub const LOCK_ENTRY: &str = "lock";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeLock {
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<DateTime<Utc>>,
}

impl UpgradeLock {
    pub fn issue(target: &str) -> Self {
        let id: Id = td_common::id::id();
        Self {
            target: target.to_owned(),
            id: Some(id.to_string()),
            timestamp: Some(id_time(&id).into()),
        }
    }

    pub fn held(&self) -> bool {
        self.id.is_some() && self.timestamp.is_some()
    }

    pub fn holds(&self, other: &Self) -> bool {
        self.held() && other.held() && self.id == other.id && self.timestamp == other.timestamp
    }

    pub fn unlocked(&self) -> Self {
        Self {
            target: self.target.clone(),
            id: None,
            timestamp: None,
        }
    }
}

pub fn deserialize_lock(yaml: &str) -> Result<Option<UpgradeLock>, UpgradeError> {
    if yaml.trim().is_empty() {
        return Ok(None);
    }
    serde_yaml::from_str(yaml)
        .map(Some)
        .map_err(|error| UpgradeError::LockParseFailed(error.to_string()))
}

pub fn serialize_lock(lock: &UpgradeLock) -> Result<String, UpgradeError> {
    serde_yaml::to_string(lock).map_err(|error| UpgradeError::LockRenderFailed(error.to_string()))
}

#[cfg(test)]
mod tests;
