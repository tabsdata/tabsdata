//
// Copyright 2026 Tabsdata Inc.
//

//! serde helpers shared by the DTO layer.

use serde::{Deserialize, Deserializer};

/// Deserializer for tri-state `Option<Option<T>>` update fields, paired with
/// `#[serde(default)]`: an absent field defaults to `None` (leave unchanged),
/// a present `null` yields `Some(None)` (clear -> NULL), and a present value
/// `Some(Some(v))` (set). `#[ta_objects::Dto]` auto-injects this onto every
/// `Option<Option<T>>` field, so the three states are distinguishable over JSON.
pub fn double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}
