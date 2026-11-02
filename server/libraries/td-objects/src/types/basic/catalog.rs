//
// Copyright 2026 Tabsdata Inc.
//

use serde::{Deserialize, Serialize};
use ta_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Copy, Hash)]
pub enum ProbeKind {
    #[typed_enum(rename = "check")]
    Check,
    #[typed_enum(rename = "metadata")]
    Metadata,
    #[typed_enum(rename = "sample")]
    Sample,
}

/// `queued` and `running` are the active states the dedupe index coalesces
/// on; the others are terminal.
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Copy, Hash)]
pub enum ProbeStatus {
    #[typed_enum(rename = "queued")]
    Queued,
    #[typed_enum(rename = "running")]
    Running,
    #[typed_enum(rename = "succeeded")]
    Succeeded,
    #[typed_enum(rename = "failed")]
    Failed,
    #[typed_enum(rename = "timeout")]
    Timeout,
    #[typed_enum(rename = "cancelled")]
    Cancelled,
}

impl ProbeStatus {
    pub const ACTIVE: [ProbeStatus; 2] = [ProbeStatus::Queued, ProbeStatus::Running];

    pub fn is_terminal(&self) -> bool {
        !Self::ACTIVE.contains(self)
    }
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Copy)]
pub enum CheckStatus {
    #[typed_enum(rename = "unknown")]
    Unknown,
    #[typed_enum(rename = "ok")]
    Ok,
    #[typed_enum(rename = "error")]
    Error,
    #[typed_enum(rename = "timeout")]
    Timeout,
    #[typed_enum(rename = "unsupported")]
    Unsupported,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Copy)]
pub enum MetadataStatus {
    #[typed_enum(rename = "none")]
    None,
    #[typed_enum(rename = "ok")]
    Ok,
    #[typed_enum(rename = "error")]
    Error,
    #[typed_enum(rename = "timeout")]
    Timeout,
    #[typed_enum(rename = "unsupported")]
    Unsupported,
}

/// The `schema_fingerprint` of a metadata probe (sha256 hex).
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min_len = 1, max_len = 128)]
pub struct CatalogSchemaHash(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(max_len = 2048)]
pub struct CheckError(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(max_len = 2048)]
pub struct MetadataError(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min = 0, default = 0)]
pub struct EntityCount(i32);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct CatalogTruncated(bool);

/// The catalog's text revision: a value of one sequence, set anew whenever the
/// texts of the catalog (its collection's name and description, its kind or
/// its entities) change, 0 while it has no catalog row.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd)]
#[typed(min = 0, default = 0)]
pub struct TextRevision(i64);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(min_len = 1, max_len = 4096)]
pub struct EntityFqn(String);

/// The explorer's entity level: `table`, `view`, `object`, `file`, ...
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min_len = 1, max_len = 100)]
pub struct CatalogEntityKind(String);

/// ` `-joined lowercase field names.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = "")]
pub struct FieldNames(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min = 0, default = 0)]
pub struct FieldCount(i32);

/// Whether an entity listing carries each entity's document.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct IncludeFields(bool);

/// The rows a sample probe reads from each entity.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd)]
#[typed(min = 1, max = 100, default = 20)]
pub struct SampleRows(i32);

/// How long a probe read waits for the probe to end, in seconds.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min = 0, default = 0)]
pub struct ProbeWaitSecs(i64);

/// `sha256(connection_id, kind, params)`: active probes with the same key
/// coalesce.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min_len = 64, max_len = 64)]
pub struct ProbeDedupeKey(String);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntityDoc(pub serde_json::Value);

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProbeParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
}

/// The runner scrubs the connection's secret values; the server holds
/// `code` and `message` to [`ProbeError::MAX_BYTES`] whatever it is sent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbeError {
    pub code: String,
    pub message: String,
}

impl ProbeError {
    pub const MAX_BYTES: usize = 2048;

    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: Self::bound(code.into()),
            message: Self::bound(message.into()),
        }
    }

    pub fn bounded(self) -> Self {
        Self::new(self.code, self.message)
    }

    pub fn bound(mut text: String) -> String {
        text.truncate(text.floor_char_boundary(Self::MAX_BYTES));
        text
    }
}

/// A kind's result as the probe reported it; a metadata result keeps its
/// summary only, the entities go to the catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProbeResult(pub serde_json::Value);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_probe_error_is_cut_to_the_bound_on_a_char_boundary() {
        let error = ProbeError::new("c".repeat(5000), "é".repeat(3000));
        assert_eq!(error.code.len(), ProbeError::MAX_BYTES);
        assert!(error.message.len() <= ProbeError::MAX_BYTES);
        assert!(error.message.chars().all(|c| c == 'é'));
        assert!(CheckError::try_from(error.message.as_str()).is_ok());
        assert!(MetadataError::try_from("x".repeat(ProbeError::MAX_BYTES + 1)).is_err());
        assert_eq!(ProbeError::bound("short".to_owned()), "short");
    }
}
