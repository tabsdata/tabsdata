//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeConnectionError {
    #[error("A connection upsert must include the dehydrated connection.")]
    MissingDocument = 0,
    #[error("The collection has no connection configured.")]
    NotConfigured = 1000,
    #[error("Could not derive the kubernetes name for the connection: {cause}.")]
    NameDerivationFailed { cause: String } = 5000,
    #[error("Could not serialize the connection document: {cause}.")]
    DocumentSerializationFailed { cause: String } = 5002,
    #[error("Stored secret '{key}' is not valid UTF-8: {cause}.")]
    SecretNotUtf8 { key: String, cause: String } = 5004,
    #[error("Connection document configmap is missing the '{key}' entry.")]
    DocumentKeyMissing { key: String } = 5005,
    #[error("Corrupt connection document: {cause}.")]
    DocumentCorrupt { cause: String } = 5006,
}
