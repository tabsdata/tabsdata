//
// Copyright 2026 Tabs Data Inc.
//

//! Error type for the transporter pipeline. A failure whose class is unknown is
//! treated as **terminal**, not transient (module invariant 8) — that policy
//! lives in the task layer; this enum just carries the cause.

use crate::transporter2::spec::Format;

#[derive(Debug, thiserror::Error)]
pub enum TransporterError {
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("object_store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("invalid uri '{0}': {1}")]
    InvalidUri(String, url::ParseError),

    #[error("unsupported uri scheme: '{0}'")]
    UnsupportedScheme(String),

    #[error("unknown config key for {0}: '{1}'")]
    UnknownConfigKey(String, String),

    #[error("missing required config for {0}: {1}")]
    MissingConfigKeys(String, String),

    #[error("environment variable not set: '{0}'")]
    EnvVarNotFound(String),

    #[error("source uri has no file name (a wildcard is required): '{0}'")]
    NoFileName(String),

    #[error("invalid lastmod cursor: {0}")]
    Cursor(String),

    #[error("request file not found: '{0}'")]
    RequestFileNotFound(String),

    #[error("response file already exists: '{0}'")]
    ResponseFileExists(String),

    #[error("could not read request '{0}': {1}")]
    CouldNotReadRequest(String, serde_yaml::Error),

    #[error("could not write response '{0}': {1}")]
    CouldNotWriteResponse(String, serde_yaml::Error),

    #[error("invalid --{0} value: '{1}'")]
    InvalidOption(String, String),

    /// `write_as` is a read-only format (e.g. `Text`).
    #[error("format {0:?} is read-only and cannot be a conversion target")]
    UnsupportedTarget(Format),

    /// A declared codec does not match the bytes (magic-byte sanity guard).
    #[error("codec mismatch: declared {declared}, but the data is not {declared} ({detail})")]
    CodecMismatch { declared: String, detail: String },

    #[error("{0}")]
    Other(String),
}

impl TransporterError {
    pub fn other(msg: impl Into<String>) -> Self {
        TransporterError::Other(msg.into())
    }
}

pub type Result<T> = std::result::Result<T, TransporterError>;
