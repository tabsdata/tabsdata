//
// Copyright 2026 Tabs Data Inc.
//

//! Object-store IO: resolve a [`Location`] (`{ uri, configs }`) to a backend
//! store and read/write whole objects. The single place that touches the
//! `object_store` crate.
//!
//! Config keys are the friendly aliases (`region`, `access_key_id`,
//! `account_name`, `service_account_key`, ...) normalized to `object_store`'s
//! canonical `aws_*` / `azure_*` / `google_*` keys; the URI and every config
//! value pass through `env:` / `val:` resolution first.
//!
//! Phase 1 reads/writes each object in one shot (`get().bytes()` / `put`). True
//! byte-bounded streaming + multipart upload is a documented refinement (the
//! convert pipeline already bounds memory per-file via staging on large files).

use crate::transporter2::error::{Result, TransporterError};
use crate::transporter2::spec::Location;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, parse_url_opts, path::Path as OsPath};
use serde_yaml::{Mapping, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufRead, BufReader};
use tokio_util::io::StreamReader;
use url::Url;

/// Metadata for one stored object (the subset `find` needs).
#[derive(Debug, Clone)]
pub struct FileMeta {
    /// Backend-native key (e.g. `dir/file.parquet`).
    pub path: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
}

// --------------------------------------------------------------------------
// env: / val: resolution
// --------------------------------------------------------------------------

const ENV_PREFIX: &str = "env:";
const VAL_PREFIX: &str = "val:";

/// Resolve a value per the `val:` / `env:` convention: `val:<v>` → literal `<v>`
/// (checked first), `env:<VAR>` → that env var (error if unset), else verbatim.
pub fn resolve_value(value: &str) -> Result<String> {
    if let Some(literal) = value.strip_prefix(VAL_PREFIX) {
        return Ok(literal.to_string());
    }
    if let Some(var) = value.strip_prefix(ENV_PREFIX) {
        return std::env::var(var).map_err(|_| TransporterError::EnvVarNotFound(var.to_string()));
    }
    Ok(value.to_string())
}

// --------------------------------------------------------------------------
// Cloud kind + friendly-key mapping (mirrors transporter2)
// --------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CloudKind {
    S3,
    Azure,
    Gcs,
    Local,
}

impl CloudKind {
    fn label(self) -> &'static str {
        match self {
            CloudKind::S3 => "s3",
            CloudKind::Azure => "azure",
            CloudKind::Gcs => "gcs",
            CloudKind::Local => "file",
        }
    }

    fn of_scheme(scheme: &str) -> Option<CloudKind> {
        match scheme {
            "s3" => Some(CloudKind::S3),
            "az" | "azure" | "abfs" | "abfss" | "adl" => Some(CloudKind::Azure),
            "gs" | "gcs" => Some(CloudKind::Gcs),
            "file" => Some(CloudKind::Local),
            _ => None,
        }
    }
}

/// Friendly config alias → canonical `object_store` key, per cloud.
fn friendly_keys(kind: CloudKind) -> &'static [(&'static str, &'static str)] {
    match kind {
        CloudKind::S3 => &[
            ("access_key_id", "aws_access_key_id"),
            ("secret_access_key", "aws_secret_access_key"),
            ("session_token", "aws_session_token"),
            ("region", "aws_region"),
            ("endpoint", "aws_endpoint"),
            (
                "virtual_hosted_style_request",
                "aws_virtual_hosted_style_request",
            ),
            ("allow_http", "aws_allow_http"),
            ("anonymous", "aws_skip_signature"),
            ("skip_signature", "aws_skip_signature"),
        ],
        CloudKind::Azure => &[
            ("account_name", "azure_storage_account_name"),
            ("account_key", "azure_storage_account_key"),
            ("sas_token", "azure_storage_sas_token"),
            ("endpoint", "azure_storage_endpoint"),
            ("allow_http", "azure_allow_http"),
        ],
        CloudKind::Gcs => &[
            ("service_account_key", "google_service_account_key"),
            ("service_account", "google_service_account"),
            ("allow_http", "google_allow_http"),
        ],
        CloudKind::Local => &[],
    }
}

fn canonical_prefix(kind: CloudKind) -> Option<&'static str> {
    match kind {
        CloudKind::S3 => Some("aws_"),
        CloudKind::Azure => Some("azure_"),
        CloudKind::Gcs => Some("google_"),
        CloudKind::Local => None,
    }
}

fn canonical_key(kind: CloudKind, key: &str) -> Result<String> {
    if let Some((_, canonical)) = friendly_keys(kind).iter().find(|(f, _)| *f == key) {
        return Ok((*canonical).to_string());
    }
    if let Some(prefix) = canonical_prefix(kind)
        && key.starts_with(prefix)
    {
        return Ok(key.to_string());
    }
    Err(TransporterError::UnknownConfigKey(
        kind.label().to_string(),
        key.to_string(),
    ))
}

/// Normalize + `env:`/`val:`-resolve every config value to canonical keys.
fn normalize_configs(
    kind: CloudKind,
    configs: &HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let mut out = HashMap::with_capacity(configs.len());
    for (key, value) in configs {
        out.insert(canonical_key(kind, key)?, resolve_value(value)?);
    }
    Ok(out)
}

/// Early-fail validation of the minimal required keys per cloud.
fn validate_required(kind: CloudKind, canonical: &HashMap<String, String>) -> Result<()> {
    let scheme = kind.label().to_string();
    let missing = |what: &str| {
        Err(TransporterError::MissingConfigKeys(
            scheme.clone(),
            what.to_string(),
        ))
    };
    match kind {
        CloudKind::S3 => {
            let anonymous = canonical
                .get("aws_skip_signature")
                .map(|v| v == "true")
                .unwrap_or(false);
            if !(anonymous
                || canonical.contains_key("aws_access_key_id")
                    && canonical.contains_key("aws_secret_access_key"))
            {
                return missing("access_key_id + secret_access_key (or anonymous)");
            }
            if !canonical.contains_key("aws_region") && !canonical.contains_key("aws_endpoint") {
                return missing("region or endpoint");
            }
        }
        CloudKind::Azure => {
            if !canonical.contains_key("azure_storage_account_name") {
                return missing("account_name");
            }
            if !canonical.contains_key("azure_storage_account_key")
                && !canonical.contains_key("azure_storage_sas_token")
            {
                return missing("account_key or sas_token");
            }
        }
        CloudKind::Gcs => {
            if !canonical.contains_key("google_service_account_key")
                && !canonical.contains_key("google_service_account")
            {
                return missing("service_account_key or service_account");
            }
        }
        CloudKind::Local => {}
    }
    Ok(())
}

/// `object_store` 0.13 mis-parses `az://container/key` (container → host); the
/// real key is the URL path minus the leading slash.
fn tweak_path(url: &Url, path: &OsPath) -> String {
    if url.scheme() == "az" {
        url.path().strip_prefix('/').unwrap_or("").to_string()
    } else {
        path.to_string()
    }
}

// --------------------------------------------------------------------------
// Store
// --------------------------------------------------------------------------

/// A resolved, ready-to-use object store plus the backend key to operate on.
/// Cheap to [`Clone`] (the backend is an `Arc`), so retry loops can rebuild a
/// job each attempt.
#[derive(Clone)]
pub struct Store {
    store: Arc<dyn ObjectStore>,
    path: String,
    object_path: OsPath,
    uri: String,
}

/// Resolve a [`Location`] to a [`Store`]: `env:`/`val:`-resolve the uri + every
/// config value, map friendly keys, validate, and build the backend.
pub fn resolve(loc: &Location) -> Result<Store> {
    let uri = resolve_value(&loc.uri)?;
    let url = Url::parse(&uri).map_err(|e| TransporterError::InvalidUri(uri.clone(), e))?;
    let kind = CloudKind::of_scheme(url.scheme())
        .ok_or_else(|| TransporterError::UnsupportedScheme(url.scheme().to_string()))?;
    let canonical = normalize_configs(kind, &loc.configs)?;
    validate_required(kind, &canonical)?;
    let (store, path) = parse_url_opts(&url, canonical)?;
    let path = tweak_path(&url, &path);
    let object_path = OsPath::parse(&path)?;
    tracing::debug!(
        uri = %uri,
        path = %path,
        object_path = %object_path,
        "shuttle.transporter2: resolved store path"
    );
    Ok(Store {
        store: Arc::from(store),
        path,
        object_path,
        uri,
    })
}

impl Store {
    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// `(size, last_modified)` of the object.
    pub async fn head(&self) -> Result<(u64, DateTime<Utc>)> {
        let meta = self.store.head(&self.object_path).await?;
        Ok((meta.size, meta.last_modified))
    }

    /// Read the whole object into memory.
    pub async fn read_all(&self) -> Result<Bytes> {
        let got = self.store.get(&self.object_path).await?;
        Ok(got.bytes().await?)
    }

    /// Write the whole object (overwrites).
    pub async fn write_all(&self, data: Bytes) -> Result<u64> {
        let len = data.len() as u64;
        self.store
            .put(&self.object_path, PutPayload::from_bytes(data))
            .await?;
        Ok(len)
    }

    pub async fn delete(&self) -> Result<()> {
        self.store.delete(&self.object_path).await?;
        Ok(())
    }

    /// A streaming reader over the object's bytes (no whole-object buffering).
    pub async fn open_read_stream(&self) -> Result<impl AsyncBufRead + Send + Unpin + 'static> {
        let got = self.store.get(&self.object_path).await?;
        let stream = got.into_stream().map_err(std::io::Error::other);
        Ok(BufReader::new(StreamReader::new(stream)))
    }

    /// A streaming, multipart writer for the object. Bytes are uploaded in parts
    /// as they are written; the object becomes visible only on `shutdown`
    /// (atomic — a partial object is never exposed).
    pub fn open_write(&self) -> BufWriter {
        BufWriter::new(self.store.clone(), self.object_path.clone())
    }

    /// List objects one level under `dir` (the directory the wildcard finder
    /// scans). The whole listing is materialized.
    pub async fn list(&self, dir: &str) -> Result<Vec<FileMeta>> {
        let prefix = OsPath::parse(dir)?;
        let mut stream = self.store.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            out.push(FileMeta {
                path: meta.location.to_string(),
                size: meta.size,
                last_modified: meta.last_modified,
            });
        }
        Ok(out)
    }
}

/// Recursively `env:`/`val:`-resolve every string scalar in a parsed request:
/// load the YAML into a generic [`Value`], resolve here (early error on an unset
/// `env:`), then deserialize into the typed request. Non-string scalars pass
/// through. Used by the CLI's request loader.
pub fn resolve_yaml(value: Value) -> Result<Value> {
    match value {
        Value::String(s) => Ok(Value::String(resolve_value(&s)?)),
        Value::Sequence(items) => Ok(Value::Sequence(
            items.into_iter().map(resolve_yaml).collect::<Result<_>>()?,
        )),
        Value::Mapping(map) => {
            let mut out = Mapping::new();
            for (k, v) in map {
                out.insert(resolve_yaml(k)?, resolve_yaml(v)?);
            }
            Ok(Value::Mapping(out))
        }
        other => Ok(other),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn resolve_value_handles_val_env_verbatim() {
        assert_eq!(resolve_value("val:secret").unwrap(), "secret");
        assert_eq!(resolve_value("plain").unwrap(), "plain");
        // val: wins even if it looks like env:
        assert_eq!(resolve_value("val:env:X").unwrap(), "env:X");
        assert!(resolve_value("env:__TRANSPORTER2_DEFINITELY_UNSET__").is_err());
    }

    #[test]
    fn canonical_key_maps_friendly_and_passes_through_native() {
        assert_eq!(
            canonical_key(CloudKind::S3, "access_key_id").unwrap(),
            "aws_access_key_id"
        );
        assert_eq!(
            canonical_key(CloudKind::S3, "region").unwrap(),
            "aws_region"
        );
        assert_eq!(
            canonical_key(CloudKind::S3, "aws_endpoint").unwrap(),
            "aws_endpoint"
        );
        assert_eq!(
            canonical_key(CloudKind::Azure, "account_name").unwrap(),
            "azure_storage_account_name"
        );
        assert_eq!(
            canonical_key(CloudKind::Gcs, "service_account_key").unwrap(),
            "google_service_account_key"
        );
        assert!(canonical_key(CloudKind::S3, "bogus").is_err());
    }

    #[test]
    fn validate_required_s3_needs_creds_and_region() {
        let mut c = HashMap::new();
        assert!(validate_required(CloudKind::S3, &c).is_err());
        c.insert("aws_access_key_id".into(), "k".into());
        c.insert("aws_secret_access_key".into(), "s".into());
        assert!(validate_required(CloudKind::S3, &c).is_err()); // no region/endpoint
        c.insert("aws_region".into(), "us-east-1".into());
        assert!(validate_required(CloudKind::S3, &c).is_ok());
    }

    #[test]
    fn resolve_local_file_uri() {
        let loc = Location {
            uri: "file:///tmp/x.parquet".into(),
            configs: HashMap::new(),
        };
        let store = resolve(&loc).unwrap();
        assert!(store.path().ends_with("x.parquet"));
    }
}
