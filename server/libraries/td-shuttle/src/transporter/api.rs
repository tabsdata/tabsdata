//
// Copyright 2024 Tabs Data Inc.
//

use crate::transporter::error::TransporterError;
use chrono::{DateTime, Utc};
use getset::Getters;
use polars::prelude::CsvParseOptions;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransporterRequest {
    ImportV1(ImportRequest),
    CopyV1(CopyRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransporterReport {
    ImportV1(ImportReport),
    CopyV1(CopyReport),
    ErrorV1(ErrorReport),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportRequest {
    source: ImportSource,
    format: ImportFormat,
    target: ImportTarget,
    parallelism: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportSource {
    location: Location<WildcardUrl>,
    last_modified: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportTarget {
    location: Location<TokenUrl>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct CopyRequest {
    pub source_target_pairs: Vec<(Location<Url>, Location<Url>)>,
    pub parallelism: Option<usize>,
}

impl CopyRequest {
    pub fn new(
        source_target_pairs: Vec<(Location<Url>, Location<Url>)>,
        parallelism: Option<usize>,
    ) -> Self {
        Self {
            source_target_pairs,
            parallelism,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Location<L> {
    LocalFile { url: L },
    S3 { url: L, configs: AwsConfigs },
    Azure { url: L, configs: AzureConfigs },
}

impl<L> Location<L> {
    pub fn buffer_size(&self) -> usize {
        match self {
            Location::LocalFile { .. } => 1024 * 1024,
            Location::S3 { .. } => 1024 * 1024 * 5,
            Location::Azure { .. } => 1024 * 1024 * 4,
        }
    }
}

pub trait AsUrl {
    fn as_url(&self) -> Url;
}

impl AsUrl for Url {
    fn as_url(&self) -> Url {
        self.clone()
    }
}

impl AsUrl for WildcardUrl {
    fn as_url(&self) -> Url {
        self.0.clone()
    }
}
impl AsUrl for TokenUrl {
    fn as_url(&self) -> Url {
        self.0.clone()
    }
}

impl<L: AsUrl> Location<L> {
    pub fn url(&self) -> Url {
        match self {
            Location::LocalFile { url } => url.as_url(),
            Location::S3 { url, .. } => url.as_url(),
            Location::Azure { url, .. } => url.as_url(),
        }
    }

    pub fn cloud_configs(&self) -> HashMap<String, String> {
        match self {
            Location::LocalFile { .. } => HashMap::new(),
            Location::S3 { configs, .. } => {
                // keys defined at object_store::aws::builder::AmazonS3ConfigKey
                let mut options = HashMap::new();
                options.insert(
                    "aws_access_key_id".into(),
                    configs.access_key.value().unwrap(),
                );
                options.insert(
                    "aws_secret_access_key".into(),
                    configs.secret_key.value().unwrap(),
                );
                if let Some(region) = &configs.region {
                    options.insert("aws_region".into(), region.value().unwrap());
                }
                if let Some(configs) = &configs.extra_configs {
                    options.extend(configs.clone());
                }
                options
            }
            Location::Azure { configs, .. } => {
                // keys defined at object_store::azure::builder::AzureConfigKey
                let mut options = HashMap::new();
                options.insert(
                    "azure_storage_account_name".into(),
                    configs.account_name.value().unwrap(),
                );
                options.insert(
                    "azure_storage_account_key".into(),
                    configs.account_key.value().unwrap(),
                );
                if let Some(configs) = &configs.extra_configs {
                    options.extend(configs.clone());
                }
                options
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WildcardUrl(Url);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUrl(Url);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Literal(String),
    Env(String),
}

impl Value {
    fn unquote_if_quoted(str: &str) -> &str {
        str.strip_prefix('\'')
            .and_then(|n| n.strip_suffix('\''))
            .unwrap_or(str)
    }

    pub fn value(&self) -> Result<String, TransporterError> {
        match self {
            Value::Literal(value) => Ok(value.clone()),
            Value::Env(name) => std::env::var(Self::unquote_if_quoted(name))
                .map_err(|_| TransporterError::EnvironmentVariableNotFound(name.to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsConfigs {
    pub access_key: Value,
    pub secret_key: Value,
    pub region: Option<Value>,
    pub extra_configs: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureConfigs {
    pub account_name: Value,
    pub account_key: Value,
    pub extra_configs: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ImportFormat {
    Csv(ImportCsvOptions),
    Json,
    Log,
    Parquet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportCsvOptions {
    pub parse_options: Option<CsvParseOptions>,
    pub has_header: bool,
    pub skip_rows: usize,
    pub skip_rows_after_header: usize,
    pub raise_if_empty: bool,
    pub ignore_errors: bool,
}

impl Default for ImportCsvOptions {
    fn default() -> Self {
        Self {
            parse_options: None,
            has_header: true,
            skip_rows: 0,
            skip_rows_after_header: 0,
            raise_if_empty: true,
            ignore_errors: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileImportReport {
    idx: usize,
    from: Url,
    size: u64,
    rows: usize,
    last_modified: DateTime<Utc>,
    to: Url,
    imported_at: DateTime<Utc>,
    import_millis: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportReport {
    files: Vec<FileImportReport>,
}

impl ImportReport {
    pub fn new(file_reports: Vec<FileImportReport>) -> Self {
        Self {
            files: file_reports,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCopyReport {
    pub idx: usize,
    pub from: Url,
    pub size: usize,
    pub to: Url,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct CopyReport {
    files: Vec<FileCopyReport>,
}

impl CopyReport {
    pub fn new(file_reports: Vec<FileCopyReport>) -> Self {
        Self {
            files: file_reports,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorReport {
    message: String,
}

impl ErrorReport {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

fn write_yaml<T: Serialize>(v: &T, comment: &str) -> String {
    let yaml = serde_yaml::to_string(v).unwrap();
    format!(
        "---------------------------------\n{}\n\n{}\n",
        comment, yaml
    )
}

impl TransporterReport {
    pub fn yaml_samples() -> String {
        let mut samples = String::new();
        samples.push_str(&TransporterReport::sample_copy().sample_yaml("# Export Report"));
        samples
    }

    fn sample_yaml(&self, comment: &str) -> String {
        write_yaml(self, comment)
    }

    fn sample_copy() -> Self {
        TransporterReport::CopyV1(CopyReport {
            files: vec![
                FileCopyReport {
                    idx: 0,
                    from: Url::parse("file:///import-dir/file0.parquet").unwrap(),
                    size: 1024,
                    to: Url::parse("file:///export-dir/file0.parquet").unwrap(),
                    started_at: DateTime::from(
                        DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap(),
                    ),
                    ended_at: DateTime::from(
                        DateTime::parse_from_rfc3339("2024-01-01T00:10:00Z").unwrap(),
                    ),
                },
                FileCopyReport {
                    idx: 0,
                    from: Url::parse("file:///import-dir/file1.parquet").unwrap(),
                    size: 1024,
                    to: Url::parse("file:///export-dir/file1.parquet").unwrap(),
                    started_at: DateTime::from(
                        DateTime::parse_from_rfc3339("2024-01-01T00:10:01Z").unwrap(),
                    ),
                    ended_at: DateTime::from(
                        DateTime::parse_from_rfc3339("2024-01-01T00:20:00Z").unwrap(),
                    ),
                },
            ],
        })
    }
}
impl TransporterRequest {
    pub fn yaml_samples() -> String {
        let mut samples = String::new();
        samples.push_str(&Self::copy_local_to_local().sample_yaml("# Copy local to local"));
        samples.push_str(
            &Self::copy_local_to_s3_literal()
                .sample_yaml("# Copy local to S3 with literal credentials"),
        );
        samples.push_str(
            &Self::copy_local_to_s3_env().sample_yaml("# Copy local to S3 with env credentials"),
        );
        samples.push_str(
            &Self::copy_local_to_azure_literal()
                .sample_yaml("# Copy local to Azure with literal credentials"),
        );
        samples.push_str(
            &Self::copy_local_to_azure_env()
                .sample_yaml("# Copy local to Azure with env credentials"),
        );
        samples
    }

    fn sample_yaml(&self, comment: &str) -> String {
        write_yaml(self, comment)
    }

    fn copy_local_to_local() -> TransporterRequest {
        TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![
                (
                    Location::LocalFile {
                        url: Url::parse("file:///import-dir/file0.parquet").unwrap(),
                    },
                    Location::LocalFile {
                        url: Url::parse("file:///export-dir/file0.parquet").unwrap(),
                    },
                ),
                (
                    Location::LocalFile {
                        url: Url::parse("file:///import-dir/file1.parquet").unwrap(),
                    },
                    Location::LocalFile {
                        url: Url::parse("file:///export-dir/file1.parquet").unwrap(),
                    },
                ),
            ],
            parallelism: None,
        })
    }

    fn copy_local_to_s3_literal() -> TransporterRequest {
        TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![
                (
                    Location::LocalFile {
                        url: Url::parse("file:///import-dir/file0.parquet").unwrap(),
                    },
                    Location::S3 {
                        url: Url::parse("s3:/bucket/import-dir/file0.parquet").unwrap(),
                        configs: AwsConfigs {
                            access_key: Value::Literal("access_key".into()),
                            secret_key: Value::Literal("secret_key".into()),
                            region: Some(Value::Literal("region".into())),
                            extra_configs: Some(HashMap::new()),
                        },
                    },
                ),
                (
                    Location::LocalFile {
                        url: Url::parse("file:///export-dir/file1.parquet").unwrap(),
                    },
                    Location::S3 {
                        url: Url::parse("s3:/bucket/import-dir/file1.parquet").unwrap(),
                        configs: AwsConfigs {
                            access_key: Value::Literal("access_key".into()),
                            secret_key: Value::Literal("secret_key".into()),
                            region: None,
                            extra_configs: None,
                        },
                    },
                ),
            ],
            parallelism: None,
        })
    }

    fn copy_local_to_s3_env() -> TransporterRequest {
        TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![
                (
                    Location::LocalFile {
                        url: Url::parse("file:///import-dir/file0.parquet").unwrap(),
                    },
                    Location::S3 {
                        url: Url::parse("s3:/bucket/import-dir/file0.parquet").unwrap(),
                        configs: AwsConfigs {
                            access_key: Value::Env("IMPORT_AWS_ACCESS_KEY".into()),
                            secret_key: Value::Env("IMPORT_AWS_SECRET_KEY".into()),
                            region: Some(Value::Env("IMPORT_AWS_REGION".into())),
                            extra_configs: Some(HashMap::new()),
                        },
                    },
                ),
                (
                    Location::LocalFile {
                        url: Url::parse("file:///export-dir/file1.parquet").unwrap(),
                    },
                    Location::S3 {
                        url: Url::parse("s3:/bucket/import-dir/file1.parquet").unwrap(),
                        configs: AwsConfigs {
                            access_key: Value::Env("IMPORT_AWS_ACCESS_KEY".into()),
                            secret_key: Value::Env("IMPORT_AWS_SECRET_KEY".into()),
                            region: None,
                            extra_configs: None,
                        },
                    },
                ),
            ],
            parallelism: None,
        })
    }

    fn copy_local_to_azure_literal() -> TransporterRequest {
        TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![
                (
                    Location::LocalFile {
                        url: Url::parse("file:///import-dir/file0.csv").unwrap(),
                    },
                    Location::Azure {
                        url: Url::parse("az://container/import-dir/file0.csv").unwrap(),
                        configs: AzureConfigs {
                            account_name: Value::Literal("account_name".into()),
                            account_key: Value::Literal("account_key".into()),
                            extra_configs: Some(HashMap::new()),
                        },
                    },
                ),
                (
                    Location::LocalFile {
                        url: Url::parse("file:///import-dir/file1.csv").unwrap(),
                    },
                    Location::Azure {
                        url: Url::parse("az://container/import-dir/file1.csv").unwrap(),
                        configs: AzureConfigs {
                            account_name: Value::Literal("account_name".into()),
                            account_key: Value::Literal("account_key".into()),
                            extra_configs: None,
                        },
                    },
                ),
            ],
            parallelism: None,
        })
    }

    fn copy_local_to_azure_env() -> TransporterRequest {
        TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![
                (
                    Location::LocalFile {
                        url: Url::parse("file:///import-dir/file0.csv").unwrap(),
                    },
                    Location::Azure {
                        url: Url::parse("az://container/import-dir/file0.csv").unwrap(),
                        configs: AzureConfigs {
                            account_name: Value::Env("IMPORT_AZURE_ACCOUNT_NAME".into()),
                            account_key: Value::Env("IMPORT_AZURE_ACCOUNT_KEY".into()),
                            extra_configs: Some(HashMap::new()),
                        },
                    },
                ),
                (
                    Location::LocalFile {
                        url: Url::parse("file:///import-dir/file1.csv").unwrap(),
                    },
                    Location::Azure {
                        url: Url::parse("az://container/import-dir/file1.csv").unwrap(),
                        configs: AzureConfigs {
                            account_name: Value::Env("IMPORT_AZURE_ACCOUNT_NAME".into()),
                            account_key: Value::Env("IMPORT_AZURE_ACCOUNT_KEY".into()),
                            extra_configs: None,
                        },
                    },
                ),
            ],
            parallelism: None,
        })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_value() {
        let value = super::Value::Literal("value".into());
        assert_eq!(value.value().unwrap(), "value");

        // Setting env vars is not thread-safe; use with care.
        unsafe {
            std::env::set_var("TEST_VALUE_ENV_VAR", "env_value");
        }

        let value = super::Value::Env("TEST_VALUE_ENV_VAR".into());
        assert_eq!(value.value().unwrap(), "env_value");

        let value = super::Value::Env("'TEST_VALUE_ENV_VAR'".into());
        assert_eq!(value.value().unwrap(), "env_value");

        // Setting env vars is not thread-safe; use with care.
        unsafe {
            std::env::remove_var("TEST_VALUE_ENV_VAR");
        }
    }

    #[test]
    fn test_request_yaml_samples() {
        super::TransporterRequest::yaml_samples();
    }

    #[test]
    fn test_response_yaml_samples() {
        super::TransporterReport::yaml_samples();
    }
}
