//
// Copyright 2025. Tabs Data Inc.
//

use crate::transporter::api::{
    FileImportReport, FileImportReportBuilder, ImportFormat, ImportRequest, ImportSource,
};
use crate::transporter::args::{
    Format, ImporterLogReadOptions, ImporterNdJsonReadOptions, ImporterOptions,
    ImporterOptionsBuilder, ImporterParquetReadOptions, ToFormat,
};
use crate::transporter::error::TransporterError;
use crate::transporter::logic::to_file_to_import_instructions;
use async_trait::async_trait;
use itertools::Itertools;
use object_store::ObjectMeta;
use polars::prelude::{ParquetCompression, ParquetWriteOptions};
use std::path::Path;
use std::sync::Arc;
use url::Url;

#[async_trait]
pub trait Importer {
    async fn import(
        import_request: &ImportRequest,
        files_to_import: Vec<(Url, ObjectMeta)>,
    ) -> Result<Vec<FileImportReport>, TransporterError>;
}

fn extract_from_url<T>(url: &Url, extractor: impl Fn(&Url) -> T) -> T {
    match url.scheme() {
        "file" | "s3" | "az" => extractor(url),
        _ => panic!("Unsupported scheme: {}", url.scheme()),
    }
}

/// Return the base URL of the source location.
///
/// For file:// schemes, it returns file:/// or file:///c:/ depending on running OS
///
/// For s3:// schemes, it returns s3://<bucket>
///
fn base_url(source: &ImportSource) -> Url {
    let url = source.location().url();
    extract_from_url(&url, |url| match url.scheme() {
        "file" => Url::parse(&crate::transporter::args::root_folder()).unwrap(),
        "s3" => Url::parse(&format!("s3://{}", url.authority())).unwrap(),
        "az" => Url::parse(&format!("az://{}", url.authority())).unwrap(),
        _ => unreachable!(),
    })
}

/// Return the base path of the source location.
///
/// This is where files will be searched for.
pub fn base_path(source: &ImportSource) -> String {
    let url = source.location().url();
    extract_from_url(&url, |url| {
        let path = Path::new(url.path());
        path.parent()
            .expect("Url has not path")
            .to_str()
            .expect("Cannot convert path to String")
            .to_string()
    })
}

pub fn file_pattern(source: &ImportSource) -> String {
    let url = source.location().url();
    extract_from_url(&url, |url| {
        let path = Path::new(url.path());
        path.file_name()
            .expect("Url has not file name or pattern")
            .to_str()
            .expect("Cannot convert file name or pattern to String")
            .to_string()
    })
}

impl From<&ImportFormat> for Format {
    fn from(format: &ImportFormat) -> Self {
        match format {
            ImportFormat::Csv(options) => Format::Csv(options.into()),
            ImportFormat::Json => Format::NdJson(ImporterNdJsonReadOptions::default()),
            ImportFormat::Log => Format::Log(ImporterLogReadOptions::default()),
            ImportFormat::Parquet => Format::Parquet(ImporterParquetReadOptions::default()),
        }
    }
}

impl From<&ImportRequest> for ImporterOptions {
    fn from(req: &ImportRequest) -> Self {
        let options = ImporterOptionsBuilder::default()
            .base_url(base_url(req.source())) // not used, for ref only
            .base_path(base_path(req.source())) // not used, for ref only
            .file_pattern(file_pattern(req.source())) // not used, for ref only
            .format(req.format())
            .modified_since(None) // not used, for ref only
            .location_configs(req.source().location().cloud_configs())
            .to(req.target().location().url())
            .to_configs(req.target().location().cloud_configs())
            .to_format(ToFormat::Parquet(ParquetWriteOptions {
                compression: ParquetCompression::Snappy,
                ..Default::default()
            }))
            .parallel(req.parallelism().unwrap_or(4)) // not used, for ref only
            .out(None) // not used, for ref only
            .build()
            .expect("Could not build ImporterOptions");
        options
    }
}

fn create_file_import_instructions(
    import_request: &ImportRequest,
    files_to_import: Vec<(Url, ObjectMeta)>,
) -> Result<Vec<crate::transporter::logic::FileImportInstructions>, TransporterError> {
    let importer_options: Arc<ImporterOptions> = Arc::new(import_request.into());

    files_to_import
        .into_iter()
        .enumerate()
        .map(|(idx, (_url, meta))| (idx, meta))
        .map(crate::transporter::logic::take_files_limit())
        .sorted_by(crate::transporter::logic::file_last_modified_comparator())
        .map(to_file_to_import_instructions(&importer_options))
        .map(|res| {
            res.map_err(|e| TransporterError::CouldNotCreateImportInstructions(e.to_string()))
        })
        .collect()
}

impl From<crate::transporter::logic::FileImportReport> for FileImportReport {
    fn from(file_import_report: crate::transporter::logic::FileImportReport) -> Self {
        FileImportReportBuilder::default()
            .idx(*file_import_report.idx())
            .from(Url::parse(file_import_report.from()).unwrap())
            .size(*file_import_report.size())
            .rows(*file_import_report.rows())
            .last_modified(*file_import_report.last_modified())
            .to(Url::parse(file_import_report.to()).unwrap())
            .imported_at(*file_import_report.imported_at())
            .import_millis(*file_import_report.import_millis())
            .build()
            .expect("Could not build FileImportReport")
    }
}

pub struct FilesImporter;

#[async_trait]
impl Importer for FilesImporter {
    async fn import(
        import_request: &ImportRequest,
        files_to_import: Vec<(Url, ObjectMeta)>,
    ) -> Result<Vec<FileImportReport>, TransporterError> {
        let import_instructions = create_file_import_instructions(import_request, files_to_import)?;

        let import_reports: Vec<_> = tokio::task::spawn_blocking(move || {
            import_instructions
                .into_iter()
                .map(crate::transporter::logic::run_import)
                .collect::<Vec<_>>()
        })
        .await
        .expect("Could not run import files")
        .into_iter()
        .collect::<Result<_, crate::transporter::logic::ImportError>>()
        .expect("Could not import files");

        let import_reports = import_reports
            .into_iter()
            .map(FileImportReport::from)
            .collect::<Vec<_>>();
        Ok(import_reports)
    }
}

#[cfg(test)]
mod tests {
    use crate::transporter::api::{
        AwsConfigs, AzureConfigs, BaseImportUrl, ImportFormat, ImportRequestBuilder,
        ImportSourceBuilder, ImportTargetBuilder, Location, Value, WildcardUrl,
    };
    use crate::transporter::cli::tests::check_envs;
    use crate::transporter::files_importer::{FilesImporter, Importer};
    use chrono::Utc;
    use object_store::ObjectMeta;
    use polars::datatypes::{Float64Chunked, Int64Chunked, PlSmallStr, StringChunked};
    use polars::frame::DataFrame;
    use polars::prelude::{
        Column, IntoLazy, IntoSeries, LazyFrame, PlPath, ScanArgsParquet, SinkOptions, SinkTarget,
    };
    use polars_io::cloud::{CloudOptions, ObjectStorePath};
    use polars_io::prelude::ParquetWriteOptions;
    use polars_io::utils::sync_on_close::SyncOnCloseType;
    use std::collections::HashMap;
    use std::env::var;
    use td_common::id::id;
    use testdir::testdir;
    use url::Url;

    //noinspection DuplicatedCode
    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_from_local_to_aws() {
        const AWS_ACCESS_KEY_ID_ENV: &str = "IMPORT_AWS_ACCESS_KEY";
        const AWS_REGION_ENV: &str = "IMPORT_AWS_REGION";
        const AWS_SECRET_ACCESS_KEY_ENV: &str = "IMPORT_AWS_SECRET_KEY";
        const BASE_URL_ENV: &str = "IMPORT_AWS_BASE_URL";

        if check_envs(
            "test_import_from_local_to_aws",
            vec![
                AWS_ACCESS_KEY_ID_ENV,
                AWS_REGION_ENV,
                AWS_SECRET_ACCESS_KEY_ENV,
                BASE_URL_ENV,
            ],
        ) {
            let col_int = Int64Chunked::from_iter((0i64..1000i64).map(Some))
                .into_series()
                .with_name(PlSmallStr::from("col_int"));
            let col_float = Float64Chunked::from_iter((0..1000).map(|i| Some(i as f64)))
                .into_series()
                .with_name(PlSmallStr::from("col_float"));
            let col_string = StringChunked::from_iter((0..1000).map(|i| Some(i.to_string())))
                .into_series()
                .with_name(PlSmallStr::from("col_string"));
            let df_out = DataFrame::new(vec![
                Column::from(col_int),
                Column::from(col_float),
                Column::from(col_string),
            ])
            .unwrap();
            let lf = df_out.clone().lazy();

            let folder = testdir!();
            let id = id();
            let file_r = format!("{id}.parquet");
            let path_r = folder.join(file_r.clone());
            let url_r = Url::from_file_path(path_r.clone()).unwrap();

            let _ = lf
                .sink_parquet(
                    SinkTarget::Path(PlPath::new(path_r.to_string_lossy().to_string().as_str())),
                    ParquetWriteOptions::default(),
                    None,
                    SinkOptions {
                        sync_on_close: SyncOnCloseType::All,
                        maintain_order: true,
                        mkdir: true,
                    },
                )
                .unwrap()
                .collect();

            let url_w = Url::parse(&format!(
                "{}/unit_test/{}",
                var(BASE_URL_ENV).unwrap(),
                file_r.clone()
            ))
            .unwrap();

            let request = ImportRequestBuilder::default()
                .source(
                    ImportSourceBuilder::default()
                        .location(Location::LocalFile {
                            url: WildcardUrl(url_r.clone()),
                        })
                        .initial_lastmod(None)
                        .lastmod_info(None)
                        .build()
                        .unwrap(),
                )
                .format(ImportFormat::Parquet)
                .target(
                    ImportTargetBuilder::default()
                        .location(Location::S3 {
                            url: BaseImportUrl(url_w.clone()),
                            configs: AwsConfigs {
                                access_key: Value::Env(AWS_ACCESS_KEY_ID_ENV.into()),
                                secret_key: Value::Env(AWS_SECRET_ACCESS_KEY_ENV.into()),
                                region: Some(Value::Env(AWS_REGION_ENV.into())),
                                extra_configs: None,
                            },
                        })
                        .build()
                        .unwrap(),
                )
                .parallelism(None)
                .build()
                .unwrap();

            let meta = ObjectMeta {
                location: ObjectStorePath::from(path_r.clone().to_string_lossy().as_ref()),
                last_modified: Utc::now(),
                size: 1024,
                e_tag: Some(id.to_string()),
                version: Some("v1".to_string()),
            };
            let files: Vec<(Url, ObjectMeta)> = vec![(url_r.clone(), meta)];

            let response = FilesImporter::import(&request, files).await.unwrap();
            assert!(!response.is_empty());
            assert_eq!(response.len(), 1);

            let url_tf = response.first().unwrap().to.clone();

            let mut config = HashMap::new();
            config.insert(
                "AWS_ACCESS_KEY_ID".to_string(),
                var(AWS_ACCESS_KEY_ID_ENV).unwrap(),
            );
            config.insert(
                "AWS_SECRET_ACCESS_KEY".to_string(),
                var(AWS_SECRET_ACCESS_KEY_ENV).unwrap(),
            );
            config.insert("AWS_REGION".to_string(), var(AWS_REGION_ENV).unwrap());
            let cloud_options = CloudOptions::from_untyped_config(url_tf.as_str(), config).unwrap();

            let lf = LazyFrame::scan_parquet(
                PlPath::new(url_tf.to_string().as_str()),
                ScanArgsParquet {
                    cloud_options: Some(cloud_options),
                    ..Default::default()
                },
            )
            .unwrap();

            let df_in = tokio::task::spawn_blocking(move || lf.collect())
                .await
                .unwrap()
                .unwrap();

            assert!(df_out.equals(&df_in));
        }
    }

    //noinspection DuplicatedCode
    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_from_local_to_azure() {
        const AZURE_ACCOUNT_KEY_ENV: &str = "IMPORT_AZURE_ACCOUNT_KEY";
        const AZURE_ACCOUNT_NAME_ENV: &str = "IMPORT_AZURE_ACCOUNT_NAME";
        const BASE_URL_ENV: &str = "IMPORT_AZURE_BASE_URL";

        if check_envs(
            "test_import_from_local_to_azure",
            vec![AZURE_ACCOUNT_KEY_ENV, AZURE_ACCOUNT_NAME_ENV, BASE_URL_ENV],
        ) {
            let col_int = Int64Chunked::from_iter((0i64..1000i64).map(Some))
                .into_series()
                .with_name(PlSmallStr::from("col_int"));
            let col_float = Float64Chunked::from_iter((0..1000).map(|i| Some(i as f64)))
                .into_series()
                .with_name(PlSmallStr::from("col_float"));
            let col_string = StringChunked::from_iter((0..1000).map(|i| Some(i.to_string())))
                .into_series()
                .with_name(PlSmallStr::from("col_string"));
            let df_out = DataFrame::new(vec![
                Column::from(col_int),
                Column::from(col_float),
                Column::from(col_string),
            ])
            .unwrap();
            let lf = df_out.clone().lazy();

            let folder = testdir!();
            let id = id();
            let file_r = format!("{id}.parquet");
            let path_r = folder.join(file_r.clone());
            let url_r = Url::from_file_path(path_r.clone()).unwrap();

            let _ = lf
                .sink_parquet(
                    SinkTarget::Path(PlPath::new(path_r.to_string_lossy().to_string().as_str())),
                    ParquetWriteOptions::default(),
                    None,
                    SinkOptions {
                        sync_on_close: SyncOnCloseType::All,
                        maintain_order: true,
                        mkdir: true,
                    },
                )
                .unwrap()
                .collect();

            let url_w = Url::parse(&format!(
                "{}/unit_test/{}",
                var(BASE_URL_ENV).unwrap(),
                file_r.clone()
            ))
            .unwrap();

            let request = ImportRequestBuilder::default()
                .source(
                    ImportSourceBuilder::default()
                        .location(Location::LocalFile {
                            url: WildcardUrl(url_r.clone()),
                        })
                        .initial_lastmod(None)
                        .lastmod_info(None)
                        .build()
                        .unwrap(),
                )
                .format(ImportFormat::Parquet)
                .target(
                    ImportTargetBuilder::default()
                        .location(Location::Azure {
                            url: BaseImportUrl(url_w.clone()),
                            configs: AzureConfigs {
                                account_name: Value::Env(AZURE_ACCOUNT_NAME_ENV.into()),
                                account_key: Value::Env(AZURE_ACCOUNT_KEY_ENV.into()),
                                extra_configs: None,
                            },
                        })
                        .build()
                        .unwrap(),
                )
                .parallelism(None)
                .build()
                .unwrap();

            let meta = ObjectMeta {
                location: ObjectStorePath::from(path_r.clone().to_string_lossy().as_ref()),
                last_modified: Utc::now(),
                size: 1024,
                e_tag: Some(id.to_string()),
                version: Some("v1".to_string()),
            };
            let files: Vec<(Url, ObjectMeta)> = vec![(url_r.clone(), meta)];

            let response = FilesImporter::import(&request, files).await.unwrap();
            assert!(!response.is_empty());
            assert_eq!(response.len(), 1);

            let url_tf = response.first().unwrap().to.clone();

            let mut config = HashMap::new();
            config.insert(
                "azure_storage_account_name".to_string(),
                var(AZURE_ACCOUNT_NAME_ENV).unwrap(),
            );
            config.insert(
                "azure_storage_account_key".to_string(),
                var(AZURE_ACCOUNT_KEY_ENV).unwrap(),
            );
            let cloud_options = CloudOptions::from_untyped_config(url_tf.as_str(), config).unwrap();

            let lf = LazyFrame::scan_parquet(
                PlPath::new(url_tf.to_string().as_str()),
                ScanArgsParquet {
                    cloud_options: Some(cloud_options),
                    ..Default::default()
                },
            )
            .unwrap();

            let df_in = tokio::task::spawn_blocking(move || lf.collect())
                .await
                .unwrap()
                .unwrap();

            assert!(df_out.equals(&df_in));
        }
    }

    //noinspection DuplicatedCode
    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_from_local_to_local() {
        let col_int = Int64Chunked::from_iter((0i64..1000i64).map(Some))
            .into_series()
            .with_name(PlSmallStr::from("col_int"));
        let col_float = Float64Chunked::from_iter((0..1000).map(|i| Some(i as f64)))
            .into_series()
            .with_name(PlSmallStr::from("col_float"));
        let col_string = StringChunked::from_iter((0..1000).map(|i| Some(i.to_string())))
            .into_series()
            .with_name(PlSmallStr::from("col_string"));
        let df_out = DataFrame::new(vec![
            Column::from(col_int),
            Column::from(col_float),
            Column::from(col_string),
        ])
        .unwrap();
        let lf = df_out.clone().lazy();

        let folder = testdir!();
        let id_r = id();
        let file_r = format!("{id_r}.parquet");
        let path_r = folder.clone().join(file_r.clone());
        let url_r = Url::from_file_path(path_r.clone()).unwrap();

        let _ = lf
            .sink_parquet(
                SinkTarget::Path(PlPath::new(path_r.to_string_lossy().to_string().as_str())),
                ParquetWriteOptions::default(),
                None,
                SinkOptions {
                    sync_on_close: SyncOnCloseType::All,
                    maintain_order: true,
                    mkdir: true,
                },
            )
            .unwrap()
            .collect();

        let id_w = id();
        let file_w = format!("{id_w}.parquet");
        let path_w = folder.clone().join(file_w.clone());
        let url_w = Url::from_file_path(path_w.clone()).unwrap();

        let request = ImportRequestBuilder::default()
            .source(
                ImportSourceBuilder::default()
                    .location(Location::LocalFile {
                        url: WildcardUrl(url_r.clone()),
                    })
                    .initial_lastmod(None)
                    .lastmod_info(None)
                    .build()
                    .unwrap(),
            )
            .format(ImportFormat::Parquet)
            .target(
                ImportTargetBuilder::default()
                    .location(Location::LocalFile {
                        url: BaseImportUrl(url_w.clone()),
                    })
                    .build()
                    .unwrap(),
            )
            .parallelism(None)
            .build()
            .unwrap();

        let meta = ObjectMeta {
            location: ObjectStorePath::from(path_r.clone().to_string_lossy().as_ref()),
            last_modified: Utc::now(),
            size: 1024,
            e_tag: Some(id_r.to_string()),
            version: Some("v1".to_string()),
        };
        let files: Vec<(Url, ObjectMeta)> = vec![(url_w.clone(), meta)];

        let response = FilesImporter::import(&request, files).await.unwrap();

        assert!(!response.is_empty());
        assert_eq!(response.len(), 1);

        let url_tf = response.first().unwrap().to.clone();

        let lf = LazyFrame::scan_parquet(
            PlPath::new(url_tf.to_string().as_str()),
            ScanArgsParquet {
                ..Default::default()
            },
        )
        .unwrap();

        let df_in = tokio::task::spawn_blocking(move || lf.collect())
            .await
            .unwrap()
            .unwrap();

        assert!(df_out.equals(&df_in));
    }
}
