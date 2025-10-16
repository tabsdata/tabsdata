//
// Copyright 2025. Tabs Data Inc.
//

use crate::transporter::api::{
    CopyReport, CopyRequest, FileImportReport, FileImportReportBuilder, ImportFormat,
    ImportRequest, ImportSource,
};
use crate::transporter::args::{
    Format, ImporterLogReadOptions, ImporterNdJsonReadOptions, ImporterOptions,
    ImporterOptionsBuilder, ImporterParquetReadOptions, ToFormat,
};
use crate::transporter::copy::copy;
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
        "file" | "s3" | "az" | "gs" => extractor(url),
        _ => panic!("Unsupported scheme: {}", url.scheme()),
    }
}

/// Return the base URL of the source location.
///
/// For file:// schemes, it returns file:/// or file:///c:/ depending on running OS
///
/// For s3:// schemes, it returns s3://<bucket>
///
/// For az:// schemes, it returns az://<bucket>
///
/// For gs:// schemes, it returns gs://<bucket>
///
fn base_url(source: &ImportSource) -> Url {
    let url = source.location().url();
    extract_from_url(&url, |url| match url.scheme() {
        "file" => Url::parse(&crate::transporter::args::root_folder()).unwrap(),
        "s3" => Url::parse(&format!("s3://{}", url.authority())).unwrap(),
        "az" => Url::parse(&format!("az://{}", url.authority())).unwrap(),
        "gs" => Url::parse(&format!("gs://{}", url.authority())).unwrap(),
        _ => unreachable!(),
    })
}

/// Return the base path of the source location.
///
/// This is where files will be searched for.
pub fn base_path(source: &ImportSource) -> Result<String, TransporterError> {
    let url = source.location().url();
    extract_from_url(&url, |url| {
        let path = Path::new(url.path());
        let parent = path
            .parent()
            .ok_or_else(|| TransporterError::UrlHasNoPath(url.to_string()))?;
        let path_str = parent
            .to_str()
            .ok_or_else(|| TransporterError::CannotConvertPathToString(url.to_string()))?;
        Ok(path_str.to_string())
    })
}

pub fn file_pattern(source: &ImportSource) -> Result<String, TransporterError> {
    let url = source.location().url();
    extract_from_url(&url, |url| {
        let path = Path::new(url.path());
        let file_name = path
            .file_name()
            .ok_or_else(|| TransporterError::UrlHasNoFileName(url.to_string()))?;
        let file_name_str = file_name
            .to_str()
            .ok_or_else(|| TransporterError::CannotConvertFileNameToString(url.to_string()))?;
        Ok(file_name_str.to_string())
    })
}

impl From<&ImportFormat> for Format {
    fn from(format: &ImportFormat) -> Self {
        match format {
            ImportFormat::Csv(options) => Format::Csv(options.into()),
            ImportFormat::Json => Format::NdJson(ImporterNdJsonReadOptions::default()),
            ImportFormat::Log => Format::Log(ImporterLogReadOptions::default()),
            ImportFormat::Parquet => Format::Parquet(ImporterParquetReadOptions::default()),
            ImportFormat::Binary => Format::Binary,
        }
    }
}

impl TryFrom<&ImportRequest> for ImporterOptions {
    type Error = TransporterError;

    fn try_from(req: &ImportRequest) -> Result<Self, Self::Error> {
        ImporterOptionsBuilder::default()
            .base_url(base_url(req.source())) // not used, for ref only
            .base_path(base_path(req.source())?) // not used, for ref only
            .file_pattern(file_pattern(req.source())?) // not used, for ref only
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
            .map_err(|e| TransporterError::CouldNotBuildImporterOptions(e.to_string()))
    }
}

fn create_file_import_instructions(
    import_request: &ImportRequest,
    files_to_import: Vec<(Url, ObjectMeta)>,
) -> Result<Vec<crate::transporter::logic::FileImportInstructions>, TransporterError> {
    let importer_options: Arc<ImporterOptions> = Arc::new(import_request.try_into()?);

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

impl TryFrom<crate::transporter::logic::FileImportReport> for FileImportReport {
    type Error = TransporterError;

    fn try_from(
        file_import_report: crate::transporter::logic::FileImportReport,
    ) -> Result<Self, Self::Error> {
        let from_url = Url::parse(file_import_report.from())
            .map_err(|e| TransporterError::InvalidUrl(file_import_report.from().to_string(), e))?;
        let to_url = Url::parse(file_import_report.to())
            .map_err(|e| TransporterError::InvalidUrl(file_import_report.to().to_string(), e))?;

        FileImportReportBuilder::default()
            .idx(*file_import_report.idx())
            .from(from_url)
            .size(*file_import_report.size())
            .rows(*file_import_report.rows())
            .last_modified(*file_import_report.last_modified())
            .to(to_url)
            .imported_at(*file_import_report.imported_at())
            .import_millis(*file_import_report.import_millis())
            .build()
            .map_err(|e| TransporterError::CouldNotBuildFileImportReport(e.to_string()))
    }
}

pub struct FilesImporter;

#[async_trait]
impl Importer for FilesImporter {
    async fn import(
        import_request: &ImportRequest,
        files_to_import: Vec<(Url, ObjectMeta)>,
    ) -> Result<Vec<FileImportReport>, TransporterError> {
        let import_reports = if import_request.format().using_polars() {
            let import_instructions =
                create_file_import_instructions(import_request, files_to_import)?;
            let import_reports: Vec<_> = tokio::task::spawn_blocking(move || {
                import_instructions
                    .into_iter()
                    .map(crate::transporter::logic::run_polars_import)
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(|e| TransporterError::CouldNotRunImportFiles(e.to_string()))?
            .into_iter()
            .collect::<Result<_, crate::transporter::logic::ImportError>>()
            .map_err(|e| TransporterError::ImportFilesTaskFailed(e.to_string()))?;

            import_reports
                .into_iter()
                .map(FileImportReport::try_from)
                .collect::<Result<Vec<_>, _>>()?
        } else if files_to_import.is_empty() {
            return Ok(vec![]);
        } else {
            let copy_request =
                convert_import_instructions_to_copy_request(import_request, &files_to_import);
            let copy_report = copy(copy_request).await?;
            convert_copy_report_to_import_reports(&files_to_import, copy_report)?
        };
        Ok(import_reports)
    }
}

fn convert_import_instructions_to_copy_request(
    import_request: &ImportRequest,
    files_to_import: &[(Url, ObjectMeta)],
) -> CopyRequest {
    let source_target_pairs = files_to_import
        .iter()
        .map(|(url, _)| {
            (
                import_request.source().source_location(url),
                import_request.target().target_location(url),
            )
        })
        .collect::<Vec<_>>();
    CopyRequest::new(source_target_pairs, None)
}

fn convert_copy_report_to_import_reports(
    files_to_import: &[(Url, ObjectMeta)],
    copy_report: CopyReport,
) -> Result<Vec<FileImportReport>, TransporterError> {
    copy_report
        .files()
        .iter()
        .zip(files_to_import.iter().map(|(_, meta)| meta))
        .map(|(file_report, meta)| {
            FileImportReportBuilder::default()
                .idx(file_report.idx)
                .from(file_report.from.clone())
                .size(file_report.size as usize)
                .rows(0usize) // Polars will calculate rows, so we set it to 0
                .last_modified(meta.last_modified)
                .to(file_report.to.clone())
                .imported_at(file_report.started_at)
                .import_millis(
                    (file_report.ended_at - file_report.started_at).num_milliseconds() as usize,
                )
                .build()
                .map_err(|e| TransporterError::CouldNotBuildFileImportReport(e.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::transporter::api::{
        AwsConfigs, AzureConfigs, BaseImportUrl, GcpConfigs, ImportCsvOptions, ImportFormat,
        ImportRequestBuilder, ImportSourceBuilder, ImportTargetBuilder, Location, Value,
        WildcardUrl,
    };
    use crate::transporter::cloud::create_sink_target;
    use crate::transporter::files_importer::{FilesImporter, Importer};
    use chrono::Utc;
    use object_store::ObjectMeta;
    use polars::datatypes::{Float64Chunked, Int64Chunked, PlSmallStr, StringChunked};
    use polars::frame::DataFrame;
    use polars::prelude::{
        Column, IntoLazy, IntoSeries, LazyCsvReader, LazyFileListReader, LazyFrame, PlPath,
        ScanArgsParquet, SinkOptions, SinkTarget,
    };
    use polars_io::cloud::{CloudOptions, ObjectStorePath};
    use polars_io::json::JsonWriterOptions;
    use polars_io::prelude::QuoteStyle::Never;
    use polars_io::prelude::{CsvWriterOptions, ParquetWriteOptions, SerializeOptions};
    use polars_io::utils::sync_on_close::SyncOnCloseType;
    use std::collections::HashMap;
    use td_common::id::id;
    use td_test::reqs::{
        AzureStorageWithAccountKeyReqs, GcpStorageWithServiceAccountKeyReqs,
        S3WithAccessKeySecretKeyReqs,
    };
    use testdir::testdir;
    use url::Url;

    //noinspection DuplicatedCode
    #[td_test::test(when(reqs = S3WithAccessKeySecretKeyReqs, env_prefix= "s30"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_from_local_to_aws(s3: S3WithAccessKeySecretKeyReqs) {
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

        lf.sink_parquet(
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
        .collect()
        .unwrap();

        let url_w = Url::parse(&format!("{}/unit_test/{}", s3.uri, file_r.clone())).unwrap();

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
                            access_key: Value::Literal(s3.access_key.to_string()),
                            secret_key: Value::Literal(s3.secret_key.to_string()),
                            region: Some(Value::Literal(s3.region.to_string())),
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
            location: ObjectStorePath::from(
                path_r.clone().to_string_lossy().replace('\\', "/").as_ref(),
            ),
            last_modified: Utc::now(),
            size: 1024,
            e_tag: Some(id.to_string()),
            version: Some("v1".to_string()),
        };
        let files: Vec<(Url, ObjectMeta)> = vec![(url_r.clone(), meta)];

        let e_files = files.clone();
        let response = FilesImporter::import(&request, files)
            .await
            .map_err(|e| {
                eprintln!("FilesImporter::import failed:");
                eprintln!("  Request: {:?}", request);
                eprintln!("  Files: {:?}", e_files);
                eprintln!("  Error: {:?}", e);
                e
            })
            .unwrap();
        assert!(!response.is_empty());
        assert_eq!(response.len(), 1);

        let url_tf = response.first().unwrap().to.clone();

        let mut config = HashMap::new();
        config.insert("AWS_ACCESS_KEY_ID".to_string(), s3.access_key.to_string());
        config.insert(
            "AWS_SECRET_ACCESS_KEY".to_string(),
            s3.secret_key.to_string(),
        );
        config.insert("AWS_REGION".to_string(), s3.region.to_string());
        let e_config = config.clone();
        let cloud_options = CloudOptions::from_untyped_config(url_tf.as_str(), config)
            .map_err(|e| {
                eprintln!("CloudOptions::from_untyped_config failed:");
                eprintln!("  URL: {}", url_tf.as_str());
                eprintln!("  Config: {:?}", e_config);
                eprintln!("  Error: {:?}", e);
                e
            })
            .unwrap();

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

    //noinspection DuplicatedCode
    #[td_test::test(when(reqs = AzureStorageWithAccountKeyReqs, env_prefix= "az0"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_from_local_to_azure(az: AzureStorageWithAccountKeyReqs) {
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

        lf.sink_parquet(
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
        .collect()
        .unwrap();

        let url_w = Url::parse(&format!("{}/unit_test/{}", az.uri, file_r.clone())).unwrap();

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
                            account_name: Value::Literal(az.account_name.to_string()),
                            account_key: Value::Literal(az.account_key.to_string()),
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
            location: ObjectStorePath::from(
                path_r.clone().to_string_lossy().replace('\\', "/").as_ref(),
            ),
            last_modified: Utc::now(),
            size: 1024,
            e_tag: Some(id.to_string()),
            version: Some("v1".to_string()),
        };
        let files: Vec<(Url, ObjectMeta)> = vec![(url_r.clone(), meta)];

        let e_files = files.clone();
        let response = FilesImporter::import(&request, files)
            .await
            .map_err(|e| {
                eprintln!("FilesImporter::import failed:");
                eprintln!("  Request: {:?}", request);
                eprintln!("  Files: {:?}", e_files);
                eprintln!("  Error: {:?}", e);
                e
            })
            .unwrap();
        assert!(!response.is_empty());
        assert_eq!(response.len(), 1);

        let url_tf = response.first().unwrap().to.clone();

        let mut config = HashMap::new();
        config.insert(
            "azure_storage_account_name".to_string(),
            az.account_name.to_string(),
        );
        config.insert(
            "azure_storage_account_key".to_string(),
            az.account_key.to_string(),
        );
        let e_config = config.clone();
        let cloud_options = CloudOptions::from_untyped_config(url_tf.as_str(), config)
            .map_err(|e| {
                eprintln!("CloudOptions::from_untyped_config failed:");
                eprintln!("  URL: {}", url_tf.as_str());
                eprintln!("  Config: {:?}", e_config);
                eprintln!("  Error: {:?}", e);
                e
            })
            .unwrap();

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

    //noinspection DuplicatedCode
    #[td_test::test(when(reqs = GcpStorageWithServiceAccountKeyReqs, env_prefix= "gcp0"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_from_local_to_gcp(gcp: GcpStorageWithServiceAccountKeyReqs) {
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

        lf.sink_parquet(
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
        .collect()
        .unwrap();

        let url_w = Url::parse(&format!("{}/unit_test/{}", gcp.uri, file_r.clone())).unwrap();

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
                    .location(Location::GCS {
                        url: BaseImportUrl(url_w.clone()),
                        configs: GcpConfigs {
                            service_account_key: Value::Literal(
                                gcp.service_account_key.to_string(),
                            ),
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
            location: ObjectStorePath::from(
                path_r.clone().to_string_lossy().replace('\\', "/").as_ref(),
            ),
            last_modified: Utc::now(),
            size: 1024,
            e_tag: Some(id.to_string()),
            version: Some("v1".to_string()),
        };
        let files: Vec<(Url, ObjectMeta)> = vec![(url_r.clone(), meta)];

        let e_files = files.clone();
        let response = FilesImporter::import(&request, files)
            .await
            .map_err(|e| {
                eprintln!("FilesImporter::import failed:");
                eprintln!("  Request: {:?}", request);
                eprintln!("  Files: {:?}", e_files);
                eprintln!("  Error: {:?}", e);
                e
            })
            .unwrap();
        assert!(!response.is_empty());
        assert_eq!(response.len(), 1);

        let url_tf = response.first().unwrap().to.clone();

        let mut config = HashMap::new();
        config.insert(
            "google_service_account_key".to_string(),
            gcp.service_account_key.to_string(),
        );

        let e_config = config.clone();
        let cloud_options = CloudOptions::from_untyped_config(url_tf.as_str(), config)
            .map_err(|e| {
                eprintln!("CloudOptions::from_untyped_config failed:");
                eprintln!("  URL: {}", url_tf.as_str());
                eprintln!("  Config: {:?}", e_config);
                eprintln!("  Error: {:?}", e);
                e
            })
            .unwrap();

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
        let path_r_normalized = path_r.to_string_lossy().replace("\\", "/");
        let url_r = Url::parse(&format!("file:///{path_r_normalized}")).unwrap();

        lf.sink_parquet(
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
        .collect()
        .unwrap();

        let id_w = id();
        let file_w = format!("{id_w}.parquet");
        let path_w = folder.clone().join(file_w.clone());
        let path_w_normalized = path_w.to_string_lossy().replace("\\", "/");
        let url_w = Url::parse(&format!("file:///{path_w_normalized}")).unwrap();

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
            location: ObjectStorePath::from(path_r_normalized.as_str()),
            last_modified: Utc::now(),
            size: 1024,
            e_tag: Some(id_r.to_string()),
            version: Some("v1".to_string()),
        };
        let files: Vec<(Url, ObjectMeta)> = vec![(url_r.clone(), meta)];

        let e_files = files.clone();
        let response = FilesImporter::import(&request, files)
            .await
            .map_err(|e| {
                eprintln!("FilesImporter::import failed:");
                eprintln!("  Request: {:?}", request);
                eprintln!("  Files: {:?}", e_files);
                eprintln!("  Error: {:?}", e);
                e
            })
            .unwrap();

        assert!(!response.is_empty());
        assert_eq!(response.len(), 1);

        let url_tf = response.first().unwrap().to.clone();

        let path_tf = if url_tf.scheme() == "file" {
            #[cfg(not(windows))]
            {
                url_tf.path().to_string()
            }
            #[cfg(windows)]
            {
                let mut path_str_lf = url_tf.path().to_string();
                if path_str_lf.starts_with('/')
                    && path_str_lf.len() > 1
                    && path_str_lf.chars().nth(2) == Some(':')
                {
                    path_str_lf.remove(0);
                }
                path_str_lf.replace("/", "\\")
            }
        } else {
            url_tf.to_string()
        };

        let lf = LazyFrame::scan_parquet(
            PlPath::new(path_tf.as_str()),
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

    //noinspection DuplicatedCode
    async fn test_import(
        from_base: &Location<Url>,
        to_base: &Location<BaseImportUrl>,
        format: &ImportFormat,
    ) {
        let sink_options = SinkOptions {
            sync_on_close: SyncOnCloseType::All,
            maintain_order: true,
            mkdir: true,
        };

        let from_file = from_base.join(&id().to_string()).unwrap();

        let col_int = Int64Chunked::from_iter((0i64..1000i64).map(Some))
            .into_series()
            .with_name(PlSmallStr::from("col_int"));
        let col_string = StringChunked::from_iter((0..1000).map(|i| Some(format!("str_{}\"", i))))
            .into_series()
            .with_name(PlSmallStr::from("col_string"));
        let df = DataFrame::new(vec![Column::from(col_int), Column::from(col_string)]).unwrap();
        let lf = df.clone().lazy();

        let _df = match format {
            ImportFormat::Csv(_) => lf
                .sink_csv(
                    create_sink_target(&from_file.url(), &from_file.cloud_configs()).unwrap(),
                    CsvWriterOptions::default(),
                    None,
                    sink_options.clone(),
                )
                .unwrap()
                .collect()
                .unwrap(),
            ImportFormat::Json => lf
                .sink_json(
                    create_sink_target(&from_file.url(), &from_file.cloud_configs()).unwrap(),
                    JsonWriterOptions::default(),
                    None,
                    sink_options.clone(),
                )
                .unwrap()
                .collect()
                .unwrap(),
            ImportFormat::Log => lf
                .sink_csv(
                    create_sink_target(&from_file.url(), &from_file.cloud_configs()).unwrap(),
                    CsvWriterOptions {
                        serialize_options: SerializeOptions {
                            quote_style: Never,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    None,
                    sink_options.clone(),
                )
                .unwrap()
                .collect()
                .unwrap(),
            ImportFormat::Parquet => lf
                .sink_parquet(
                    create_sink_target(&from_file.url(), &from_file.cloud_configs()).unwrap(),
                    ParquetWriteOptions::default(),
                    None,
                    sink_options.clone(),
                )
                .unwrap()
                .collect()
                .unwrap(),
            ImportFormat::Binary => lf
                .sink_csv(
                    create_sink_target(&from_file.url(), &from_file.cloud_configs()).unwrap(),
                    CsvWriterOptions::default(),
                    Some(
                        CloudOptions::from_untyped_config(
                            from_file.url().as_str(),
                            from_file.cloud_configs(),
                        )
                        .unwrap(),
                    ),
                    sink_options.clone(),
                )
                .unwrap()
                .collect()
                .unwrap(),
        };

        let request = ImportRequestBuilder::default()
            .source(
                ImportSourceBuilder::default()
                    .location(&from_file)
                    .initial_lastmod(None)
                    .lastmod_info(None)
                    .build()
                    .unwrap(),
            )
            .format(format.clone())
            .target(
                ImportTargetBuilder::default()
                    .location(to_base.clone())
                    .build()
                    .unwrap(),
            )
            .parallelism(None)
            .build()
            .unwrap();

        let meta = ObjectMeta {
            location: ObjectStorePath::from(from_file.url().path()),
            last_modified: Utc::now(),
            size: 1024,
            e_tag: Some("e_tag".to_string()),
            version: Some("v1".to_string()),
        };
        let files: Vec<(Url, ObjectMeta)> = vec![(from_file.url().clone(), meta)];

        let e_files = files.clone();
        let response = FilesImporter::import(&request, files)
            .await
            .map_err(|e| {
                eprintln!("FilesImporter::import failed:");
                eprintln!("  Request: {:?}", request);
                eprintln!("  Files: {:?}", e_files);
                eprintln!("  Error: {:?}", e);
                e
            })
            .unwrap();

        assert!(!response.is_empty());
        assert_eq!(response.len(), 1);

        let url_to = response.first().unwrap().to.clone();
        let e_cloud_configs = to_base.cloud_configs();
        let cloud_options = Some(
            CloudOptions::from_untyped_config(url_to.as_str(), to_base.cloud_configs())
                .map_err(|e| {
                    eprintln!("CloudOptions::from_untyped_config failed in import function:");
                    eprintln!("  URL: {}", url_to.as_str());
                    eprintln!("  Config: {:?}", e_cloud_configs);
                    eprintln!("  Error: {:?}", e);
                    e
                })
                .unwrap(),
        );
        let lf = match format {
            ImportFormat::Binary => {
                let reader =
                    LazyCsvReader::new(PlPath::new(url_to.as_str())).with_infer_schema_length(None);
                reader.with_cloud_options(cloud_options).finish().unwrap()
            }
            _ => {
                let parquet_config = ScanArgsParquet {
                    cloud_options,
                    ..Default::default()
                };
                LazyFrame::scan_parquet(PlPath::new(url_to.as_str()), parquet_config).unwrap()
            }
        };

        let df_out = tokio::task::spawn_blocking(move || lf.collect())
            .await
            .unwrap()
            .unwrap();

        match format {
            ImportFormat::Log => {
                assert_eq!(df_out.shape().0, df.shape().0 + 1); // Log format adds an extra row for the header
            }
            _ => {
                assert!(df_out.equals(&df));
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_local_csv() {
        let test_dir = testdir!();
        let base_url = Url::from_directory_path(&test_dir).unwrap();
        test_import(
            &Location::LocalFile {
                url: base_url.clone(),
            },
            &Location::LocalFile {
                url: BaseImportUrl(base_url.clone()),
            },
            &ImportFormat::Csv(ImportCsvOptions::default()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_local_json() {
        let test_dir = testdir!();
        let base_url = Url::from_directory_path(&test_dir).unwrap();
        test_import(
            &Location::LocalFile {
                url: base_url.clone(),
            },
            &Location::LocalFile {
                url: BaseImportUrl(base_url.clone()),
            },
            &ImportFormat::Json,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_local_log() {
        let test_dir = testdir!();
        let base_url = Url::from_directory_path(&test_dir).unwrap();
        test_import(
            &Location::LocalFile {
                url: base_url.clone(),
            },
            &Location::LocalFile {
                url: BaseImportUrl(base_url.clone()),
            },
            &ImportFormat::Log,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_local_parquet() {
        let test_dir = testdir!();
        let base_url = Url::from_directory_path(&test_dir).unwrap();
        test_import(
            &Location::LocalFile {
                url: base_url.clone(),
            },
            &Location::LocalFile {
                url: BaseImportUrl(base_url.clone()),
            },
            &ImportFormat::Parquet,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_local_binary() {
        let test_dir = testdir!();
        let base_url = Url::from_directory_path(&test_dir).unwrap();
        test_import(
            &Location::LocalFile {
                url: base_url.clone(),
            },
            &Location::LocalFile {
                url: BaseImportUrl(base_url.clone()),
            },
            &ImportFormat::Binary,
        )
        .await;
    }

    //noinspection DuplicatedCode
    #[td_test::test(when(reqs = S3WithAccessKeySecretKeyReqs, env_prefix= "s30"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_s3_binary(s3: S3WithAccessKeySecretKeyReqs) {
        let from_base = Location::S3 {
            url: Url::parse(&format!("{}/{}", s3.uri, id())).unwrap(),
            configs: AwsConfigs {
                access_key: Value::Literal(s3.access_key.to_string()),
                secret_key: Value::Literal(s3.secret_key.to_string()),
                region: Some(Value::Literal(s3.region.to_string())),
                extra_configs: None,
            },
        };

        let test_dir = testdir!();
        let base_url = Url::from_directory_path(&test_dir).unwrap();
        test_import(
            &from_base,
            &Location::LocalFile {
                url: BaseImportUrl(base_url.clone()),
            },
            &ImportFormat::Binary,
        )
        .await;
    }

    #[td_test::test(when(reqs = AzureStorageWithAccountKeyReqs, env_prefix= "az0"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_azure_binary(az: AzureStorageWithAccountKeyReqs) {
        let from_base = Location::Azure {
            url: Url::parse(&format!("{}/{}", az.uri, id())).unwrap(),
            configs: AzureConfigs {
                account_name: Value::Literal(az.account_name.to_string()),
                account_key: Value::Literal(az.account_key.to_string()),
                extra_configs: None,
            },
        };

        let test_dir = testdir!();
        let base_url = Url::from_directory_path(&test_dir).unwrap();
        test_import(
            &from_base,
            &Location::LocalFile {
                url: BaseImportUrl(base_url.clone()),
            },
            &ImportFormat::Binary,
        )
        .await;
    }

    #[td_test::test(when(reqs = GcpStorageWithServiceAccountKeyReqs, env_prefix= "gcp0"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_import_gcp_binary(gcp: GcpStorageWithServiceAccountKeyReqs) {
        let from_base = Location::GCS {
            url: Url::parse(&format!("{}/{}", gcp.uri, id())).unwrap(),
            configs: GcpConfigs {
                service_account_key: Value::Literal(gcp.service_account_key.to_string()),
                extra_configs: None,
            },
        };

        let test_dir = testdir!();
        let base_url = Url::from_directory_path(&test_dir).unwrap();
        test_import(
            &from_base,
            &Location::LocalFile {
                url: BaseImportUrl(base_url.clone()),
            },
            &ImportFormat::Binary,
        )
        .await;
    }
}
