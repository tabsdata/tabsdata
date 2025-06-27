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
