//
// Copyright 2024 Tabs Data Inc.
//

use crate::transporter::api::{ErrorReport, TransporterReport, TransporterRequest};
use crate::transporter::copy::copy;
use crate::transporter::error::TransporterError;
use crate::transporter::import::import;
use serde::Serialize;
use std::fs::File;
use std::path::{Path, PathBuf};
use td_common::cli::{Cli, NoConfig};
use td_common::env::get_current_dir;
use td_common::logging;
use td_common::status::ExitStatus;
use tracing::Level;

#[derive(Debug, Clone, clap_derive::Parser)]
#[command(version)]
pub struct TransporterParams {
    #[arg(
        short_alias = 'a',
        long,
        required = true,
        help = "File with the request instructions for the transporter",
        value_parser = load_request
    )]
    pub request: Option<TransporterRequest>,

    #[arg(
        short_alias = 'p',
        long,
        required = true,
        help = "File to write the transporter report of the execution",
        value_parser = non_existing_file
    )]
    pub report: Option<PathBuf>,

    #[arg(
        short_alias = 's',
        long,
        exclusive = true,
        help = "Prints multiple samples of the transporter request and the report files"
    )]
    pub samples: bool,
}

fn full_path(path: &str) -> PathBuf {
    let current_dir: PathBuf = get_current_dir();
    current_dir.join(path)
}
fn open_file(path: &PathBuf) -> Result<File, TransporterError> {
    File::open(path).map_err(|e| {
        TransporterError::RequestFileCannotBeOpened(path.to_str().unwrap().to_string(), e)
    })
}
fn existing_file(s: &str) -> Result<PathBuf, TransporterError> {
    let path = full_path(s);
    path.exists()
        .then_some(&path)
        .ok_or(TransporterError::RequestFileNotFound(s.to_string()))?;
    Ok(path)
}

fn load_request(s: &str) -> Result<TransporterRequest, TransporterError> {
    let path = existing_file(s)?;
    let file = open_file(&path)?;
    serde_yaml::from_reader(file).map_err(|err| {
        TransporterError::CouldNotReadRequest(path.to_str().unwrap().to_string(), err)
    })
}

fn non_existing_file(s: &str) -> Result<PathBuf, TransporterError> {
    let path = full_path(s);
    if path.exists() {
        return Err(TransporterError::ReportFileMustNotExist(s.to_string()));
    }
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            return Err(TransporterError::ReportFileDirNotFound(
                parent.to_string_lossy().to_string(),
            ));
        }
    }
    Ok(path)
}

fn write_to_file(path: &Path, data: &impl Serialize) -> Result<(), TransporterError> {
    let file = File::create(path).map_err(|e| {
        TransporterError::ReportFileCannotBeCreated(path.to_str().unwrap().to_string(), e)
    })?;
    serde_yaml::to_writer(file, &data).map_err(|e| {
        TransporterError::CouldNotWriteReport(path.to_str().unwrap().to_string(), e)
    })?;
    Ok(())
}

/// Run the transporter. This function is the entry point for the `transporter` binary.
pub fn run() {
    Cli::<NoConfig, TransporterParams>::exec_async(
        "transporter",
        |_config, params| async move { run_impl_report_to_file(params).await },
        None,
    );
}

async fn run_impl_report_to_file(params: TransporterParams) -> ExitStatus {
    let report_file = params.report.clone();
    let res = run_impl(params).await;
    let status = if res.is_ok() {
        ExitStatus::Success
    } else {
        ExitStatus::GeneralError
    };
    let report = res.unwrap_or_else(Some);
    if let Some(report) = report {
        write_to_file(&report_file.unwrap(), &report).unwrap();
    }
    status
}

async fn run_impl(
    params: TransporterParams,
) -> Result<Option<TransporterReport>, TransporterReport> {
    if params.samples {
        println!(
            "Transporter request samples\n: {}\n",
            TransporterRequest::yaml_samples()
        );
        println!(
            "Transporter report samples\n: {}\n",
            TransporterReport::yaml_samples()
        );
        return Ok(None);
    }

    // Initialize logging
    logging::start(Level::INFO, None, false);

    let request = params.request.unwrap();

    let res = match request {
        TransporterRequest::ImportV1(request) => import(request)
            .await
            .map(TransporterReport::ImportV1)
            .map_err(TransporterReport::ErrorV1),

        TransporterRequest::CopyV1(request) => copy(request)
            .await
            .map(TransporterReport::CopyV1)
            .map_err(|err| TransporterReport::ErrorV1(ErrorReport::new(err.to_string()))),
    };
    res.map(Some)
}

#[cfg(test)]
mod tests {
    use crate::transporter::api::{
        AwsConfigs, AzureConfigs, CopyRequest, Location, TransporterReport, TransporterRequest,
        Value,
    };
    use crate::transporter::cli::{run_impl, run_impl_report_to_file, TransporterParams};
    use crate::transporter::common::create_store;
    use clap::Parser;
    use itertools::Itertools;
    use std::collections::HashSet;
    use std::fs::File;
    use std::io::Write;
    use testdir::testdir;
    use url::Url;

    #[tokio::test]
    async fn test_run_with_input_output_files() {
        let test_dir = testdir!();
        let request_file = test_dir.join("request.yaml");
        let report_file = test_dir.join("report.yaml");
        let source_file = test_dir.join("source.txt");
        let target_file = test_dir.join("target.txt");
        let str = "Hello, World!";
        let data = str.as_bytes();
        File::create(&source_file).unwrap().write_all(data).unwrap();
        let source = Location::LocalFile {
            url: Url::from_file_path(&source_file).unwrap(),
        };
        let target = Location::LocalFile {
            url: Url::from_file_path(&target_file).unwrap(),
        };
        let request = TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![(source, target.clone())],
            parallelism: None,
        });
        let request_str = serde_yaml::to_string(&request).unwrap();
        File::create(&request_file)
            .unwrap()
            .write_all(request_str.as_bytes())
            .unwrap();

        #[derive(Debug, clap_derive::Parser)]
        #[command(version)]
        struct ParamsParser {
            #[command(flatten)]
            params: TransporterParams,
        }

        let args = vec![
            "transporter",
            "--request",
            request_file.to_str().unwrap(),
            "--report",
            report_file.to_str().unwrap(),
        ];

        let params: TransporterParams = ParamsParser::try_parse_from(args).unwrap().params;

        let response = run_impl_report_to_file(params).await;
        assert!(matches!(response, td_common::status::ExitStatus::Success));
        let report = serde_yaml::from_reader(File::open(&report_file).unwrap()).unwrap();
        assert!(matches!(report, Some(TransporterReport::CopyV1(_))));
    }

    fn check_envs(test_name: &str, required_envs: Vec<&str>) -> bool {
        let required_envs = required_envs
            .into_iter()
            .map(ToString::to_string)
            .collect::<HashSet<_>>();
        let defined_envs = std::env::vars()
            .filter(|(name, _value)| required_envs.contains(name))
            .map(|(name, _value)| name)
            .collect::<HashSet<_>>();

        let diff = required_envs.difference(&defined_envs).collect::<Vec<_>>();
        if !diff.is_empty() {
            println!(
                ">>>>>>>> !!!!Skipping test {} because the following ENVs are not set: {:?}",
                test_name,
                diff.into_iter().cloned().join(",")
            );
            false
        } else {
            true
        }
    }

    async fn test_run_impl_copy(target: Location<Url>) {
        let str = "Hello, World!".repeat(1024 * 1024);
        let data = str.as_bytes();
        let test_dir = testdir!();
        let source_file = test_dir.join("source.txt");
        File::create(&source_file).unwrap().write_all(data).unwrap();
        let source = Location::LocalFile {
            url: Url::from_file_path(&source_file).unwrap(),
        };

        let request = TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![(source, target.clone())],
            parallelism: None,
        });
        let response = run_impl(TransporterParams {
            request: Some(request),
            report: None,
            samples: false,
        })
        .await;
        assert!(response.is_ok());
        match response {
            Ok(Some(TransporterReport::CopyV1(report))) => {
                assert_eq!(report.files().len(), 1);
                assert_eq!(report.files()[0].to, target.url());
                let (store, path) = create_store(&target).unwrap();
                let written_data = store.get(&path).await.unwrap().bytes().await.unwrap();
                assert_eq!(written_data, data);
            }
            Ok(Some(TransporterReport::ImportV1(_))) => panic!("Unexpected import report"),
            Ok(Some(TransporterReport::ErrorV1(_))) => panic!("Unexpected error report"),
            Ok(None) => panic!("Expected a report"),
            Err(report) => panic!("Error: {:?}", report),
        }
    }

    #[tokio::test]
    async fn test_copy_local_to_local() {
        let target = Location::LocalFile {
            url: Url::from_file_path(testdir!().join("target.txt")).unwrap(),
        };
        test_run_impl_copy(target).await;
    }

    #[tokio::test]
    async fn test_copy_local_to_aws() {
        const BASE_URL_ENV: &str = "COPY_AWS_BASE_URL";
        const AWS_REGION_ENV: &str = "COPY_AWS_AWS_REGION";
        const AWS_ACCESS_KEY_ID_ENV: &str = "COPY_AWS_AWS_ACCESS_KEY_ID";
        const AWS_SECRET_ACCESS_KEY_ENV: &str = "COPY_AWS_AWS_SECRET_ACCESS_KEY";

        if check_envs(
            "test_copy_to_aws",
            vec![
                BASE_URL_ENV,
                AWS_REGION_ENV,
                AWS_ACCESS_KEY_ID_ENV,
                AWS_SECRET_ACCESS_KEY_ENV,
            ],
        ) {
            let target = Location::S3 {
                url: Url::parse(&std::env::var(BASE_URL_ENV).unwrap()).unwrap(),
                configs: AwsConfigs {
                    region: Some(Value::Env(AWS_REGION_ENV.to_string())),
                    access_key: Value::Env(AWS_ACCESS_KEY_ID_ENV.to_string()),
                    secret_key: Value::Env(AWS_SECRET_ACCESS_KEY_ENV.to_string()),
                    extra_configs: None,
                },
            };
            test_run_impl_copy(target).await;
        }
    }

    #[tokio::test]
    async fn test_copy_local_to_azure() {
        const BASE_URL_ENV: &str = "COPY_AZURE_BASE_URL";
        const AZURE_ACCOUNT_NAME_ENV: &str = "COPY_AZURE_ACCOUNT_NAME";
        const AZURE_ACCOUNT_KEY_ENV: &str = "COPY_AZURE_ACCOUNT_KEY";

        if check_envs(
            "test_copy_to_azure",
            vec![BASE_URL_ENV, AZURE_ACCOUNT_NAME_ENV, AZURE_ACCOUNT_KEY_ENV],
        ) {
            let target = Location::Azure {
                url: Url::parse(&std::env::var(BASE_URL_ENV).unwrap()).unwrap(),
                configs: AzureConfigs {
                    account_name: Value::Env(AZURE_ACCOUNT_NAME_ENV.to_string()),
                    account_key: Value::Env(AZURE_ACCOUNT_KEY_ENV.to_string()),
                    extra_configs: None,
                },
            };
            test_run_impl_copy(target).await;
        }
    }
}
