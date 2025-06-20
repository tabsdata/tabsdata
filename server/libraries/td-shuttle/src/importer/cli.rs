//
// Copyright 2024 Tabs Data Inc.
//

use crate::importer::args::Params;
use crate::importer::logic::{create_object_store, find_files_to_import, run_import, ImportError};
use std::fs::File;
use std::io::{stdout, Write};
use std::sync::Arc;
use td_common::logging;
use td_common::status::ExitStatus;
use td_process::launcher::cli::{Cli, NoConfig};
use tracing::Level;

/// Run the importer. This function is the entry point for the `importer` binary.
pub fn run() {
    Cli::<NoConfig, Params>::exec_async(
        "importer",
        |_config, params| async move {
            // Initialize logging
            logging::start(Level::INFO, None, false);

            run_impl(params).await;

            ExitStatus::Success
        },
        None,
        None,
    );
}

async fn run_impl(params: Params) {
    let importer_options = Arc::new(params.importer_options().await);
    tracing::trace!("Starting importer \n {:#?}", importer_options);

    let object_store = create_object_store(&importer_options)
        .await
        .expect("Could not create object store");
    let files_to_import = find_files_to_import(object_store, &importer_options)
        .await
        .expect("Could not get files to import");
    tracing::info!("Found {} files to import", files_to_import.len());

    // TODO - parallelize the imports [TD-352]
    let _parallel = *importer_options.parallel();
    let import_reports: Vec<_> = tokio::task::spawn_blocking(move || {
        files_to_import
            .into_iter()
            .map(run_import)
            .collect::<Vec<_>>()
    })
    .await
    .expect("Could not run import files")
    .into_iter()
    .collect::<Result<_, ImportError>>()
    .expect("Could not import files");

    let report_as_json = serde_json::to_string_pretty(&import_reports).unwrap();

    let mut out: Box<dyn Write> = match importer_options.out() {
        None => Box::new(stdout()),
        Some(_out) if _out == "-" => Box::new(stdout()),
        Some(out) => Box::new(File::create(out).unwrap()),
    };
    out.write_all(report_as_json.as_bytes())
        .expect("Could not write out the imports report");
}

#[cfg(test)]
mod tests {
    use crate::importer::args::Params;
    use crate::importer::logic::{FileImportReport, TD_SYSTEM_COLUMNS};
    use clap::{command, Parser};
    use object_store::path::Path;
    use polars::df;
    use polars::prelude::{IntoLazy, LazyFrame, ParquetWriteOptions, ScanArgsParquet};
    use std::collections::{HashMap, HashSet};
    use std::fs::File;
    use td_common::absolute_path::AbsolutePath;
    use testdir::testdir;
    use url::Url;

    async fn test_run_impl(
        format: &str,
        url_to_import: &Url,
        location_configs: &HashMap<String, String>,
        expected_rows: usize,
        expected_columns: Vec<&'static str>,
    ) {
        #[derive(Debug, clap_derive::Parser)]
        #[command(version)]
        struct ParamsParser {
            #[command(flatten)]
            params: Params,
        }

        let test_dir = testdir!();

        let file_name = url_to_import
            .path_segments()
            .unwrap()
            .next_back()
            .unwrap()
            .to_string();

        let mut url_to_import = url_to_import.clone();
        url_to_import.path_segments_mut().unwrap().pop();
        let location = url_to_import.as_str();

        let location_configs_file = test_dir.join("location_configs.json");
        serde_json::to_writer(
            File::create(&location_configs_file).unwrap(),
            &location_configs,
        )
        .unwrap();
        let location_configs_file = location_configs_file.to_str().unwrap();

        let to = format!("file://{}", test_dir.to_str().unwrap());
        let report = format!("{}/report.json", test_dir.to_str().unwrap());
        let args = vec![
            "importer",
            "--location",
            &location,
            "--file-pattern",
            &file_name,
            "--format",
            format,
            "--to",
            &to,
            "--out",
            &report,
            "--location-configs",
            &location_configs_file,
        ];
        let params: Params = ParamsParser::try_parse_from(args).unwrap().params;

        super::run_impl(params).await;

        let report = test_dir.join("report.json");
        assert!(report.exists());
        let report: Vec<FileImportReport> =
            serde_json::from_reader(File::open(report).unwrap()).unwrap();
        assert_eq!(report.len(), 1);
        /*
        // Not used if system columns are not being generated...
        assert_eq!(*report[0].rows(), 2);
         */
        let written_to = Url::parse(report[0].to()).unwrap().to_file_path().unwrap();
        assert!(written_to.exists());

        tokio::task::spawn_blocking(move || {
            let url = Url::from_file_path(&written_to).unwrap().to_string();
            let frame = LazyFrame::scan_parquet(url, ScanArgsParquet::default()).unwrap();
            let frame = frame.collect().unwrap();
            assert_eq!(frame.height(), expected_rows);
            assert_eq!(frame.get_column_names_str(), expected_columns);
        })
        .await
        .unwrap();
    }

    async fn write_to_object_store(url: &Url, data: Vec<u8>, configs: &HashMap<String, String>) {
        let object_store = object_store::parse_url_opts(url, configs).unwrap().0;
        object_store
            .put(&Path::from(url.abs_path()), data.into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_run_impl_local_csv() {
        let test_dir = testdir!();
        let url = Url::from_file_path(test_dir.join("input.csv")).unwrap();

        let data = r#"0,1,2
a,b,c
A,B,C"#;
        write_to_object_store(&url, data.as_bytes().into(), &HashMap::new()).await;

        test_run_impl(
            "csv",
            &url,
            &HashMap::new(),
            2,
            ["0", "1", "2"]
                .into_iter()
                .chain(TD_SYSTEM_COLUMNS)
                .collect(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_run_impl_local_json() {
        let test_dir = testdir!();
        let url = Url::from_file_path(test_dir.join("input.json")).unwrap();

        let data = r##"{"A": "a0", "B": "b0"}
{"A": "a1", "B": "b1"}"##;
        write_to_object_store(&url, data.as_bytes().into(), &HashMap::new()).await;

        test_run_impl(
            "nd-json",
            &url,
            &HashMap::new(),
            2,
            ["A", "B"].into_iter().chain(TD_SYSTEM_COLUMNS).collect(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_run_impl_local_log() {
        let test_dir = testdir!();
        let url = Url::from_file_path(test_dir.join("input.log")).unwrap();

        let data = r"2024-11-18T21:12:44.754411Z DEBUG message 1
2024-11-18T21:12:44.757657Z DEBUG message 2";
        write_to_object_store(&url, data.as_bytes().into(), &HashMap::new()).await;

        test_run_impl(
            "log",
            &url,
            &HashMap::new(),
            2,
            ["file", "message"]
                .into_iter()
                .chain(TD_SYSTEM_COLUMNS)
                .collect(),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_run_impl_local_parquet() {
        let test_dir = testdir!();
        let url = Url::from_file_path(test_dir.join("input.parquet")).unwrap();

        let url_for_task = url.clone();
        tokio::task::block_in_place(move || {
            let df = df!("A" => ["a0", "a1"],"B" => ["b0", "B1"]).unwrap();

            df.lazy()
                .sink_parquet_cloud(
                    url_for_task.to_string(),
                    None,
                    ParquetWriteOptions::default(),
                )
                .unwrap();
        });

        test_run_impl(
            "parquet",
            &url,
            &HashMap::new(),
            2,
            ["A", "B"].into_iter().chain(TD_SYSTEM_COLUMNS).collect(),
        )
        .await;
    }

    async fn test_run_impl_cloud(
        test_name: &str,
        env_prefix: &str,
        envs: HashSet<&str>,
        format: &str,
        data: Vec<u8>,
        expected_rows: usize,
        expected_columns: Vec<&'static str>,
    ) {
        let cloud_envs = std::env::vars()
            .filter(|(name, _value)| name.starts_with(env_prefix))
            .map(|(name, value)| (name.strip_prefix(env_prefix).unwrap().to_string(), value))
            .collect::<HashMap<String, String>>();
        let mut cloud_configs = cloud_envs
            .into_iter()
            .map(|(name, value)| (name.to_lowercase(), value))
            .collect::<HashMap<_, _>>();
        if cloud_configs.len() != envs.len() {
            println!(
                ">>>>>>>> !!!!Skipping test {} because the following ENVs are not set: {:?}",
                test_name,
                envs.iter()
                    .map(|s| format!("{}{}", env_prefix, s))
                    .collect::<Vec<_>>()
            );
        } else {
            let base_url = cloud_configs.remove("base_url").unwrap();

            let file_to_import = Url::parse(&format!("{}/input.{}", base_url, format)).unwrap();
            write_to_object_store(&file_to_import, data, &cloud_configs).await;

            test_run_impl(
                format,
                &file_to_import,
                &cloud_configs,
                expected_rows,
                expected_columns,
            )
            .await;
        }
    }

    async fn test_run_impl_cloud_csv(test_name: &str, env_prefix: &str, envs: HashSet<&str>) {
        let data = r#"0,1,2
a,b,c
A,B,C"#;
        test_run_impl_cloud(
            test_name,
            env_prefix,
            envs,
            "csv",
            data.as_bytes().into(),
            2,
            vec!["0", "1", "2"]
                .into_iter()
                .chain(TD_SYSTEM_COLUMNS)
                .collect(),
        )
        .await;
    }

    async fn test_run_impl_cloud_log(test_name: &str, env_prefix: &str, envs: HashSet<&str>) {
        let data = r"2024-11-18T21:12:44.754411Z DEBUG message 1
2024-11-18T21:12:44.757657Z DEBUG message 2";
        test_run_impl_cloud(
            test_name,
            env_prefix,
            envs,
            "log",
            data.as_bytes().into(),
            2,
            ["file", "message"]
                .into_iter()
                .chain(TD_SYSTEM_COLUMNS)
                .collect(),
        )
        .await;
    }

    async fn test_run_impl_cloud_json(test_name: &str, env_prefix: &str, envs: HashSet<&str>) {
        let data = r##"{"A": "a0", "B": "b0"}
{"A": "a1", "B": "b1"}"##;
        test_run_impl_cloud(
            test_name,
            env_prefix,
            envs,
            "nd-json",
            data.as_bytes().into(),
            2,
            vec!["A", "B"]
                .into_iter()
                .chain(TD_SYSTEM_COLUMNS)
                .collect(),
        )
        .await;
    }

    async fn test_run_impl_cloud_parquet(test_name: &str, env_prefix: &str, envs: HashSet<&str>) {
        let test_dir = testdir!();
        let url = Url::from_file_path(test_dir.join("input.parquet")).unwrap();

        let url_for_task = url.clone();
        tokio::task::block_in_place(move || {
            let df = df!("A" => ["a0", "a1"],"B" => ["b0", "B1"]).unwrap();

            df.lazy()
                .sink_parquet_cloud(
                    url_for_task.to_string(),
                    None,
                    ParquetWriteOptions::default(),
                )
                .unwrap();
        });
        let data = tokio::fs::read(url.to_file_path().unwrap()).await.unwrap();

        test_run_impl_cloud(
            test_name,
            env_prefix,
            envs,
            "parquet",
            data,
            2,
            vec!["A", "B"]
                .into_iter()
                .chain(TD_SYSTEM_COLUMNS)
                .collect(),
        )
        .await;
    }

    /// Tests the importer with AWS S3
    ///
    /// The following environment variables must be set:
    ///
    /// - `IMPORTER_AWS_BASE_URL`: A s3:// URL, for example: "s3://tabsdata-tucu-test/importer"
    /// - `IMPORTER_AWS_REGION`: The AWS region, for example: "eu-north-1"
    /// - `IMPORTER_AWS_ACCESS_KEY_ID`: The AWS access key ID
    /// - `IMPORTER_AWS_SECRET_ACCESS_KEY`: The AWS secret access key
    #[tokio::test]
    async fn test_run_impl_aws_csv() {
        let env_prefix = "IMPORTER_AWS_";
        let envs = vec![
            "BASE_URL",
            "AWS_REGION",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ]
        .into_iter()
        .collect::<HashSet<_>>();
        test_run_impl_cloud_csv("importer AWS", env_prefix, envs).await;
    }

    /// Tests the importer with Azure File storage
    ///
    /// The following environment variables must be set:
    ///
    /// - `IMPORTER_AZURE_BASE_URL`: An az:// URL, for example: "az://tucutest/importer"
    /// - `IMPORTER_AZURE_ACCOUNT_NAME`: The Azure account name, for example: "tabsdatadev"
    /// - `IMPORTER_AZURE_ACCOUNT_KEY`: The Azure account key

    #[tokio::test]
    async fn test_run_impl_azure_csv() {
        let env_prefix = "IMPORTER_AZURE_";
        let envs = vec!["BASE_URL", "AZURE_ACCOUNT_NAME", "AZURE_ACCOUNT_KEY"]
            .into_iter()
            .collect::<HashSet<_>>();
        test_run_impl_cloud_csv("importer Azure", env_prefix, envs).await;
    }

    /// Tests the importer with AWS S3
    ///
    /// The following environment variables must be set:
    ///
    /// - `IMPORTER_AWS_BASE_URL`: A s3:// URL, for example: "s3://tabsdata-tucu-test/importer"
    /// - `IMPORTER_AWS_REGION`: The AWS region, for example: "eu-north-1"
    /// - `IMPORTER_AWS_ACCESS_KEY_ID`: The AWS access key ID
    /// - `IMPORTER_AWS_SECRET_ACCESS_KEY`: The AWS secret access key
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_run_impl_aws_parquet() {
        let env_prefix = "IMPORTER_AWS_";
        let envs = vec![
            "BASE_URL",
            "AWS_REGION",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ]
        .into_iter()
        .collect::<HashSet<_>>();
        test_run_impl_cloud_parquet("importer AWS", env_prefix, envs).await;
    }

    /// Tests the importer with Azure File storage
    ///
    /// The following environment variables must be set:
    ///
    /// - `IMPORTER_AZURE_BASE_URL`: An az:// URL, for example: "az://tucutest/importer"
    /// - `IMPORTER_AZURE_ACCOUNT_NAME`: The Azure account name, for example: "tabsdatadev"
    /// - `IMPORTER_AZURE_ACCOUNT_KEY`: The Azure account key

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_run_impl_azure_parquet() {
        let env_prefix = "IMPORTER_AZURE_";
        let envs = vec!["BASE_URL", "AZURE_ACCOUNT_NAME", "AZURE_ACCOUNT_KEY"]
            .into_iter()
            .collect::<HashSet<_>>();
        test_run_impl_cloud_parquet("importer Azure", env_prefix, envs).await;
    }

    /// Tests the importer with AWS S3
    ///
    /// The following environment variables must be set:
    ///
    /// - `IMPORTER_AWS_BASE_URL`: A s3:// URL, for example: "s3://tabsdata-tucu-test/importer"
    /// - `IMPORTER_AWS_REGION`: The AWS region, for example: "eu-north-1"
    /// - `IMPORTER_AWS_ACCESS_KEY_ID`: The AWS access key ID
    /// - `IMPORTER_AWS_SECRET_ACCESS_KEY`: The AWS secret access key
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_run_impl_aws_json() {
        let env_prefix = "IMPORTER_AWS_";
        let envs = vec![
            "BASE_URL",
            "AWS_REGION",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ]
        .into_iter()
        .collect::<HashSet<_>>();
        test_run_impl_cloud_json("importer AWS", env_prefix, envs).await;
    }

    /// Tests the importer with Azure File storage
    ///
    /// The following environment variables must be set:
    ///
    /// - `IMPORTER_AZURE_BASE_URL`: An az:// URL, for example: "az://tucutest/importer"
    /// - `IMPORTER_AZURE_ACCOUNT_NAME`: The Azure account name, for example: "tabsdatadev"
    /// - `IMPORTER_AZURE_ACCOUNT_KEY`: The Azure account key

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_run_impl_azure_json() {
        let env_prefix = "IMPORTER_AZURE_";
        let envs = vec!["BASE_URL", "AZURE_ACCOUNT_NAME", "AZURE_ACCOUNT_KEY"]
            .into_iter()
            .collect::<HashSet<_>>();
        test_run_impl_cloud_json("importer Azure", env_prefix, envs).await;
    }

    /// Tests the importer with AWS S3
    ///
    /// The following environment variables must be set:
    ///
    /// - `IMPORTER_AWS_BASE_URL`: A s3:// URL, for example: "s3://tabsdata-tucu-test/importer"
    /// - `IMPORTER_AWS_REGION`: The AWS region, for example: "eu-north-1"
    /// - `IMPORTER_AWS_ACCESS_KEY_ID`: The AWS access key ID
    /// - `IMPORTER_AWS_SECRET_ACCESS_KEY`: The AWS secret access key
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_run_impl_aws_log() {
        let env_prefix = "IMPORTER_AWS_";
        let envs = vec![
            "BASE_URL",
            "AWS_REGION",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ]
        .into_iter()
        .collect::<HashSet<_>>();
        test_run_impl_cloud_log("importer AWS", env_prefix, envs).await;
    }

    /// Tests the importer with Azure File storage
    ///
    /// The following environment variables must be set:
    ///
    /// - `IMPORTER_AZURE_BASE_URL`: An az:// URL, for example: "az://tucutest/importer"
    /// - `IMPORTER_AZURE_ACCOUNT_NAME`: The Azure account name, for example: "tabsdatadev"
    /// - `IMPORTER_AZURE_ACCOUNT_KEY`: The Azure account key

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_run_impl_azure_log() {
        let env_prefix = "IMPORTER_AZURE_";
        let envs = vec!["BASE_URL", "AZURE_ACCOUNT_NAME", "AZURE_ACCOUNT_KEY"]
            .into_iter()
            .collect::<HashSet<_>>();
        test_run_impl_cloud_log("importer Azure", env_prefix, envs).await;
    }
}
