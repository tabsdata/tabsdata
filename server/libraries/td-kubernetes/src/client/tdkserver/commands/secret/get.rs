//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use clap_derive::{Args, ValueEnum};
use td_common::status::ExitStatus::GeneralError;
use td_error::TdError;
use tracing::info;

use crate::base::constants::SECRET_FAMILY_CONFIG;
use crate::base::error::cli::CliError;
use crate::base::error::secret::SecretError;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::render::report::component::secret::{
    ReportContext, ReportInput, settlement,
};
use crate::core::system::exit::{CommandResult, abort_with};
use crate::kube::api::secret::get_secret;
use crate::kube::context::KubeContext;
use crate::runtime::provider::provider_for_or_tdkcontext;
use crate::runtime::secrets::discovery::{SecretLeaf, extract_secret_leaves};
use crate::runtime::secrets::manifest::{build_secret_manifest, serialize_secret_manifest};
use crate::runtime::secrets::source::{SourceKind, read_configs, resolve_source};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SourceArg {
    Default,
    Current,
}

impl From<SourceArg> for SourceKind {
    fn from(value: SourceArg) -> Self {
        match value {
            SourceArg::Default => SourceKind::Default,
            SourceArg::Current => SourceKind::Current,
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct GetArguments {
    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[command(flatten)]
    pub instance: InstanceArguments,

    #[arg(
        long,
        value_enum,
        required = true,
        long_help = "Where to read the configs from. 'default' reads the on-disk kprofile shipped with this binary. \
                     'current' reads the deployed state: for kminus it reads the staging folder if present (instance \
                     never started yet), otherwise it queries the live cluster; for other providers it queries the \
                     live cluster. 'current' requires a running cluster (except when the kminus stage is present)."
    )]
    pub source: SourceArg,

    #[arg(
        long,
        default_value_t = false,
        long_help = "Populate the dumped manifests with the live Secret values. Without this flag, every key is dumped \
                     with an empty string. Plaintext secret values land on disk; the operator owns the file's location \
                     and permissions."
    )]
    pub values: bool,

    #[arg(
        long,
        name = "folder",
        required = true,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Empty target folder. One file per Secret, named after the Secret \
                     (e.g. 'tabsdata-secret-apiserver.yaml'). Each file is a complete \
                     Kubernetes Secret manifest ready to 'kubectl apply -f'."
    )]
    pub folder: PathBuf,
}

pub async fn command_get(arguments: GetArguments, globals: &GlobalArguments) -> CommandResult {
    let GetArguments {
        cluster,
        instance,
        source,
        values,
        folder,
    } = arguments;

    ensure_empty_folder(&folder)?;

    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    let resolved = resolve_source(provider.as_ref(), source.into()).await?;
    let configs = read_configs(provider.as_ref(), resolved, globals.verbose).await?;

    let cluster_alive = provider.cluster_alive().await;
    let kube_context = if cluster_alive {
        Some(provider.kube_context().await?)
    } else {
        None
    };

    let mut count = 0usize;
    for entry in configs {
        let mut yaml: serde_yaml::Value = serde_yaml::from_str(&entry.yaml).map_err(|error| {
            SecretError::ConfigYamlParseFailed {
                config: entry.file.clone(),
                cause: error.to_string(),
            }
        })?;
        let leaves = extract_secret_leaves(&entry.file, &mut yaml)?;
        if leaves.is_empty() {
            continue;
        }
        let live_values = if values && let Some(context) = kube_context.as_ref() {
            get_secret(context, &entry.secret).await?
        } else {
            None
        };
        let string_values = live_values.as_ref().map(secret_values_to_strings);
        let manifest = build_secret_manifest(
            &entry.secret,
            SECRET_FAMILY_CONFIG,
            &leaves,
            string_values.as_ref(),
        );
        let yaml_manifest = serialize_secret_manifest(&manifest)?;
        write_manifest(&folder, &entry.secret, &yaml_manifest)?;
        emit_report(kube_context.as_ref(), &entry.secret, &leaves).await?;
        count += 1;
    }

    let noun = if count == 1 { "Secret" } else { "Secrets" };
    info!("Wrote {count} tabsdata {noun} to {}.", folder.display());
    Ok(())
}

fn ensure_empty_folder(folder: &Path) -> CommandResult {
    if !folder.is_dir() {
        return abort_with(
            GeneralError,
            CliError::FolderNotFound(folder.display().to_string()),
        );
    }
    let mut entries = std::fs::read_dir(folder).map_err(|error| {
        TdError::from(CliError::FolderReadFailed {
            folder: folder.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    if entries.next().is_some() {
        return abort_with(
            GeneralError,
            CliError::FolderNotEmpty(folder.display().to_string()),
        );
    }
    Ok(())
}

fn write_manifest(folder: &Path, secret_name: &str, yaml: &str) -> CommandResult {
    let file = folder.join(secret_name);
    std::fs::write(&file, yaml).map_err(|error| {
        TdError::from(CliError::FileWriteFailed {
            file: file.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    Ok(())
}

async fn emit_report(
    kube_context: Option<&KubeContext>,
    secret_name: &str,
    leaves: &[SecretLeaf],
) -> CommandResult {
    let expected: Vec<String> = leaves.iter().map(|leaf| leaf.key.clone()).collect();
    let context = match kube_context {
        Some(ctx) => ReportContext::Live(ctx),
        None => ReportContext::ClusterDown,
    };
    let input = ReportInput {
        secret_name,
        expected_keys: &expected,
    };
    settlement(&context, &input).await?;
    Ok(())
}

fn secret_values_to_strings(live: &BTreeMap<String, Vec<u8>>) -> BTreeMap<String, String> {
    live.iter()
        .filter_map(|(key, value)| {
            std::str::from_utf8(value)
                .ok()
                .map(|text| (key.clone(), text.to_owned()))
        })
        .collect()
}
