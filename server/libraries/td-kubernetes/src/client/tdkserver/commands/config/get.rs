//
// Copyright 2026 Tabs Data Inc.
//

use std::path::PathBuf;

use clap_derive::Args;
use td_common::status::ExitStatus::GeneralError;
use td_error::TdError;
use tracing::info;

use crate::base::constants::{CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY};
use crate::base::error::cli::CliError;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::core::system::exit::{CommandResult, abort_with};
use crate::kube::api::{LabelFilter, get_configmaps};
use crate::runtime::provider::provider_for_or_tdkcontext;

#[derive(Debug, Clone, Args)]
pub struct GetArguments {
    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[command(flatten)]
    pub instance: InstanceArguments,

    #[arg(
        long,
        name = "folder",
        required = true,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Empty target folder. One file per config, the file name matching \
                     the configmap key (e.g. 'apiserver.yaml'). The configmap name is \
                     inferred as 'tabsdata-config-<file>'."
    )]
    pub folder: PathBuf,
}

pub async fn command_get(arguments: GetArguments, globals: &GlobalArguments) -> CommandResult {
    let GetArguments {
        cluster,
        instance,
        folder,
    } = arguments;

    if !folder.is_dir() {
        return abort_with(
            GeneralError,
            CliError::FolderNotFound(folder.display().to_string()),
        );
    }
    let mut entries = std::fs::read_dir(&folder).map_err(|error| {
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

    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    let context = provider.kube_context().await?;
    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY.kind);
    let configmaps = get_configmaps(&context, Some(filter)).await?;

    let mut count = 0usize;
    for (_name, entries) in configmaps {
        for (key, value) in entries {
            let file = folder.join(&key);
            std::fs::write(&file, value).map_err(|error| {
                TdError::from(CliError::FileWriteFailed {
                    file: file.display().to_string(),
                    cause: error.to_string(),
                })
            })?;
            count += 1;
        }
    }

    let noun = if count == 1 { "config" } else { "configs" };
    info!("Wrote {count} tabsdata {noun} to {}.", folder.display());
    Ok(())
}
