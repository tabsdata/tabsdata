//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use clap_derive::Args;
use td_common::status::ExitStatus::GeneralError;
use td_error::TdError;
use tracing::{info, warn};

use crate::base::constants::{CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY};
use crate::base::error::cli::CliError;
use crate::base::naming::to_configmap_name;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::core::system::exit::{CommandResult, abort_with};
use crate::core::system::file::is_hidden_or_system;
use crate::kube::api::{LabelFilter, get_configmaps, merge_configmap};
use crate::runtime::provider::{Provider, provider_for_or_tdkcontext};

#[derive(Debug, Clone, Args)]
pub struct PutArguments {
    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[command(flatten)]
    pub instance: InstanceArguments,

    #[arg(
        long,
        name = "folder",
        required = true,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Source folder. One file per config, the file name matching the \
                     configmap key (e.g. 'apiserver.yaml'). File contents become the \
                     new key value. Only files matching an existing config in the \
                     cluster are updated; anything else is ignored with a warning."
    )]
    pub folder: PathBuf,
}

pub async fn command_put(arguments: PutArguments, globals: &GlobalArguments) -> CommandResult {
    let PutArguments {
        cluster,
        instance,
        folder,
    } = arguments;

    ensure_source_folder(&folder)?;
    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    put_configs(provider.as_ref(), &folder).await
}

pub fn ensure_source_folder(folder: &PathBuf) -> CommandResult {
    if !folder.is_dir() {
        return abort_with(
            GeneralError,
            CliError::FolderNotFound(folder.display().to_string()),
        );
    }
    let mut folder_entries = std::fs::read_dir(folder).map_err(|error| {
        TdError::from(CliError::FolderReadFailed {
            folder: folder.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    if folder_entries.next().is_none() {
        return abort_with(
            GeneralError,
            CliError::FolderEmpty(folder.display().to_string()),
        );
    }
    Ok(())
}

pub async fn put_configs(provider: &dyn Provider, folder: &PathBuf) -> CommandResult {
    let context = provider.kube_context().await?;

    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY.item);
    let known_configs: BTreeSet<String> = get_configmaps(&context, Some(filter))
        .await?
        .into_iter()
        .map(|(name, _entries)| name)
        .collect();

    let mut warnings: Vec<String> = Vec::new();
    let mut updated = 0;

    for entry in read_sorted(folder)? {
        let entry_path = entry.path();
        if is_hidden_or_system(&entry_path) {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if entry_path.is_dir() {
            warnings.push(format!("Subfolder '{file_name}' ignored; expected a flat layout."));
            continue;
        }
        let configmap_name = to_configmap_name(&KPROFILE_CONFIGS_FAMILY, &file_name)?;
        if !known_configs.contains(&configmap_name) {
            warnings.push(format!("No config named '{configmap_name}' exists; ignored."));
            continue;
        }
        let bytes = match std::fs::read(&entry_path) {
            Ok(bytes) => bytes,
            Err(error) => {
                warnings.push(format!("Could not read file '{file_name}': {error}; ignored."));
                continue;
            }
        };
        let text = match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(_) => {
                warnings.push(format!("File '{file_name}' is not valid UTF-8; ignored."));
                continue;
            }
        };
        let mut updates: BTreeMap<String, String> = BTreeMap::new();
        updates.insert(file_name.clone(), text);
        merge_configmap(&context, &configmap_name, updates).await?;
        updated += 1;
    }

    for warning in &warnings {
        warn!("{warning}");
    }

    let noun = if updated == 1 { "config" } else { "configs" };
    info!("Updated {updated} tabsdata {noun}.");
    Ok(())
}

fn read_sorted(folder: &PathBuf) -> CommandResult<Vec<std::fs::DirEntry>> {
    let reader = std::fs::read_dir(folder).map_err(|error| {
        TdError::from(CliError::FolderReadFailed {
            folder: folder.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    let mut entries: Vec<std::fs::DirEntry> = reader.filter_map(|entry| entry.ok()).collect();
    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}
