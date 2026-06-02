//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use clap_derive::Args;
use td_common::status::ExitStatus::GeneralError;
use td_error::TdError;
use tracing::{info, warn};

use crate::base::error::cli::CliError;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::core::system::exit::{Result, abort_with};
use crate::core::system::file::is_hidden_or_system;
use crate::kube::api::{get_configmaps, merge_configmap};
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
        long_help = "Source folder. One subfolder per config, one file per key, \
                     file contents being the new key value. Only existing configs \
                     and keys are updated; anything else is ignored with a warning."
    )]
    pub folder: PathBuf,
}

pub async fn command_put(arguments: PutArguments, globals: &GlobalArguments) -> Result {
    let PutArguments {
        cluster,
        instance,
        folder,
    } = arguments;

    ensure_source_folder(&folder)?;
    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    put_configs(provider.as_ref(), &folder).await
}

pub fn ensure_source_folder(folder: &PathBuf) -> Result {
    if !folder.is_dir() {
        return abort_with(GeneralError, CliError::FolderNotFound(folder.display().to_string()));
    }
    let mut folder_entries = std::fs::read_dir(folder).map_err(|error| {
        TdError::from(CliError::FolderReadFailed {
            folder: folder.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    if folder_entries.next().is_none() {
        return abort_with(GeneralError, CliError::FolderEmpty(folder.display().to_string()));
    }
    Ok(())
}

pub async fn put_configs(provider: &dyn Provider, folder: &PathBuf) -> Result {
    let context = provider.kube_context().await?;

    let keys_by_config: BTreeMap<String, BTreeSet<String>> = get_configmaps(&context)
        .await?
        .into_iter()
        .map(|(name, entries)| (name, entries.into_iter().map(|(key, _)| key).collect()))
        .collect();

    let mut warnings: Vec<String> = Vec::new();
    let mut updated = 0;

    for config_entry in read_sorted(folder)? {
        let config_path = config_entry.path();
        if is_hidden_or_system(&config_path) {
            continue;
        }
        let config_name = config_entry.file_name().to_string_lossy().into_owned();
        if !config_path.is_dir() {
            warnings.push(format!("Entry '{config_name}' is not a config folder; ignored."));
            continue;
        }
        let Some(existing_keys) = keys_by_config.get(&config_name) else {
            warnings.push(format!("No config named '{config_name}' exists; ignored."));
            continue;
        };

        let mut updates: BTreeMap<String, String> = BTreeMap::new();
        for key_entry in read_sorted(&config_path)? {
            let key_path = key_entry.path();
            if is_hidden_or_system(&key_path) {
                continue;
            }
            let key = key_entry.file_name().to_string_lossy().into_owned();
            if key_path.is_dir() {
                warnings.push(format!(
                    "Subfolder '{config_name}/{key}' ignored; nested folders are not supported."
                ));
                continue;
            }
            if !existing_keys.contains(&key) {
                warnings.push(format!(
                    "Key '{key}' does not exist in config '{config_name}'; file ignored."
                ));
                continue;
            }
            match std::fs::read(&key_path) {
                Ok(bytes) => match String::from_utf8(bytes) {
                    Ok(text) => {
                        updates.insert(key, text.trim().to_owned());
                    }
                    Err(_) => warnings.push(format!(
                        "File '{config_name}/{key}' is not valid UTF-8; ignored."
                    )),
                },
                Err(error) => warnings.push(format!(
                    "Could not read file '{config_name}/{key}': {error}; ignored."
                )),
            }
        }

        if !updates.is_empty() {
            let count = updates.len();
            merge_configmap(&context, &config_name, updates).await?;
            updated += count;
        }
    }

    for warning in &warnings {
        warn!("{warning}");
    }

    let noun = if updated == 1 { "key" } else { "keys" };
    info!("Updated {updated} tabsdata config {noun}.");
    Ok(())
}

fn read_sorted(folder: &PathBuf) -> Result<Vec<std::fs::DirEntry>> {
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
