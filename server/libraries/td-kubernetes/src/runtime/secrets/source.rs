//
// Copyright 2026 Tabs Data Inc.
//

use std::fs;
use std::path::Path;

use crate::base::constants::{
    CONFIGMAP_FAMILY_LABEL, Family, KPROFILE_CONFIGS_FAMILY, KUBE_NAME_SEPARATOR,
    TABSDATA_SECRET_PREFIX,
};
use crate::base::error::provider::ProviderError;
use crate::base::error::secret::SecretError;
use crate::base::naming::to_configmap_file;
use crate::core::system::exit::CommandError;
use crate::core::system::file::is_hidden_or_system;
use crate::kube::api::{LabelFilter, get_configmaps};
use crate::kube::context::KubeContext;
use crate::runtime::instance::profile::load_profile;
use crate::runtime::provider::{ClusterProvider, Provider};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Default,
    Current,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedSource {
    Kprofile,
    Stage,
    Configmaps,
}

#[derive(Debug, Clone)]
pub struct ConfigEntry {
    pub file: String,
    pub yaml: String,
    pub secret: String,
}

pub async fn resolve_source(
    provider: &dyn Provider,
    source: SourceKind,
) -> Result<ResolvedSource, SecretError> {
    if source == SourceKind::Default {
        return Ok(ResolvedSource::Kprofile);
    }
    let cluster_alive = provider.cluster_alive().await;
    let staging = provider
        .staging_profile_path()
        .filter(|path| path.is_dir());
    match (provider.cluster_provider(), cluster_alive, staging) {
        (ClusterProvider::Kminus, true, Some(path)) => Err(SecretError::KminusInconsistentState {
            staging_path: path.display().to_string(),
        }),
        (ClusterProvider::Kminus, false, Some(_)) => Ok(ResolvedSource::Stage),
        (_, true, _) => Ok(ResolvedSource::Configmaps),
        (ClusterProvider::Kminus, false, None) => Err(ProviderError::ClusterNotRunningKminus.into()),
        (other, false, _) => Err(ProviderError::ClusterNotRunningExternal {
            provider: other.as_str().to_owned(),
        }
        .into()),
    }
}

pub async fn read_configs(
    provider: &dyn Provider,
    source: ResolvedSource,
    verbose: bool,
) -> Result<Vec<ConfigEntry>, SecretError> {
    match source {
        ResolvedSource::Kprofile => read_configs_from_kprofile(provider, verbose),
        ResolvedSource::Stage => read_configs_from_stage(provider),
        ResolvedSource::Configmaps => read_configs_from_configmaps(provider).await,
    }
}

fn read_configs_from_kprofile(
    provider: &dyn Provider,
    verbose: bool,
) -> Result<Vec<ConfigEntry>, SecretError> {
    let profile = load_profile(provider.cluster_provider(), verbose)?;
    let mut entries: Vec<ConfigEntry> = profile
        .configs
        .into_iter()
        .map(|(file, yaml)| ConfigEntry {
            secret: to_secret_name(&file),
            file,
            yaml,
        })
        .collect();
    entries.sort_by(|left, right| left.file.cmp(&right.file));
    Ok(entries)
}

fn read_configs_from_stage(provider: &dyn Provider) -> Result<Vec<ConfigEntry>, SecretError> {
    let Some(staging) = provider.staging_profile_path() else {
        return Ok(Vec::new());
    };
    let configs_folder = staging.join(KPROFILE_CONFIGS_FAMILY.folder);
    if !configs_folder.is_dir() {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    for entry in read_sorted_files(&configs_folder)? {
        let path = entry.path();
        if is_hidden_or_system(&path) {
            continue;
        }
        let file = entry.file_name().to_string_lossy().into_owned();
        let yaml = fs::read_to_string(&path).map_err(|error| SecretError::StageReadFailed {
            path: path.display().to_string(),
            cause: error.to_string(),
        })?;
        entries.push(ConfigEntry {
            secret: to_secret_name(&file),
            file,
            yaml,
        });
    }
    Ok(entries)
}

async fn read_configs_from_configmaps(
    provider: &dyn Provider,
) -> Result<Vec<ConfigEntry>, SecretError> {
    let context = provider
        .kube_context()
        .await
        .map_err(|error| SecretError::CurrentSourceReadFailed {
            cause: command_error_to_string(&error),
        })?;
    read_configs_via_context(&context, &KPROFILE_CONFIGS_FAMILY).await
}

async fn read_configs_via_context(
    context: &KubeContext,
    family: &Family,
) -> Result<Vec<ConfigEntry>, SecretError> {
    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, family.item);
    let configmaps = get_configmaps(context, Some(filter)).await?;
    let mut entries = Vec::new();
    for (configmap_name, items) in configmaps {
        let file = to_configmap_file(family, &configmap_name)?.to_owned();
        for (_key, value) in items {
            let yaml = String::from_utf8(value).map_err(|error| {
                SecretError::ConfigmapEntryNotUtf8 {
                    configmap: configmap_name.clone(),
                    cause: error.to_string(),
                }
            })?;
            entries.push(ConfigEntry {
                secret: to_secret_name(&file),
                file: file.clone(),
                yaml,
            });
        }
    }
    entries.sort_by(|left, right| left.file.cmp(&right.file));
    Ok(entries)
}

pub fn to_secret_name(file: &str) -> String {
    format!(
        "{TABSDATA_SECRET_PREFIX}{}{KUBE_NAME_SEPARATOR}{file}",
        KPROFILE_CONFIGS_FAMILY.item
    )
}

fn read_sorted_files(folder: &Path) -> Result<Vec<fs::DirEntry>, SecretError> {
    let reader = fs::read_dir(folder).map_err(|error| SecretError::StageReadFailed {
        path: folder.display().to_string(),
        cause: error.to_string(),
    })?;
    let mut entries: Vec<fs::DirEntry> = reader.filter_map(|entry| entry.ok()).collect();
    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}

fn command_error_to_string(error: &CommandError) -> String {
    match error.source() {
        Some(td) => td.to_string(),
        None => format!("{:?}", error.status()),
    }
}
