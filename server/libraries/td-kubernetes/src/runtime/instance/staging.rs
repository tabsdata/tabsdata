//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use tracing::info;

use crate::base::constants::{
    CONFIGMAP_FAMILY_LABEL, CONFIGMAP_TABSDATA_MANIFEST, CONFIGMAP_TABSDATA_SETTINGS, Family,
    KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY, SETTINGS_FILE,
};
use crate::base::error::provider::ProviderError;
use crate::base::naming::{id_from_file, to_kube_resource_name};
use crate::core::system::exit::CommandResult;
use crate::core::system::file::is_hidden_or_system;
use crate::kube::api::domain::labels::LabelFilter;
use crate::kube::api::entity::configmap::{
    create_configmap, delete_configmap, exists_configmap, list_configmaps,
};
use crate::kube::context::KubeContext;
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::profile::RenderedConfigmap;

pub struct StagedProfile {
    pub pods: RenderedConfigmap,
    pub assets: RenderedConfigmap,
    pub configs: RenderedConfigmap,
    pub manifest: Option<String>,
}

pub fn read_staged_family(stage: &Path, family: &Family) -> CommandResult<RenderedConfigmap> {
    let family_path = stage.join(family.folder);
    let mut data = BTreeMap::new();
    if !family_path.is_dir() {
        return Ok(data);
    }
    let entries = fs::read_dir(&family_path).map_err(|error| {
        ProviderError::StagingReadFailed(family_path.display().to_string(), error.to_string())
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            ProviderError::StagingReadFailed(family_path.display().to_string(), error.to_string())
        })?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if is_hidden_or_system(&path) {
            continue;
        }
        let file_type = entry.file_type().map_err(|error| {
            ProviderError::StagingReadFailed(path.display().to_string(), error.to_string())
        })?;
        if file_type.is_dir() {
            return Err(ProviderError::StagingFamilyNotFlat {
                family: family.folder.to_owned(),
                subfolder: name.to_owned(),
            }
            .into());
        }
        if !file_type.is_file() {
            continue;
        }
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                info!(
                    "Skipping non-UTF-8 file in staging folder: '{}'",
                    path.display()
                );
                continue;
            }
            Err(error) => {
                return Err(ProviderError::StagingReadFailed(
                    path.display().to_string(),
                    error.to_string(),
                )
                .into());
            }
        };
        data.insert(name.to_owned(), contents);
    }
    Ok(data)
}

pub fn read_staged_manifest(stage: &Path) -> CommandResult<Option<String>> {
    let manifest_path = stage.join(MANIFEST_FILE);
    if !manifest_path.is_file() {
        return Ok(None);
    }
    let contents = fs::read_to_string(&manifest_path).map_err(|error| {
        ProviderError::StagingReadFailed(manifest_path.display().to_string(), error.to_string())
    })?;
    Ok(Some(contents))
}

pub fn read_staged_profile(stage: &Path) -> CommandResult<Option<StagedProfile>> {
    if !stage.is_dir() {
        return Ok(None);
    }
    Ok(Some(StagedProfile {
        pods: read_staged_family(stage, &KPROFILE_PODS_FAMILY)?,
        assets: read_staged_family(stage, &KPROFILE_ASSETS_FAMILY)?,
        configs: read_staged_family(stage, &KPROFILE_CONFIGS_FAMILY)?,
        manifest: read_staged_manifest(stage)?,
    }))
}

pub async fn promote_staged_profile(
    context: &KubeContext,
    stage: &Path,
    instance: &Path,
) -> CommandResult<bool> {
    let Some(staged) = read_staged_profile(stage)? else {
        return Ok(false);
    };
    for (family, files) in [
        (&KPROFILE_PODS_FAMILY, staged.pods),
        (&KPROFILE_ASSETS_FAMILY, staged.assets),
        (&KPROFILE_CONFIGS_FAMILY, staged.configs),
    ] {
        replace_family_configmaps(context, family, files).await?;
    }
    if let Some(manifest_yaml) = staged.manifest {
        replace_manifest_configmap(context, manifest_yaml).await?;
    }
    replace_settings_configmap(context, instance).await?;
    Ok(true)
}

async fn replace_settings_configmap(context: &KubeContext, instance: &Path) -> CommandResult {
    let settings_file = instance.join(SETTINGS_FILE);
    let settings = fs::read_to_string(&settings_file).map_err(|error| {
        ProviderError::StagingReadFailed(settings_file.display().to_string(), error.to_string())
    })?;
    if exists_configmap(context, CONFIGMAP_TABSDATA_SETTINGS).await? {
        delete_configmap(context, CONFIGMAP_TABSDATA_SETTINGS).await?;
    }
    let mut data = BTreeMap::new();
    data.insert(SETTINGS_FILE.to_owned(), settings);
    create_configmap(
        context,
        CONFIGMAP_TABSDATA_SETTINGS,
        data,
        BTreeMap::new(),
        BTreeMap::new(),
        None,
        None,
        None,
    )
    .await?;
    Ok(())
}

pub async fn replace_family_configmaps(
    context: &KubeContext,
    family: &Family,
    files: RenderedConfigmap,
) -> CommandResult {
    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, family.kind);
    let existing = list_configmaps(context, Some(filter)).await?;
    for (name, _) in existing {
        delete_configmap(context, &name).await?;
    }
    for (file, content) in files {
        let id = id_from_file(&file);
        let name = to_kube_resource_name(family.scope, id, family.kind, &file)?;
        let mut data = BTreeMap::new();
        data.insert(file, content);
        create_configmap(
            context,
            &name,
            data,
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            Some(family.kind),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn replace_manifest_configmap(context: &KubeContext, manifest: String) -> CommandResult {
    if exists_configmap(context, CONFIGMAP_TABSDATA_MANIFEST).await? {
        delete_configmap(context, CONFIGMAP_TABSDATA_MANIFEST).await?;
    }
    let mut data = BTreeMap::new();
    data.insert(MANIFEST_FILE.to_owned(), manifest);
    create_configmap(
        context,
        CONFIGMAP_TABSDATA_MANIFEST,
        data,
        BTreeMap::new(),
        BTreeMap::new(),
        None,
        None,
        None,
    )
    .await?;
    Ok(())
}
