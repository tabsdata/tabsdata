//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use indicatif::MultiProgress;
use tracing::info;

use crate::base::constants::{
    CONFIGMAP_FAMILY_LABEL, CONFIGMAP_TABSDATA_MANIFEST, FOLDER_PROFILE, Family,
    KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY, POD_PHASE_FAILED, POD_PHASE_SUCCEEDED,
};
use crate::base::error::provider::ProviderError;
use crate::base::naming::{id_from_file, to_kube_resource_name};
use crate::core::system::exit::CommandResult;
use crate::core::system::file::is_hidden_or_system;
use crate::kube::api::pod::list_pods;
use crate::kube::api::{
    LabelFilter, create_configmap, delete_configmap, exists_configmap, list_configmaps,
    require_configmap,
};
use crate::kube::context::KubeContext;
use crate::kube::kubeconfig::build_kubeconfig;
use crate::runtime::instance::launcher::{start_workload, stop_workload};
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::profile::{
    RenderedConfigmap, parse_from_configmaps, read_family_configmaps,
};
use crate::runtime::provider::{ClusterProvider, Instance};
use crate::runtime::reporting::build_kube_client;

pub struct KminusInstance {
    kubefile: PathBuf,
    staging_path: PathBuf,
}

impl KminusInstance {
    pub fn new(kubefile: impl Into<PathBuf>, staging_path: impl Into<PathBuf>) -> Self {
        Self {
            kubefile: kubefile.into(),
            staging_path: staging_path.into(),
        }
    }

    fn staging_profile_path(&self) -> PathBuf {
        self.staging_path.join(FOLDER_PROFILE)
    }

    fn read_staged_profile(&self) -> CommandResult<Option<StagedConfigmaps>> {
        let staged_folder = self.staging_profile_path();
        if !staged_folder.is_dir() {
            return Ok(None);
        }
        let pods = read_staged_family(&staged_folder, &KPROFILE_PODS_FAMILY)?;
        let configs = read_staged_family(&staged_folder, &KPROFILE_CONFIGS_FAMILY)?;
        let manifest = read_staged_manifest(&staged_folder)?;
        if pods.is_empty() && configs.is_empty() && manifest.is_none() {
            Ok(None)
        } else {
            Ok(Some(StagedConfigmaps {
                pods,
                configs,
                manifest,
            }))
        }
    }

    async fn kube_context(&self) -> CommandResult<KubeContext> {
        let kminus_config = build_kubeconfig(Some(&self.kubefile), None, None).await?;
        let namespace = kminus_config.default_namespace.clone();
        let client = build_kube_client(&kminus_config)?;
        Ok(KubeContext::new(Arc::new(client), namespace))
    }

    pub async fn promote_stage(&self) -> CommandResult {
        let staged = self.read_staged_profile()?;
        let Some(staged) = staged else {
            return Ok(());
        };
        self.replace_configmaps(staged).await?;
        fs::remove_dir_all(&self.staging_path).map_err(|error| {
            ProviderError::StagingFolderRemoveFailed(
                self.staging_path.display().to_string(),
                error.to_string(),
            )
        })?;
        Ok(())
    }

    async fn replace_configmaps(&self, staged: StagedConfigmaps) -> CommandResult {
        let context = self.kube_context().await?;
        for (family, files) in [
            (&KPROFILE_PODS_FAMILY, staged.pods),
            (&KPROFILE_CONFIGS_FAMILY, staged.configs),
        ] {
            replace_family_configmaps(&context, family, files).await?;
        }
        if let Some(manifest_yaml) = staged.manifest {
            if exists_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await? {
                delete_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await?;
            }
            let mut manifest_data = BTreeMap::new();
            manifest_data.insert(MANIFEST_FILE.to_owned(), manifest_yaml);
            create_configmap(
                &context,
                CONFIGMAP_TABSDATA_MANIFEST,
                manifest_data,
                BTreeMap::new(),
                None,
                None,
            )
            .await?;
        }
        Ok(())
    }
}

struct StagedConfigmaps {
    pods: RenderedConfigmap,
    configs: RenderedConfigmap,
    manifest: Option<String>,
}

async fn replace_family_configmaps(
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
            None,
            Some(family.kind),
        )
        .await?;
    }
    Ok(())
}

fn read_staged_family(staged_folder: &Path, family: &Family) -> CommandResult<RenderedConfigmap> {
    let family_path = staged_folder.join(family.folder);
    let mut data = BTreeMap::new();
    if !family_path.is_dir() {
        return Ok(data);
    }
    let read = fs::read_dir(&family_path).map_err(|error| {
        ProviderError::StagingReadFailed(family_path.display().to_string(), error.to_string())
    })?;
    for entry in read {
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

fn read_staged_manifest(staged_folder: &Path) -> CommandResult<Option<String>> {
    let manifest_path = staged_folder.join(MANIFEST_FILE);
    if !manifest_path.is_file() {
        return Ok(None);
    }
    let contents = fs::read_to_string(&manifest_path).map_err(|error| {
        ProviderError::StagingReadFailed(manifest_path.display().to_string(), error.to_string())
    })?;
    Ok(Some(contents))
}

impl Instance for KminusInstance {
    async fn start(&self, multi: &MultiProgress) -> CommandResult {
        let context = self.kube_context().await?;
        let pods = read_family_configmaps(&context, &KPROFILE_PODS_FAMILY).await?;
        let configs = read_family_configmaps(&context, &KPROFILE_CONFIGS_FAMILY).await?;
        let mut manifest_data = require_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await?;
        let Some(manifest_yaml) = manifest_data.remove(MANIFEST_FILE) else {
            return Err(ProviderError::ConfigmapEntryMissing(
                CONFIGMAP_TABSDATA_MANIFEST.to_owned(),
                MANIFEST_FILE.to_owned(),
            )
            .into());
        };
        let profile = parse_from_configmaps(&pods, &configs, &manifest_yaml)?;
        start_workload(&context, ClusterProvider::Kminus, &profile, multi).await
    }

    async fn stop(&self, multi: &MultiProgress) -> CommandResult<bool> {
        let context = self.kube_context().await?;
        stop_workload(&context, multi).await
    }

    async fn is_alive(&self) -> bool {
        let Ok(context) = self.kube_context().await else {
            return false;
        };
        let Ok(pods) = list_pods(&context).await else {
            return false;
        };
        pods.iter().any(|pod| {
            !matches!(
                pod.status
                    .as_ref()
                    .and_then(|status| status.phase.as_deref()),
                Some(POD_PHASE_SUCCEEDED) | Some(POD_PHASE_FAILED),
            )
        })
    }
}
