//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use indicatif::MultiProgress;

use crate::base::constants::{
    CONFIGMAP_TABSDATA_MANIFEST, FOLDER_PROFILE, KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY,
    KPROFILE_PODS_FAMILY, POD_PHASE_FAILED, POD_PHASE_SUCCEEDED,
};
use crate::base::error::provider::ProviderError;
use crate::client::registry::{InstanceSettings, init_stage_folder_in};
use crate::core::system::exit::CommandResult;
use crate::kube::api::entity::configmap::require_configmap;
use crate::kube::api::entity::pod::list_pods;
use crate::kube::context::KubeContext;
use crate::kube::kubeconfig::build_kubeconfig;
use crate::runtime::instance::launcher::{start_workload, stop_workload};
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::ports::apply_ports;
use crate::runtime::instance::profile::{parse_from_configmaps, read_family_configmaps};
use crate::runtime::instance::staging::promote_staged_profile;
use crate::runtime::provider::kminus::system_values_for;
use crate::runtime::provider::{ClusterProvider, Instance};
use crate::runtime::reporting::build_kube_client;

pub struct KminusInstance {
    kubefile: PathBuf,
    staging_path: PathBuf,
    folder: PathBuf,
}

impl KminusInstance {
    pub fn new(
        kubefile: impl Into<PathBuf>,
        staging_path: impl Into<PathBuf>,
        folder: impl Into<PathBuf>,
    ) -> Self {
        Self {
            kubefile: kubefile.into(),
            staging_path: staging_path.into(),
            folder: folder.into(),
        }
    }

    fn staging_profile_path(&self) -> PathBuf {
        self.staging_path.join(FOLDER_PROFILE)
    }

    async fn kube_context(&self) -> CommandResult<KubeContext> {
        let kminus_config = build_kubeconfig(Some(&self.kubefile), None, None).await?;
        let namespace = kminus_config.default_namespace.clone();
        let client = build_kube_client(&kminus_config)?;
        Ok(KubeContext::new(Arc::new(client), namespace))
    }

    pub async fn promote_stage(&self) -> CommandResult {
        let staged_folder = self.staging_profile_path();
        let context = self.kube_context().await?;
        if !promote_staged_profile(&context, &staged_folder, &self.folder).await? {
            return Ok(());
        }
        fs::remove_dir_all(&self.staging_path).map_err(|error| {
            ProviderError::StagingFolderRemoveFailed(
                self.staging_path.display().to_string(),
                error.to_string(),
            )
        })?;
        Ok(())
    }

    pub async fn load_stage_to_kube(&self) -> CommandResult {
        let context = self.kube_context().await?;
        promote_staged_profile(&context, &init_stage_folder_in(&self.folder), &self.folder).await?;
        Ok(())
    }
}

impl Instance for KminusInstance {
    async fn start(&self, multi: &MultiProgress) -> CommandResult {
        let context = self.kube_context().await?;
        let pods = read_family_configmaps(&context, &KPROFILE_PODS_FAMILY).await?;
        let assets = read_family_configmaps(&context, &KPROFILE_ASSETS_FAMILY).await?;
        let configs = read_family_configmaps(&context, &KPROFILE_CONFIGS_FAMILY).await?;
        let mut manifest_data = require_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await?;
        let Some(manifest_yaml) = manifest_data.remove(MANIFEST_FILE) else {
            return Err(ProviderError::ConfigmapEntryMissing(
                CONFIGMAP_TABSDATA_MANIFEST.to_owned(),
                MANIFEST_FILE.to_owned(),
            )
            .into());
        };
        let profile = parse_from_configmaps(&pods, &assets, &configs, &manifest_yaml)?;

        let instance_name = self
            .folder
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.to_owned())
            .unwrap_or_default();
        let mut system = system_values_for(&self.folder)?;
        let settings = InstanceSettings::load(&instance_name)?;
        apply_ports(&mut system, &settings.ports);

        start_workload(&context, ClusterProvider::Kminus, &system, &profile, multi).await
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
            let terminating = pod.metadata.deletion_timestamp.is_some();
            let settled = matches!(
                pod.status
                    .as_ref()
                    .and_then(|status| status.phase.as_deref()),
                Some(POD_PHASE_SUCCEEDED) | Some(POD_PHASE_FAILED),
            );
            !terminating && !settled
        })
    }
}
