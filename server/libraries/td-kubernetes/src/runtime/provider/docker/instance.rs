//
// Copyright 2026 Tabs Data Inc.
//

use std::sync::Arc;

use indicatif::MultiProgress;
use kube::Config;

use crate::core::system::exit::CommandResult;
use crate::kube::api::pod::list_pods;
use crate::kube::context::KubeContext;
use crate::runtime::instance::launcher::{start_workload, stop_workload};
use crate::runtime::instance::profile::{parse_from_configmaps, read_family_configmaps};
use crate::runtime::provider::{ClusterProvider, Instance};
use crate::runtime::reporting::build_kube_client;

use crate::base::constants::{
    CONFIGMAP_TABSDATA_MANIFEST, KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY, POD_PHASE_FAILED,
    POD_PHASE_SUCCEEDED,
};
use crate::base::error::provider::ProviderError;
use crate::kube::api::require_configmap;
use crate::runtime::instance::manifest::MANIFEST_FILE;

pub struct DockerInstance {
    config: Config,
}

impl DockerInstance {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    async fn kube_context(&self) -> CommandResult<KubeContext> {
        let namespace = self.config.default_namespace.clone();
        let client = build_kube_client(&self.config)?;
        Ok(KubeContext::new(Arc::new(client), namespace))
    }
}

impl Instance for DockerInstance {
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
        start_workload(&context, ClusterProvider::Docker, &profile, multi).await
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
