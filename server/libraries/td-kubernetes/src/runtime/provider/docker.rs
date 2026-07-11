//
// Copyright 2026 Tabs Data Inc.
//

pub mod cluster;
pub mod instance;

use std::path::PathBuf;

use async_trait::async_trait;
use indicatif::MultiProgress;
use kube::Config;
use td_common::settings::DEFAULT_INSTANCE;
use td_common::status::ExitStatus::NoAction;

use std::collections::BTreeMap;

use crate::base::constants::{
    CONFIGMAP_TABSDATA_MANIFEST, DEFAULT_INSTANCE_NAME, Family, KPROFILE_CONFIGS_FAMILY,
    KPROFILE_PODS_FAMILY,
};
use crate::base::error::provider::ProviderError;
use crate::base::naming::{id_from_file, to_kube_resource_name};
use crate::base::reporter::Reporter;
use crate::client::registry::ClusterViews;
use crate::client::session::pin::{DockerInputs, Session};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::core::system::exit::{CommandResult, abort_with};
use crate::kube::api::configmap::get_configmap;
use crate::kube::api::{
    create_configmap, ensure_cluster_reachable, ensure_namespace_exists, exists_configmap,
};
use crate::kube::kubeconfig::{
    build_kubeconfig, resolve_cluster_name, resolve_current_context, resolve_kubefile,
};
use crate::runtime::gateway::resolver::{ConfigurationResolver, SystemValues};
use crate::runtime::instance::cluster::{ClusterDescriptor, ClusterProperties, ClusterSettings};
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::profile::{RenderedConfigmap, load_profile, render_for_configmaps};
use crate::runtime::instance::verify_manifest_compatibility;
use crate::runtime::provider::docker::cluster::DockerCluster;
use crate::runtime::provider::docker::instance::DockerInstance;
use crate::runtime::provider::outcome::{ClusterStopOutcome, InstanceStopOutcome};
use crate::runtime::provider::summary::Summary;
use crate::runtime::provider::{
    Cluster, ClusterProvider, ContainerProvider, HostedProvider, Instance, LocalProvider, Provider,
    Transaction, handle_phase_secrets,
};
use crate::runtime::secrets::{SecretGenerator, SecretPhase};

pub struct DockerProvider {
    kubefile: PathBuf,
    context: String,
    cluster_name: String,
    config: Config,
    cluster: DockerCluster,
    instance: DockerInstance,
    inputs: DockerInputs,
    verbose: bool,
}

impl DockerProvider {
    pub async fn build(
        cluster: ClusterArguments,
        _instance: InstanceArguments,
        verbose: bool,
    ) -> CommandResult<Self> {
        if cluster.kubeconfig.is_none() && cluster.context.is_none() && !cluster.current {
            return Err(ProviderError::ArgumentRequired {
                argument: "kubeconfig | context | current".to_owned(),
                provider: ClusterProvider::Docker.as_str().to_owned(),
            }
            .into());
        }
        if cluster.context.is_some() && cluster.current {
            return Err(ProviderError::MutuallyExclusiveArguments {
                first: "context".to_owned(),
                second: "current".to_owned(),
            }
            .into());
        }

        let inputs = DockerInputs {
            kubeconfig: cluster.kubeconfig.clone(),
            context: cluster.context.clone(),
            current: cluster.current,
            namespace: cluster.namespace.clone(),
        };

        let kubefile_argument = cluster.kubeconfig.as_deref();
        let context_argument = cluster.context.as_deref();
        let namespace_override = cluster.namespace.as_deref();

        let Some(kubefile) = resolve_kubefile(kubefile_argument) else {
            return Err(ProviderError::KubeconfigNotFound.into());
        };

        let mut config =
            build_kubeconfig(kubefile_argument, context_argument, namespace_override).await?;

        let Some(context) = cluster
            .context
            .clone()
            .or_else(|| resolve_current_context(kubefile_argument))
        else {
            return Err(ProviderError::KubeconfigContextMissing.into());
        };

        let Some(cluster_name) = resolve_cluster_name(kubefile_argument, Some(&context)) else {
            return Err(ProviderError::ContextClusterMissing(context).into());
        };

        if config.default_namespace.trim().is_empty() {
            config.default_namespace = DEFAULT_INSTANCE.to_string();
        }

        let cluster = DockerCluster::new(config.clone());
        let instance = DockerInstance::new(config.clone());

        Ok(Self {
            kubefile,
            context,
            cluster_name,
            config,
            cluster,
            instance,
            inputs,
            verbose,
        })
    }
}

#[async_trait]
impl Provider for DockerProvider {
    fn name(&self) -> &'static str {
        ClusterProvider::Docker.as_str()
    }

    fn display_name(&self) -> String {
        format!(
            "{}:{}/{}",
            ClusterProvider::Docker.as_str(),
            self.context,
            self.config.default_namespace
        )
    }

    fn to_session(&self) -> Session {
        Session::Docker {
            kubeconfig: self.kubefile.clone(),
            context: self.context.clone(),
            cluster: self.cluster_name.clone(),
            namespace: self.config.default_namespace.clone(),
            inputs: self.inputs.clone(),
        }
    }

    fn cluster_provider(&self) -> ClusterProvider {
        ClusterProvider::Docker
    }

    async fn kube_config(&self) -> CommandResult<kube::Config> {
        Ok(self.config.clone())
    }

    fn system_values(&self) -> CommandResult<SystemValues> {
        Ok(SystemValues {
            instance_name: Some(DEFAULT_INSTANCE_NAME.to_owned()),
            ..SystemValues::default()
        })
    }

    fn secret_generators(&self, _phase: SecretPhase) -> Vec<Box<dyn SecretGenerator>> {
        Vec::new()
    }

    fn summary(&self) -> Summary {
        Summary {
            provider: ClusterProvider::Docker,
            instance_name: None,
            instance_path: None,
            kubeconfig_path: Some(self.kubefile.clone()),
            context: Some(self.context.clone()),
            cluster_url: Some(self.config.cluster_url.to_string()),
            cluster_name: Some(self.cluster_name.clone()),
            namespace: Some(self.config.default_namespace.clone()),
            loglevel: None,
        }
    }

    fn cluster_settings(&self, _provided: ClusterProperties) -> CommandResult<ClusterSettings> {
        Ok(ClusterSettings {
            kubeconfig: Some(self.kubefile.clone()),
            context: Some(self.context.clone()),
            namespace: Some(self.config.default_namespace.clone()),
        })
    }

    fn cluster_descriptor(&self, settings: ClusterSettings) -> CommandResult<ClusterDescriptor> {
        Ok(ClusterDescriptor {
            kubeconfig: settings.kubeconfig,
            context: settings.context,
            namespace: settings.namespace,
            cluster: Some(self.cluster_name.clone()),
            url: Some(self.config.cluster_url.to_string()),
        })
    }

    fn refresh_cluster_views(&self, current: ClusterViews) -> CommandResult<ClusterViews> {
        Ok(current)
    }

    async fn ensure_pinnable_server(&self) -> CommandResult {
        let context = self.kube_context().await?;
        ensure_cluster_reachable(context.client()).await?;
        ensure_namespace_exists(&context).await?;
        if !exists_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await? {
            return Err(
                ProviderError::InstanceNotFound(self.config.default_namespace.clone()).into(),
            );
        }
        Ok(())
    }

    async fn ensure_creatable_server(&self) -> CommandResult {
        let context = self.kube_context().await?;
        ensure_cluster_reachable(context.client()).await?;
        ensure_namespace_exists(&context).await?;
        if exists_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await? {
            return abort_with(
                NoAction,
                ProviderError::InstanceAlreadyExists(self.config.default_namespace.clone()),
            );
        }
        Ok(())
    }

    async fn create_cluster_hook(
        &self,
        reporter: &dyn Reporter,
    ) -> CommandResult<Box<dyn Transaction>> {
        let step = reporter.step("Preparing kubernetes cluster");
        self.cluster.create()?;
        step.done("Kubernetes cluster prepared");
        let step = reporter.step("Storing tabsdata configmap");
        self.store_configmaps().await?;
        step.done("Tabsdata configmap stored");
        handle_phase_secrets(self, SecretPhase::ClusterStart).await?;
        Ok(Box::new(()))
    }

    async fn create_database(&self, _reporter: &dyn Reporter) -> CommandResult {
        Ok(())
    }

    async fn create_runtimes(&self, _reporter: &dyn Reporter) -> CommandResult {
        Ok(())
    }

    async fn create_images(&self, _reporter: &dyn Reporter) -> CommandResult {
        Ok(())
    }

    async fn cluster_alive(&self) -> bool {
        self.cluster.is_alive().await
    }

    async fn instance_alive(&self) -> bool {
        self.cluster_alive().await && self.instance.is_alive().await
    }

    async fn start_cluster_hook(&self, reporter: &dyn Reporter) -> CommandResult {
        let step = reporter.step("Starting kubernetes cluster");
        self.cluster.start().await?;
        step.done("Kubernetes cluster started");
        handle_phase_secrets(self, SecretPhase::ClusterStart).await?;
        Ok(())
    }

    async fn stop_cluster(&self, reporter: &dyn Reporter) -> CommandResult<ClusterStopOutcome> {
        let step = reporter.step("Stopping kubernetes cluster");
        let stopped = self.cluster.stop().await?;
        step.done("Kubernetes cluster stopped");
        match stopped {
            true => Ok(ClusterStopOutcome::Stopped),
            false => Ok(ClusterStopOutcome::AlreadyStopped),
        }
    }

    async fn ensure_startable_cluster(&self) -> CommandResult {
        Ok(())
    }

    async fn ensure_stoppable_cluster(&self) -> CommandResult {
        Ok(())
    }

    async fn ensure_restartable_cluster(&self) -> CommandResult {
        Ok(())
    }

    async fn start_instance_hook(&self, reporter: &dyn Reporter) -> CommandResult {
        handle_phase_secrets(self, SecretPhase::InstanceStart).await?;
        let step = reporter.step("Starting tabsdata instance");
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        self.apply_workload(&multi).await?;
        step.done("Tabsdata instance started");
        Ok(())
    }

    async fn stop_instance(&self, reporter: &dyn Reporter) -> CommandResult<InstanceStopOutcome> {
        let step = reporter.step("Stopping tabsdata instance");
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        let stopped = self.teardown_workload(&multi).await?;
        step.done("Tabsdata instance stopped");
        match stopped {
            true => Ok(InstanceStopOutcome::Stopped),
            false => Ok(InstanceStopOutcome::AlreadyStopped),
        }
    }

    async fn ensure_startable_instance(&self) -> CommandResult {
        Ok(())
    }

    async fn ensure_stoppable_instance(&self) -> CommandResult {
        Ok(())
    }

    async fn ensure_restartable_instance(&self) -> CommandResult {
        Ok(())
    }

    async fn ensure_startable_server(&self) -> CommandResult {
        if cfg!(td_k8s_development) {
            let context = self.kube_context().await?;
            if let Some(data) = get_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await?
                && let Some(yaml) = data.get(MANIFEST_FILE)
            {
                verify_manifest_compatibility(yaml)?;
            }
        }
        Ok(())
    }

    async fn ensure_stoppable_server(&self) -> CommandResult {
        Ok(())
    }

    async fn ensure_restartable_server(&self) -> CommandResult {
        Ok(())
    }
}

impl ContainerProvider for DockerProvider {}

impl HostedProvider for DockerProvider {}

impl LocalProvider for DockerProvider {}

impl DockerProvider {
    async fn store_configmaps(&self) -> CommandResult {
        let loaded = load_profile(self.cluster_provider(), self.verbose)?;
        let resolver = ConfigurationResolver::new(self.system_values()?);
        let rendered = render_for_configmaps(&loaded, &resolver).await?;
        let context = self.kube_context().await?;
        for (family, files) in [
            (&KPROFILE_PODS_FAMILY, rendered.pods),
            (&KPROFILE_CONFIGS_FAMILY, rendered.configs),
        ] {
            store_family_configmaps(&context, family, files).await?;
        }
        let mut manifest_data = BTreeMap::new();
        manifest_data.insert(MANIFEST_FILE.to_owned(), rendered.manifest);
        create_configmap(
            &context,
            CONFIGMAP_TABSDATA_MANIFEST,
            manifest_data,
            BTreeMap::new(),
            None,
            None,
        )
        .await?;
        Ok(())
    }

    async fn apply_workload(&self, multi: &MultiProgress) -> CommandResult {
        self.instance.start(multi).await
    }

    async fn teardown_workload(&self, multi: &MultiProgress) -> CommandResult<bool> {
        self.instance.stop(multi).await
    }
}

async fn store_family_configmaps(
    context: &crate::kube::context::KubeContext,
    family: &Family,
    files: RenderedConfigmap,
) -> CommandResult {
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
