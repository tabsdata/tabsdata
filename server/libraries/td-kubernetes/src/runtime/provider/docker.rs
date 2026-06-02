//
// Copyright 2026 Tabs Data Inc.
//

pub mod cluster;
pub mod instance;

use std::path::PathBuf;

use async_trait::async_trait;
use indicatif::MultiProgress;
use kube::Config;
use td_common::server::DEFAULT_INSTANCE;
use td_common::status::ExitStatus::{GeneralError, NoAction};
use tracing::info;

use crate::base::constants::{
    CONFIGMAP_TABSDATA_CONFIGS, CONFIGMAP_TABSDATA_GLOBALS, CONFIGMAP_TABSDATA_PODS,
    DEFAULT_INSTANCE_NAME,
};
use crate::base::reporter::Reporter;
use crate::cerror;
use crate::client::session::pin::{DockerInputs, Session};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::core::system::exit::{Result, abort};
use crate::kube::api::configmap::get_configmap;
use crate::kube::api::{
    create_configmap, ensure_cluster_reachable, ensure_namespace_exists, exists_configmap,
    require_configmap,
};
use crate::kube::kubeconfig::{
    build_kubeconfig, resolve_cluster_name, resolve_current_context, resolve_kubefile,
};
use crate::runtime::gateway::resolver::{ConfigurationResolver, SystemValues};
use crate::runtime::instance::launcher::{start_workload, stop_workload};
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::profile::{
    load_profile, parse_from_configmaps, render_for_configmaps,
};
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
        instance: InstanceArguments,
        verbose: bool,
    ) -> Result<Self> {
        if cluster.kubeconfig.is_none() && cluster.context.is_none() && !cluster.current {
            cerror!(
                "Specify --kubeconfig, --context, or --current to identify the target cluster."
            );
            return abort(GeneralError);
        }
        if cluster.context.is_some() && cluster.current {
            cerror!("Arguments --context and --current are mutually exclusive.");
            return abort(GeneralError);
        }
        if instance.name.is_some() {
            cerror!(
                "Argument --name is not valid for the '{}' provider.",
                ClusterProvider::Docker.as_str()
            );
            return abort(GeneralError);
        }
        if instance.path.is_some() {
            cerror!(
                "Argument --path is not valid for the '{}' provider.",
                ClusterProvider::Docker.as_str()
            );
            return abort(GeneralError);
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
            cerror!(
                "No kubeconfig found. Set the KUBECONFIG environment variable, create ~/.kube/config, or pass --kubeconfig."
            );
            return abort(GeneralError);
        };

        let mut config =
            build_kubeconfig(kubefile_argument, context_argument, namespace_override).await?;

        let Some(context) = cluster
            .context
            .clone()
            .or_else(|| resolve_current_context(kubefile_argument))
        else {
            cerror!(
                "Unable to resolve a kubeconfig context. Pass --context or set a current-context in the kubeconfig."
            );
            return abort(GeneralError);
        };

        let Some(cluster_name) = resolve_cluster_name(kubefile_argument, Some(&context)) else {
            cerror!("Unable to resolve the cluster of context '{context}' in the kubeconfig.");
            return abort(GeneralError);
        };

        if config.default_namespace.trim().is_empty() {
            config.default_namespace = DEFAULT_INSTANCE.to_string();
        }

        let cluster = DockerCluster::new(config.clone());
        let instance = DockerInstance::new();

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

    async fn kube_config(&self) -> Result<kube::Config> {
        Ok(self.config.clone())
    }

    fn system_values(&self) -> Result<SystemValues> {
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
        }
    }

    async fn ensure_pinnable_server(&self) -> Result {
        let context = self.kube_context().await?;
        ensure_cluster_reachable(context.client()).await?;
        ensure_namespace_exists(&context).await?;
        if !exists_configmap(&context, CONFIGMAP_TABSDATA_GLOBALS).await? {
            cerror!(
                "No tabsdata instance found in namespace '{}'. Run 'tdkserver server create ...' first.",
                self.config.default_namespace
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    async fn ensure_creatable_server(&self) -> Result {
        let context = self.kube_context().await?;
        ensure_cluster_reachable(context.client()).await?;
        ensure_namespace_exists(&context).await?;
        if exists_configmap(&context, CONFIGMAP_TABSDATA_GLOBALS).await? {
            info!(
                "A tabsdata instance already exists in namespace '{}'.",
                self.config.default_namespace
            );
            return abort(NoAction);
        }
        Ok(())
    }

    async fn create_cluster_hook(&self, reporter: &dyn Reporter) -> Result<Box<dyn Transaction>> {
        let step = reporter.step("Preparing kubernetes cluster");
        self.cluster.create()?;
        step.done("Kubernetes cluster prepared");
        let step = reporter.step("Storing tabsdata configmap");
        self.store_configmaps().await?;
        step.done("Tabsdata configmap stored");
        handle_phase_secrets(self, SecretPhase::ClusterStart).await?;
        Ok(Box::new(()))
    }

    async fn create_database(&self, _reporter: &dyn Reporter) -> Result {
        Ok(())
    }

    async fn create_runtimes(&self, _reporter: &dyn Reporter) -> Result {
        Ok(())
    }

    async fn cluster_alive(&self) -> bool {
        self.cluster.is_alive().await
    }

    async fn start_cluster_hook(&self, reporter: &dyn Reporter) -> Result {
        let step = reporter.step("Starting kubernetes cluster");
        self.cluster.start().await?;
        step.done("Kubernetes cluster started");
        handle_phase_secrets(self, SecretPhase::ClusterStart).await?;
        Ok(())
    }

    async fn stop_cluster(&self, reporter: &dyn Reporter) -> Result<ClusterStopOutcome> {
        let step = reporter.step("Stopping kubernetes cluster");
        let stopped = self.cluster.stop().await?;
        step.done("Kubernetes cluster stopped");
        match stopped {
            true => Ok(ClusterStopOutcome::Stopped),
            false => Ok(ClusterStopOutcome::AlreadyStopped),
        }
    }

    async fn ensure_startable_cluster(&self) -> Result {
        Ok(())
    }

    async fn ensure_stoppable_cluster(&self) -> Result {
        Ok(())
    }

    async fn ensure_restartable_cluster(&self) -> Result {
        Ok(())
    }

    async fn start_instance_hook(&self, reporter: &dyn Reporter) -> Result {
        handle_phase_secrets(self, SecretPhase::InstanceStart).await?;
        let step = reporter.step("Starting tabsdata instance");
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        self.apply_workload(&multi).await?;
        step.done("Tabsdata instance started");
        Ok(())
    }

    async fn stop_instance(&self, reporter: &dyn Reporter) -> Result<InstanceStopOutcome> {
        let step = reporter.step("Stopping tabsdata instance");
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        let stopped = self.teardown_workload(&multi).await?;
        step.done("Tabsdata instance stopped");
        match stopped {
            true => Ok(InstanceStopOutcome::Stopped),
            false => Ok(InstanceStopOutcome::AlreadyStopped),
        }
    }

    async fn ensure_startable_instance(&self) -> Result {
        Ok(())
    }

    async fn ensure_stoppable_instance(&self) -> Result {
        Ok(())
    }

    async fn ensure_restartable_instance(&self) -> Result {
        Ok(())
    }

    async fn ensure_startable_server(&self) -> Result {
        if cfg!(td_k8s_development) {
            let context = self.kube_context().await?;
            if let Some(data) = get_configmap(&context, CONFIGMAP_TABSDATA_GLOBALS).await?
                && let Some(yaml) = data.get(MANIFEST_FILE)
            {
                verify_manifest_compatibility(yaml)?;
            }
        }
        Ok(())
    }

    async fn ensure_stoppable_server(&self) -> Result {
        Ok(())
    }

    async fn ensure_restartable_server(&self) -> Result {
        Ok(())
    }
}

impl ContainerProvider for DockerProvider {}

impl HostedProvider for DockerProvider {}

impl LocalProvider for DockerProvider {}

impl DockerProvider {
    async fn store_configmaps(&self) -> Result {
        let loaded = load_profile(self.cluster_provider(), self.verbose)?;
        let resolver = ConfigurationResolver::new(self.system_values()?);
        let rendered = render_for_configmaps(&loaded, &resolver).await?;
        let context = self.kube_context().await?;
        for (name, data) in rendered.into_named() {
            create_configmap(&context, name, data, None).await?;
        }
        Ok(())
    }

    async fn apply_workload(&self, multi: &MultiProgress) -> Result {
        self.instance.start().await?;
        let context = self.kube_context().await?;
        let globals = require_configmap(&context, CONFIGMAP_TABSDATA_GLOBALS).await?;
        let pods = require_configmap(&context, CONFIGMAP_TABSDATA_PODS).await?;
        let configs = require_configmap(&context, CONFIGMAP_TABSDATA_CONFIGS).await?;
        let profile = parse_from_configmaps(&globals, &pods, &configs)?;
        start_workload(self, &profile, multi).await
    }

    async fn teardown_workload(&self, multi: &MultiProgress) -> Result<bool> {
        let stopped_workload = stop_workload(self, multi).await?;
        let stopped_instance = self.instance.stop().await?;
        Ok(stopped_workload || stopped_instance)
    }
}
