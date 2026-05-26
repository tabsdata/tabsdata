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

use crate::cerror;
use crate::client::tdkcontext::{DockerInputs, TdkContext};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::widgets::spinner;
use crate::base::constants::{
    CONFIGMAP_TABSDATA_CONFIGS, CONFIGMAP_TABSDATA_GLOBALS, CONFIGMAP_TABSDATA_PODS,
    DEFAULT_INSTANCE_NAME,
};
use crate::runtime::gateway::resolver::{ConfigurationResolver, SystemValues};
use crate::runtime::instance::launcher::{start_workload, stop_workload};
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::profile::{load_profile, parse_from_configmaps, render_for_configmaps};
use crate::runtime::instance::verify_manifest_compatibility;
use crate::kube::api::configmap::get_configmap;
use crate::kube::api::{
    create_configmap, ensure_cluster_reachable, ensure_namespace_exists, exists_configmap,
    require_configmap,
};
use crate::runtime::reporting::build_kube_client;
use crate::kube::kubeconfig::{
    build_kubeconfig, resolve_cluster_name, resolve_current_context, resolve_kubefile,
};
use crate::runtime::provider::docker::cluster::DockerCluster;
use crate::runtime::provider::docker::instance::DockerInstance;
use crate::runtime::provider::{
    Cluster, ClusterProvider, ContainerProvider, HostedProvider, Instance, LocalProvider, Provider,
    Transaction,
};
use crate::core::system::exit::{Result, abort};

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

    fn to_tdkcontext(&self) -> TdkContext {
        TdkContext::Docker {
            kubeconfig: self.kubefile.clone(),
            context: self.context.clone(),
            cluster: self.cluster_name.clone(),
            namespace: self.config.default_namespace.clone(),
            inputs: self.inputs.clone(),
        }
    }

    async fn ensure_creatable_runtime(&self) -> Result {
        let client = build_kube_client(&self.config)?;
        ensure_cluster_reachable(&client).await?;
        ensure_namespace_exists(&client, &self.config.default_namespace).await?;
        if exists_configmap(
            &client,
            &self.config.default_namespace,
            CONFIGMAP_TABSDATA_GLOBALS,
        )
        .await?
        {
            info!(
                "A tabsdata instance already exists in namespace '{}'.",
                self.config.default_namespace
            );
            return abort(NoAction);
        }
        Ok(())
    }

    fn preview_create_runtime_summary(&self) {
        info!("About to create tabsdata instance:");
        info!("  - provider.....: {}", ClusterProvider::Docker.as_str());
        info!("  - kubeconfig...: {}", self.kubefile.display());
        info!("  - context......: {}", self.context);
        info!("  - cluster url..: {}", self.config.cluster_url);
        info!("  - cluster name.: {}", self.cluster_name);
        info!("  - namespace....: {}", self.config.default_namespace);
    }

    fn report_created_runtime(&self) {
        info!(
            "Created tabsdata instance in namespace '{}'.",
            self.config.default_namespace
        );
    }

    async fn ensure_usable_runtime(&self) -> Result {
        let client = build_kube_client(&self.config)?;
        ensure_cluster_reachable(&client).await?;
        ensure_namespace_exists(&client, &self.config.default_namespace).await?;
        if !exists_configmap(
            &client,
            &self.config.default_namespace,
            CONFIGMAP_TABSDATA_GLOBALS,
        )
        .await?
        {
            cerror!(
                "No tabsdata instance found in namespace '{}'. Run 'tdkserver create ...' first.",
                self.config.default_namespace
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    fn preview_use_runtime_summary(&self) {
        info!("About to use tabsdata instance:");
        info!("  - provider.....: {}", ClusterProvider::Docker.as_str());
        info!("  - kubeconfig...: {}", self.kubefile.display());
        info!("  - context......: {}", self.context);
        info!("  - cluster url..: {}", self.config.cluster_url);
        info!("  - cluster name.: {}", self.cluster_name);
        info!("  - namespace....: {}", self.config.default_namespace);
    }

    fn report_used_runtime(&self) {
        info!(
            "Now using tabsdata instance in namespace '{}'.",
            self.config.default_namespace
        );
    }

    async fn ensure_startable_runtime(&self) -> Result {
        if cfg!(td_k8s_development) {
            let client = build_kube_client(&self.config)?;
            if let Some(data) = get_configmap(
                &client,
                &self.config.default_namespace,
                CONFIGMAP_TABSDATA_GLOBALS,
            )
            .await?
                && let Some(yaml) = data.get(MANIFEST_FILE)
            {
                verify_manifest_compatibility(yaml)?;
            }
        }
        Ok(())
    }

    fn preview_start_runtime_summary(&self) {
        info!("About to start tabsdata instance:");
        info!("  - provider.....: {}", ClusterProvider::Docker.as_str());
        info!("  - kubeconfig...: {}", self.kubefile.display());
        info!("  - context......: {}", self.context);
        info!("  - namespace....: {}", self.config.default_namespace);
    }

    fn report_started_runtime(&self) {
        info!(
            "Started tabsdata instance in namespace '{}'.",
            self.config.default_namespace
        );
        info!("To use kubectl against this cluster, run:");
        info!("  export KUBECONFIG={}", self.kubefile.display());
    }

    async fn ensure_stoppable_runtime(&self) -> Result {
        Ok(())
    }

    fn preview_stop_runtime_summary(&self) {
        info!("About to stop tabsdata instance:");
        info!("  - provider.....: {}", ClusterProvider::Docker.as_str());
        info!("  - kubeconfig...: {}", self.kubefile.display());
        info!("  - context......: {}", self.context);
        info!("  - namespace....: {}", self.config.default_namespace);
    }

    fn report_stopped_runtime(&self) {}

    async fn prepare_cluster(&self) -> Result<Box<dyn Transaction>> {
        let bar = spinner("Preparing kubernetes cluster");
        let result = self.cluster.create();
        match &result {
            Ok(_) => bar.finish("Kubernetes cluster prepared"),
            Err(_) => bar.finish_and_clear(),
        }
        result?;
        Ok(Box::new(()))
    }

    async fn start_cluster(&self) -> Result {
        let bar = spinner("Starting kubernetes cluster");
        let result = self.cluster.start().await;
        match &result {
            Ok(_) => bar.finish("Kubernetes cluster started"),
            Err(_) => bar.finish_and_clear(),
        }
        result
    }

    async fn stop_cluster(&self) -> Result<bool> {
        let bar = spinner("Stopping kubernetes cluster");
        let result = self.cluster.stop().await;
        match &result {
            Ok(_) => bar.finish("Kubernetes cluster stopped"),
            Err(_) => bar.finish_and_clear(),
        }
        result
    }

    async fn stage_configmap(&self) -> Result {
        let bar = spinner("Staging tabsdata configmap");
        bar.finish("Tabsdata configmap staged");
        Ok(())
    }

    async fn store_configmap(&self) -> Result {
        let bar = spinner("Storing tabsdata configmap");
        let result = self.store_configmap_inner().await;
        match &result {
            Ok(()) => bar.finish("Tabsdata configmap stored"),
            Err(_) => bar.finish_and_clear(),
        }
        result
    }

    async fn start_instance(&self) -> Result {
        let bar = spinner("Starting tabsdata instance");
        let result = self.start_instance_inner(bar.multi()).await;
        match &result {
            Ok(_) => bar.finish("Tabsdata instance started"),
            Err(_) => bar.finish_and_clear(),
        }
        result
    }

    async fn stop_instance(&self) -> Result<bool> {
        let bar = spinner("Stopping tabsdata instance");
        let result = self.stop_instance_inner(bar.multi()).await;
        match &result {
            Ok(_) => bar.finish("Tabsdata instance stopped"),
            Err(_) => bar.finish_and_clear(),
        }
        result
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

    async fn create_database(&self) -> Result {
        Ok(())
    }

    async fn create_runtimes(&self) -> Result {
        Ok(())
    }
}

impl ContainerProvider for DockerProvider {}

impl HostedProvider for DockerProvider {}

impl LocalProvider for DockerProvider {}

impl DockerProvider {
    async fn store_configmap_inner(&self) -> Result {
        let loaded = load_profile(self.cluster_provider(), self.verbose)?;
        let resolver = ConfigurationResolver::new(self.system_values()?);
        let encoded = render_for_configmaps(&loaded, &resolver).await?;
        let client = build_kube_client(&self.config)?;
        for (name, data) in encoded.into_named() {
            create_configmap(&client, &self.config.default_namespace, name, data).await?;
        }
        Ok(())
    }

    async fn start_instance_inner(&self, multi: &MultiProgress) -> Result {
        self.instance.start().await?;
        let client = build_kube_client(&self.config)?;
        let namespace = &self.config.default_namespace;
        let globals = require_configmap(&client, namespace, CONFIGMAP_TABSDATA_GLOBALS).await?;
        let pods = require_configmap(&client, namespace, CONFIGMAP_TABSDATA_PODS).await?;
        let configs = require_configmap(&client, namespace, CONFIGMAP_TABSDATA_CONFIGS).await?;
        let profile = parse_from_configmaps(&globals, &pods, &configs)?;
        start_workload(self, &profile, multi).await
    }

    async fn stop_instance_inner(&self, multi: &MultiProgress) -> Result<bool> {
        let stopped_workload = stop_workload(self, multi).await?;
        let stopped_instance = self.instance.stop().await?;
        Ok(stopped_workload || stopped_instance)
    }
}
