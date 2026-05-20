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
use crate::client::tdkcontext::{KubernetesInputs, TdkContext};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::widgets::spinner;
use crate::constants::{CONFIGMAP_TABSDATA, PROVIDER_KUBERNETES};
use crate::gateway::resolver::{ConfigurationResolver, SystemValues};
use crate::instance::launcher::{start_workload, stop_workload};
use crate::instance::manifest::MANIFEST_FILE;
use crate::instance::profile::{decode_from_configmap, encode_for_configmap, load_profile};
use crate::instance::verify_manifest_compatibility;
use crate::kubernetes::action::configmap::read_configmap;
use crate::kubernetes::action::{
    configmap_exists, create_configmap, ensure_cluster_reachable, ensure_namespace_exists,
};
use crate::kubernetes::client::build_kube_client;
use crate::kubernetes::kubeconfig::{
    build_kubeconfig, resolve_cluster_name, resolve_current_context, resolve_kubefile,
};
use crate::provider::kubernetes::cluster::KubernetesCluster;
use crate::provider::kubernetes::instance::KubernetesInstance;
use crate::provider::{Cluster, Instance, Provider, Transaction};
use crate::system::exit::{Result, abort};
use crate::topology::{Topology, topology_for};

pub struct KubernetesProvider {
    kubefile: PathBuf,
    context: String,
    cluster_name: String,
    config: Config,
    cluster: KubernetesCluster,
    instance: KubernetesInstance,
    topology: &'static dyn Topology,
    inputs: KubernetesInputs,
}

impl KubernetesProvider {
    pub async fn build(cluster: ClusterArguments, instance: InstanceArguments) -> Result<Self> {
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
            cerror!("Argument --name is not valid for the '{PROVIDER_KUBERNETES}' provider.");
            return abort(GeneralError);
        }
        if instance.path.is_some() {
            cerror!("Argument --path is not valid for the '{PROVIDER_KUBERNETES}' provider.");
            return abort(GeneralError);
        }

        let inputs = KubernetesInputs {
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

        let topology = topology_for(crate::provider::ClusterProvider::Kubernetes);

        let cluster = KubernetesCluster::new(config.clone());
        let instance = KubernetesInstance::new();

        Ok(Self {
            kubefile,
            context,
            cluster_name,
            config,
            cluster,
            instance,
            topology,
            inputs,
        })
    }
}

#[async_trait]
impl Provider for KubernetesProvider {
    fn name(&self) -> &'static str {
        PROVIDER_KUBERNETES
    }

    fn display_name(&self) -> String {
        format!(
            "{PROVIDER_KUBERNETES}:{}/{}",
            self.context, self.config.default_namespace
        )
    }

    fn to_tdkcontext(&self) -> TdkContext {
        TdkContext::Kubernetes {
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
        if configmap_exists(&client, &self.config.default_namespace, CONFIGMAP_TABSDATA).await? {
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
        info!("  - provider.....: {PROVIDER_KUBERNETES}");
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
        if !configmap_exists(&client, &self.config.default_namespace, CONFIGMAP_TABSDATA).await? {
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
        info!("  - provider.....: {PROVIDER_KUBERNETES}");
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
            if let Some(data) =
                read_configmap(&client, &self.config.default_namespace, CONFIGMAP_TABSDATA).await?
                && let Some(yaml) = data.get(MANIFEST_FILE)
            {
                verify_manifest_compatibility(yaml)?;
            }
        }
        Ok(())
    }

    fn preview_start_runtime_summary(&self) {
        info!("About to start tabsdata instance:");
        info!("  - provider.....: {PROVIDER_KUBERNETES}");
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
        info!("  - provider.....: {PROVIDER_KUBERNETES}");
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

    fn topology(&self) -> &'static dyn Topology {
        self.topology
    }

    async fn kube_config(&self) -> Result<kube::Config> {
        Ok(self.config.clone())
    }

    fn system_values(&self) -> Result<SystemValues> {
        Ok(SystemValues::default())
    }

    async fn create_database(&self) -> Result {
        Ok(())
    }

    async fn create_runtimes(&self) -> Result {
        Ok(())
    }
}

impl KubernetesProvider {
    async fn store_configmap_inner(&self) -> Result {
        let loaded = load_profile(self.topology)?;
        let resolver = ConfigurationResolver::new(self.system_values()?);
        let configmap_data = encode_for_configmap(&loaded, &resolver).await?;
        let client = build_kube_client(&self.config)?;
        create_configmap(
            &client,
            &self.config.default_namespace,
            CONFIGMAP_TABSDATA,
            configmap_data,
        )
        .await
    }

    async fn start_instance_inner(&self, multi: &MultiProgress) -> Result {
        self.instance.start().await?;
        let client = build_kube_client(&self.config)?;
        let data = match read_configmap(&client, &self.config.default_namespace, CONFIGMAP_TABSDATA)
            .await?
        {
            Some(data) => data,
            None => {
                cerror!(
                    "ConfigMap '{CONFIGMAP_TABSDATA}' not found in namespace '{}'.",
                    self.config.default_namespace,
                );
                return abort(GeneralError);
            }
        };
        let profile = decode_from_configmap(&data)?;
        start_workload(self, &profile, multi).await
    }

    async fn stop_instance_inner(&self, multi: &MultiProgress) -> Result<bool> {
        let stopped_workload = stop_workload(self, multi).await?;
        let stopped_instance = self.instance.stop().await?;
        Ok(stopped_workload || stopped_instance)
    }
}
