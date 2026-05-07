//
// Copyright 2026 Tabs Data Inc.
//

use std::path::PathBuf;

use async_trait::async_trait;
use kube::Config;
use td_common::server::DEFAULT_INSTANCE;
use td_common::status::ExitStatus::GeneralError;
use tracing::info;

use crate::cerror;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::constants::{CONFIGMAP_TABSDATA, FABRIC_KMINUS, FABRIC_KUBERNETES};
use crate::exit::{Outcome, abort};
use crate::fabric::{Commit, Fabric};
use crate::kubernetes::action::{
    configmap_exists, create_configmap, ensure_cluster_reachable, ensure_namespace_exists,
};
use crate::kubernetes::client::build_kube_client;
use crate::kubernetes::toolkit::{
    build_kubeconfig, resolve_cluster_name, resolve_current_context, resolve_kubeconfig_path,
};
use crate::profile::load_profile;
use crate::topology::{Topology, topology_for};

pub struct KubernetesFabric {
    name: String,
    kubeconfig_path: Option<PathBuf>,
    context: Option<String>,
    cluster_name: Option<String>,
    config: Config,
    topology: &'static dyn Topology,
}

impl KubernetesFabric {
    pub async fn build(cluster: ClusterArguments, instance: InstanceArguments) -> Outcome<Self> {
        let provided_clusters = [
            cluster.kubeconfig().is_some(),
            cluster.context().is_some(),
            *cluster.current(),
        ]
        .into_iter()
        .filter(|flag| *flag)
        .count();
        if provided_clusters == 0 {
            cerror!(
                "Specify --kubeconfig, --context, or --current to identify the target cluster."
            );
            return abort(GeneralError);
        }
        if provided_clusters > 1 {
            cerror!("Arguments --kubeconfig, --context, and --current are mutually exclusive.");
            return abort(GeneralError);
        }
        if instance.name().is_some() {
            cerror!("Argument --name is not valid for the '{FABRIC_KUBERNETES}' fabric.");
            return abort(GeneralError);
        }
        if instance.path().is_some() {
            cerror!("Argument --path is not valid for the '{FABRIC_KUBERNETES}' fabric.");
            return abort(GeneralError);
        }

        let kubeconfig_argument = cluster.kubeconfig().as_deref();
        let context_argument = cluster.context().as_deref();
        let namespace_override = cluster.namespace().as_deref();

        let kubeconfig_path = resolve_kubeconfig_path(kubeconfig_argument);
        if kubeconfig_path.is_none() {
            cerror!(
                "No kubeconfig found. Set the KUBECONFIG environment variable, create ~/.kube/config, or pass --kubeconfig."
            );
            return abort(GeneralError);
        }

        let config =
            build_kubeconfig(kubeconfig_argument, context_argument, namespace_override).await?;

        let context = cluster
            .context()
            .clone()
            .or_else(|| resolve_current_context(kubeconfig_argument));
        let cluster_name = resolve_cluster_name(kubeconfig_argument, context_argument);

        let trimmed_namespace = config.default_namespace.trim();
        let name = if trimmed_namespace.is_empty() {
            DEFAULT_INSTANCE.to_string()
        } else {
            trimmed_namespace.to_owned()
        };

        let topology = topology_for(crate::fabric::ClusterFabric::Kubernetes);

        Ok(Self {
            name,
            kubeconfig_path,
            context,
            cluster_name,
            config,
            topology,
        })
    }
}

#[async_trait]
impl Fabric for KubernetesFabric {
    fn name(&self) -> &'static str {
        FABRIC_KUBERNETES
    }

    async fn ensure_creatable(&self) -> Outcome {
        let client = build_kube_client(&self.config)?;
        ensure_cluster_reachable(&client).await?;
        ensure_namespace_exists(&client, &self.config.default_namespace).await?;
        if configmap_exists(&client, &self.config.default_namespace, CONFIGMAP_TABSDATA).await? {
            cerror!(
                "A Tabsdata instance already exists in namespace '{}'.",
                self.config.default_namespace
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    fn preview_create_summary(&self, profile: &str) {
        info!("Tabsdata instance to be created:");
        if cfg!(td_k8s_development) {
            info!("  - profile......: {profile}");
        }
        info!("  - fabric.......: {FABRIC_KUBERNETES}");
        info!("  - instance name: {}", self.name);
        match &self.kubeconfig_path {
            Some(path) => info!("  - kubeconfig...: {}", path.display()),
            None => info!("  - kubeconfig...: <not found>"),
        }
        match &self.context {
            Some(name) => info!("  - context......: {name}"),
            None => info!("  - context......: <unable to resolve current-context>"),
        }
        info!("  - cluster url..: {}", self.config.cluster_url);
        match &self.cluster_name {
            Some(name) => info!("  - cluster name.: {name}"),
            None => info!("  - cluster name.: <unable to resolve cluster name>"),
        }
        info!("  - namespace....: {}", self.config.default_namespace);
    }

    async fn prepare_workspace(&self) -> Outcome<Box<dyn Commit>> {
        Ok(Box::new(()))
    }

    async fn stage_configmap(&self, _profile: &str) -> Outcome {
        Ok(())
    }

    async fn store_configmap(&self, profile: &str) -> Outcome {
        let configmap_data = load_profile(profile, self.topology)?;
        let client = build_kube_client(&self.config)?;
        create_configmap(
            &client,
            &self.config.default_namespace,
            CONFIGMAP_TABSDATA,
            configmap_data,
        )
        .await
    }

    fn report_created(&self) {
        info!(
            "Created Tabsdata instance '{}' in namespace '{}'.",
            self.name, self.config.default_namespace
        );
    }

    async fn ensure_startable(&self) -> Outcome {
        Ok(())
    }

    fn preview_start_summary(&self) {
        info!("Tabsdata instance to be started:");
        info!("  - fabric.......: {FABRIC_KUBERNETES}");
        info!("  - instance name: {}", self.name);
        if let Some(path) = &self.kubeconfig_path {
            info!("  - kubeconfig...: {}", path.display());
        }
        if let Some(name) = &self.context {
            info!("  - context......: {name}");
        }
        info!("  - namespace....: {}", self.config.default_namespace);
    }

    async fn start_runtime(&self) -> Outcome {
        info!("The 'start' command is not yet implemented for the '{FABRIC_KUBERNETES}' fabric.");
        info!(
            "Use the '{FABRIC_KMINUS}' fabric, or restart the underlying cluster through your own tooling."
        );
        Ok(())
    }

    async fn ingest_configmap(&self) -> Outcome {
        Ok(())
    }

    fn report_started(&self) {}

    async fn ensure_stoppable(&self) -> Outcome {
        Ok(())
    }

    fn preview_stop_summary(&self) {
        info!("Tabsdata instance to be stopped:");
        info!("  - fabric.......: {FABRIC_KUBERNETES}");
        info!("  - instance name: {}", self.name);
        if let Some(path) = &self.kubeconfig_path {
            info!("  - kubeconfig...: {}", path.display());
        }
        if let Some(name) = &self.context {
            info!("  - context......: {name}");
        }
        info!("  - namespace....: {}", self.config.default_namespace);
    }

    async fn stop_runtime(&self) -> Outcome {
        info!("The 'stop' command is not yet implemented for the '{FABRIC_KUBERNETES}' fabric.");
        info!(
            "Use the '{FABRIC_KMINUS}' fabric, or stop the underlying cluster through your own tooling."
        );
        Ok(())
    }

    fn report_stopped(&self) {}
}
