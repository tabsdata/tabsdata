//
// Copyright 2026 Tabsdata Inc.
//

pub mod cluster;
pub mod instance;

use std::path::PathBuf;

use async_trait::async_trait;
use kube::Config;

use td_common::settings::DEFAULT_INSTANCE;

use crate::base::error::provider::ProviderError;
use crate::base::reporter::Reporter;
use crate::client::registry::ClusterViews;
use crate::client::session::pin::{FargateInputs, Session};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::core::system::exit::CommandResult;
use crate::kube::kubeconfig::{
    build_kubeconfig, resolve_cluster_name, resolve_current_context, resolve_kubefile,
};
use crate::runtime::gateway::resolver::SystemValues;
use crate::runtime::instance::cluster::{ClusterDescriptor, ClusterProperties, ClusterSettings};
use crate::runtime::provider::fargate::cluster::FargateCluster;
use crate::runtime::provider::fargate::instance::FargateInstance;
use crate::runtime::provider::outcome::{ClusterStopOutcome, InstanceStopOutcome};
use crate::runtime::provider::summary::Summary;
use crate::runtime::provider::{
    CloudProvider, Cluster, ClusterProvider, ContainerProvider, Instance, Provider,
    ServerlessProvider, Transaction,
};
use crate::runtime::secrets::{SecretGenerator, SecretPhase};

pub struct FargateProvider {
    kubefile: PathBuf,
    context: String,
    cluster_name: String,
    config: Config,
    cluster: FargateCluster,
    instance: FargateInstance,
    inputs: FargateInputs,
    verbose: bool,
}

impl FargateProvider {
    pub async fn build(
        cluster: ClusterArguments,
        _instance: InstanceArguments,
        verbose: bool,
    ) -> CommandResult<Self> {
        if cluster.kubeconfig.is_none() && cluster.context.is_none() && !cluster.current {
            return Err(ProviderError::ArgumentRequired {
                argument: "kubeconfig | context | current".to_owned(),
                provider: ClusterProvider::Fargate.as_str().to_owned(),
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

        let inputs = FargateInputs {};

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

        let cluster = FargateCluster::new(config.clone());
        let instance = FargateInstance::new(config.clone());

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
impl Provider for FargateProvider {
    fn name(&self) -> &'static str {
        ClusterProvider::Fargate.as_str()
    }

    fn display_name(&self) -> String {
        format!(
            "{}:{}/{}",
            ClusterProvider::Fargate.as_str(),
            self.context,
            self.config.default_namespace
        )
    }

    fn to_session(&self) -> Session {
        Session::Fargate {
            inputs: self.inputs.clone(),
        }
    }

    fn cluster_provider(&self) -> ClusterProvider {
        ClusterProvider::Fargate
    }

    async fn kube_config(&self) -> CommandResult<Config> {
        Ok(self.config.clone())
    }

    fn system_values(&self) -> CommandResult<SystemValues> {
        unreachable!("The fargate provider is not yet implemented.")
    }

    fn secret_generators(&self, _phase: SecretPhase) -> Vec<Box<dyn SecretGenerator>> {
        unreachable!("The fargate provider is not yet implemented.")
    }

    fn summary(&self) -> Summary {
        Summary {
            provider: ClusterProvider::Fargate,
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
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn ensure_creatable_server(&self) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn create_cluster_hook(
        &self,
        _reporter: &dyn Reporter,
    ) -> CommandResult<Box<dyn Transaction>> {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn create_database(&self, _reporter: &dyn Reporter) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn create_runtimes(&self, _reporter: &dyn Reporter) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn create_images(&self, _reporter: &dyn Reporter) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn cluster_alive(&self) -> bool {
        self.cluster.is_alive().await
    }

    async fn instance_alive(&self) -> bool {
        self.cluster_alive().await && self.instance.is_alive().await
    }

    async fn start_cluster_hook(&self, _reporter: &dyn Reporter) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn stop_cluster(&self, _reporter: &dyn Reporter) -> CommandResult<ClusterStopOutcome> {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn ensure_startable_cluster(&self) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn ensure_stoppable_cluster(&self) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn ensure_restartable_cluster(&self) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn start_instance_hook(&self, _reporter: &dyn Reporter) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn stop_instance(&self, _reporter: &dyn Reporter) -> CommandResult<InstanceStopOutcome> {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn ensure_startable_instance(&self) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn ensure_stoppable_instance(&self) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn ensure_restartable_instance(&self) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn ensure_startable_server(&self) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn ensure_stoppable_server(&self) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }

    async fn ensure_restartable_server(&self) -> CommandResult {
        unreachable!("The fargate provider is not yet implemented.")
    }
}

impl ContainerProvider for FargateProvider {}

impl ServerlessProvider for FargateProvider {}

impl CloudProvider for FargateProvider {}
