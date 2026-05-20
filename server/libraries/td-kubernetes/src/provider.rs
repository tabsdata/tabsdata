//
// Copyright 2026 Tabs Data Inc.
//

pub mod kminus;
pub mod kubernetes;

use std::future::Future;

use async_trait::async_trait;
use clap_derive::ValueEnum;
use kube::Config;
use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::client::tdkcontext::TdkContext;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::gateway::resolver::SystemValues;
use crate::provider::kminus::KminusProvider;
use crate::provider::kubernetes::KubernetesProvider;
use crate::system::exit::{Result, abort};
use crate::topology::Topology;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClusterProvider {
    Kubernetes,
    Kminus,
}

pub trait Transaction: Send {
    fn commit(self: Box<Self>);
}

impl Transaction for () {
    fn commit(self: Box<Self>) {}
}

pub trait Cluster: Send + Sync {
    fn create(&self) -> Result;
    fn start(&self) -> impl Future<Output = Result> + Send;
    fn stop(&self) -> impl Future<Output = Result<bool>> + Send;

    fn is_alive(&self) -> impl Future<Output = bool> + Send;
    fn is_dead(&self) -> impl Future<Output = bool> + Send {
        async move { !self.is_alive().await }
    }
    fn poll_alive(&self) -> impl Future<Output = bool> + Send {
        async move { self.is_alive().await }
    }
    fn poll_dead(&self) -> impl Future<Output = bool> + Send {
        async move { self.is_dead().await }
    }
}

pub trait Instance: Send + Sync {
    fn start(&self) -> impl Future<Output = Result> + Send;
    fn stop(&self) -> impl Future<Output = Result<bool>> + Send;

    fn is_alive(&self) -> impl Future<Output = bool> + Send;
    fn is_dead(&self) -> impl Future<Output = bool> + Send {
        async move { !self.is_alive().await }
    }
    fn poll_alive(&self) -> impl Future<Output = bool> + Send {
        async move { self.is_alive().await }
    }
    fn poll_dead(&self) -> impl Future<Output = bool> + Send {
        async move { self.is_dead().await }
    }
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str;
    fn display_name(&self) -> String;
    fn to_tdkcontext(&self) -> TdkContext;

    async fn ensure_creatable_runtime(&self) -> Result;
    fn preview_create_runtime_summary(&self);
    fn report_created_runtime(&self);

    async fn ensure_usable_runtime(&self) -> Result;
    fn preview_use_runtime_summary(&self);
    fn report_used_runtime(&self);

    async fn ensure_startable_runtime(&self) -> Result;
    fn preview_start_runtime_summary(&self);
    async fn start_runtime(&self) -> Result {
        self.start_cluster().await?;
        self.start_instance().await?;
        Ok(())
    }
    fn report_started_runtime(&self);

    async fn ensure_stoppable_runtime(&self) -> Result;
    fn preview_stop_runtime_summary(&self);
    async fn stop_runtime(&self) -> Result<bool> {
        let stopped_instance = self.stop_instance().await?;
        let stopped_cluster = self.stop_cluster().await?;
        Ok(stopped_instance || stopped_cluster)
    }
    fn report_stopped_runtime(&self);

    async fn prepare_cluster(&self) -> Result<Box<dyn Transaction>>;
    async fn create_database(&self) -> Result;
    async fn create_runtimes(&self) -> Result;
    async fn start_cluster(&self) -> Result;
    async fn stop_cluster(&self) -> Result<bool>;

    async fn prepare_instance(&self) -> Result {
        self.stage_configmap().await?;
        self.store_configmap().await
    }
    async fn stage_configmap(&self) -> Result;
    async fn store_configmap(&self) -> Result;
    async fn start_instance(&self) -> Result;
    async fn stop_instance(&self) -> Result<bool>;

    fn topology(&self) -> &'static dyn Topology;
    async fn kube_config(&self) -> Result<Config>;

    fn system_values(&self) -> Result<SystemValues>;
}

pub async fn provider_for(
    cluster: ClusterArguments,
    instance: InstanceArguments,
) -> Result<Box<dyn Provider>> {
    let provider = match cluster.provider {
        Some(provider) => provider,
        None => {
            cerror!("Argument --provider is required when no active context is set.");
            return abort(GeneralError);
        }
    };
    match provider {
        ClusterProvider::Kubernetes => {
            let provider = KubernetesProvider::build(cluster, instance).await?;
            Ok(Box::new(provider))
        }
        ClusterProvider::Kminus => {
            let provider = KminusProvider::build(cluster, instance)?;
            Ok(Box::new(provider))
        }
    }
}

pub async fn provider_for_or_tdkcontext(
    cluster: ClusterArguments,
    instance: InstanceArguments,
) -> Result<Box<dyn Provider>> {
    if cluster.provider.is_some() {
        return provider_for(cluster, instance).await;
    }
    match TdkContext::load()? {
        Some(tdkcontext) => {
            let (cluster, instance) = tdkcontext.to_args();
            provider_for(cluster, instance).await
        }
        None => {
            cerror!(
                "No cluster specification supplied and no active context is set. \
                 Pass the required arguments, or run 'tdkserver use ...' first."
            );
            abort(GeneralError)
        }
    }
}
