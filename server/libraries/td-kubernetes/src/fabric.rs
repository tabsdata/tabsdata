//
// Copyright 2026 Tabs Data Inc.
//

pub mod kminus;
pub mod kubernetes;

use async_trait::async_trait;
use clap_derive::ValueEnum;
use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::client::tdkcontext::TdkContext;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::fabric::kminus::KminusFabric;
use crate::fabric::kubernetes::KubernetesFabric;
use crate::system::exit::{Outcome, abort};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClusterFabric {
    Kubernetes,
    Kminus,
}

pub trait Commit: Send {
    fn commit(self: Box<Self>);
}

impl Commit for () {
    fn commit(self: Box<Self>) {}
}

#[async_trait]
pub trait Cluster: Send + Sync {
    fn create(&self) -> Outcome;
    async fn start(&self) -> Outcome;
    async fn stop(&self) -> Outcome<bool>;

    async fn is_alive(&self) -> bool;
    async fn is_dead(&self) -> bool {
        !self.is_alive().await
    }
    async fn poll_alive(&self) -> bool {
        self.is_alive().await
    }
    async fn poll_dead(&self) -> bool {
        self.is_dead().await
    }
}

#[async_trait]
pub trait Instance: Send + Sync {
    async fn start(&self) -> Outcome;
    async fn stop(&self) -> Outcome<bool>;

    async fn is_alive(&self) -> bool;
    async fn is_dead(&self) -> bool {
        !self.is_alive().await
    }
    async fn poll_alive(&self) -> bool {
        self.is_alive().await
    }
    async fn poll_dead(&self) -> bool {
        self.is_dead().await
    }
}

#[async_trait]
pub trait Fabric: Send + Sync {
    fn name(&self) -> &'static str;
    fn display_name(&self) -> String;
    fn to_tdkcontext(&self) -> TdkContext;

    async fn ensure_creatable_runtime(&self) -> Outcome;
    fn preview_create_runtime_summary(&self, profile: &str);
    fn report_created_runtime(&self);

    async fn ensure_usable_runtime(&self) -> Outcome;
    fn preview_use_runtime_summary(&self);
    fn report_used_runtime(&self);

    async fn ensure_startable_runtime(&self) -> Outcome;
    fn preview_start_runtime_summary(&self);
    async fn start_runtime(&self) -> Outcome {
        self.start_cluster().await?;
        self.start_instance().await?;
        Ok(())
    }
    fn report_started_runtime(&self);

    async fn ensure_stoppable_runtime(&self) -> Outcome;
    fn preview_stop_runtime_summary(&self);
    async fn stop_runtime(&self) -> Outcome<bool> {
        let stopped_instance = self.stop_instance().await?;
        let stopped_cluster = self.stop_cluster().await?;
        Ok(stopped_instance || stopped_cluster)
    }
    fn report_stopped_runtime(&self);

    async fn prepare_cluster(&self) -> Outcome<Box<dyn Commit>>;
    async fn start_cluster(&self) -> Outcome;
    async fn stop_cluster(&self) -> Outcome<bool>;

    async fn prepare_instance(&self, profile: &str) -> Outcome {
        self.stage_configmap(profile).await?;
        self.store_configmap(profile).await
    }
    async fn stage_configmap(&self, profile: &str) -> Outcome;
    async fn store_configmap(&self, profile: &str) -> Outcome;
    async fn start_instance(&self) -> Outcome;
    async fn stop_instance(&self) -> Outcome<bool>;
}

pub async fn fabric_for(
    cluster: ClusterArguments,
    instance: InstanceArguments,
) -> Outcome<Box<dyn Fabric>> {
    let fabric = match cluster.fabric() {
        Some(fabric) => *fabric,
        None => {
            cerror!("Argument --fabric is required when no active context is set.");
            return abort(GeneralError);
        }
    };
    match fabric {
        ClusterFabric::Kubernetes => {
            let fabric = KubernetesFabric::build(cluster, instance).await?;
            Ok(Box::new(fabric))
        }
        ClusterFabric::Kminus => {
            let fabric = KminusFabric::build(cluster, instance)?;
            Ok(Box::new(fabric))
        }
    }
}

pub async fn fabric_for_or_tdkcontext(
    cluster: ClusterArguments,
    instance: InstanceArguments,
) -> Outcome<Box<dyn Fabric>> {
    if cluster.fabric().is_some() {
        return fabric_for(cluster, instance).await;
    }
    match TdkContext::load()? {
        Some(tdkcontext) => {
            let (cluster, instance) = tdkcontext.to_args();
            fabric_for(cluster, instance).await
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
