//
// Copyright 2026 Tabs Data Inc.
//

pub mod kminus;
pub mod kubernetes;

use std::future::Future;

use ambassador::{Delegate, delegatable_trait};
use clap_derive::ValueEnum;
use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::client::tdkcontext::TdkContext;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::fabric::kminus::KminusFabric;
use crate::fabric::kubernetes::KubernetesFabric;
use crate::system::exit::{Result, abort};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClusterFabric {
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

#[delegatable_trait]
pub trait Fabric: Send + Sync {
    fn name(&self) -> &'static str;
    fn display_name(&self) -> String;
    fn to_tdkcontext(&self) -> TdkContext;

    fn ensure_creatable_runtime(&self) -> impl Future<Output = Result> + Send;
    fn preview_create_runtime_summary(&self, profile: &str);
    fn report_created_runtime(&self);

    fn ensure_usable_runtime(&self) -> impl Future<Output = Result> + Send;
    fn preview_use_runtime_summary(&self);
    fn report_used_runtime(&self);

    fn ensure_startable_runtime(&self) -> impl Future<Output = Result> + Send;
    fn preview_start_runtime_summary(&self);
    fn start_runtime(&self) -> impl Future<Output = Result> + Send {
        async move {
            self.start_cluster().await?;
            self.start_instance().await?;
            Ok(())
        }
    }
    fn report_started_runtime(&self);

    fn ensure_stoppable_runtime(&self) -> impl Future<Output = Result> + Send;
    fn preview_stop_runtime_summary(&self);
    fn stop_runtime(&self) -> impl Future<Output = Result<bool>> + Send {
        async move {
            let stopped_instance = self.stop_instance().await?;
            let stopped_cluster = self.stop_cluster().await?;
            Ok(stopped_instance || stopped_cluster)
        }
    }
    fn report_stopped_runtime(&self);

    fn prepare_cluster(&self) -> impl Future<Output = Result<Box<dyn Transaction>>> + Send;
    fn start_cluster(&self) -> impl Future<Output = Result> + Send;
    fn stop_cluster(&self) -> impl Future<Output = Result<bool>> + Send;

    fn prepare_instance(&self, profile: &str) -> impl Future<Output = Result> + Send {
        async move {
            self.stage_configmap(profile).await?;
            self.store_configmap(profile).await
        }
    }
    fn stage_configmap(&self, profile: &str) -> impl Future<Output = Result> + Send;
    fn store_configmap(&self, profile: &str) -> impl Future<Output = Result> + Send;
    fn start_instance(&self) -> impl Future<Output = Result> + Send;
    fn stop_instance(&self) -> impl Future<Output = Result<bool>> + Send;
}

#[derive(Delegate)]
#[delegate(Fabric)]
pub enum Fabrics {
    Kubernetes(KubernetesFabric),
    Kminus(KminusFabric),
}

pub async fn fabric_for(
    cluster: ClusterArguments,
    instance: InstanceArguments,
) -> Result<Fabrics> {
    let fabric = match cluster.fabric {
        Some(fabric) => fabric,
        None => {
            cerror!("Argument --fabric is required when no active context is set.");
            return abort(GeneralError);
        }
    };
    match fabric {
        ClusterFabric::Kubernetes => {
            let fabric = KubernetesFabric::build(cluster, instance).await?;
            Ok(Fabrics::Kubernetes(fabric))
        }
        ClusterFabric::Kminus => {
            let fabric = KminusFabric::build(cluster, instance)?;
            Ok(Fabrics::Kminus(fabric))
        }
    }
}

pub async fn fabric_for_or_tdkcontext(
    cluster: ClusterArguments,
    instance: InstanceArguments,
) -> Result<Fabrics> {
    if cluster.fabric.is_some() {
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
