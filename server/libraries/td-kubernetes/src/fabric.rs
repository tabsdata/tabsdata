//
// Copyright 2026 Tabs Data Inc.
//

pub mod kminus;
pub mod kubernetes;

use async_trait::async_trait;
use clap_derive::ValueEnum;
use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::exit::{Outcome, abort};
use crate::fabric::kminus::KminusFabric;
use crate::fabric::kubernetes::KubernetesFabric;
use crate::tdkcontext::TdkContext;

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
pub trait Fabric: Send + Sync {
    fn name(&self) -> &'static str;
    fn display_name(&self) -> String;
    fn to_tdkcontext(&self) -> TdkContext;

    async fn ensure_creatable(&self) -> Outcome;
    fn preview_create_summary(&self, profile: &str);
    async fn prepare_workspace(&self) -> Outcome<Box<dyn Commit>>;
    async fn stage_configmap(&self, profile: &str) -> Outcome;
    async fn store_configmap(&self, profile: &str) -> Outcome;
    fn report_created(&self);

    async fn ensure_usable(&self) -> Outcome;
    fn preview_use_summary(&self);
    fn report_used(&self);

    async fn ensure_startable(&self) -> Outcome;
    fn preview_start_summary(&self);
    async fn start_runtime(&self) -> Outcome;
    async fn ingest_configmap(&self) -> Outcome;
    fn report_started(&self);

    async fn ensure_stoppable(&self) -> Outcome;
    fn preview_stop_summary(&self);
    async fn stop_runtime(&self) -> Outcome;
    fn report_stopped(&self);
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
