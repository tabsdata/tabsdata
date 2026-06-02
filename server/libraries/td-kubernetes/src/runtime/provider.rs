//
// Copyright 2026 Tabs Data Inc.
//

pub(crate) mod axes;
pub mod docker;
pub mod eks;
pub mod fargate;
pub mod kminus;

use std::future::Future;

use async_trait::async_trait;
use clap_derive::ValueEnum;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::Config;
use td_common::status::ExitStatus::GeneralError;

use crate::base::constants::KMINUS_MIN_NOFILE;
use crate::cerror;
use crate::client::session::pin::{Session, PinContext};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use std::sync::Arc;

use crate::core::pod::{ValidationError, container as container_pod, process as process_pod};
use crate::core::system::exit::{Result, abort};
use crate::kube::context::KubeContext;
use crate::runtime::gateway::resolver::SystemValues;
use crate::runtime::instance::profile::bundle::Axis;
use crate::runtime::provider::axes::{Lifecycle, Location, Platform, Workload};
use crate::runtime::provider::docker::DockerProvider;
use crate::runtime::provider::eks::EksProvider;
use crate::runtime::provider::fargate::FargateProvider;
use crate::runtime::provider::kminus::KminusProvider;
use crate::runtime::reporting::build_kube_client;
use crate::runtime::secrets::{SecretGenerator, SecretHandler, SecretPhase};

pub const PROVIDER_AXIS_NAME: &str = "provider";

const PROVIDER_VALUES: &[&str] = &[
    ClusterProvider::Kminus.as_str(),
    ClusterProvider::Docker.as_str(),
    ClusterProvider::Eks.as_str(),
    ClusterProvider::Fargate.as_str(),
];

const PREFERRED_KMINUS: &[&str] = &[ClusterProvider::Kminus.as_str()];
const PREFERRED_DOCKER: &[&str] = &[ClusterProvider::Docker.as_str()];
const PREFERRED_EKS: &[&str] = &[ClusterProvider::Eks.as_str()];
const PREFERRED_FARGATE: &[&str] = &[ClusterProvider::Fargate.as_str()];

const AXES: &[Axis] = &[Axis {
    name: PROVIDER_AXIS_NAME,
    known: PROVIDER_VALUES,
}];

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClusterProvider {
    Kminus,
    Docker,
    Eks,
    Fargate,
}

#[allow(dead_code)]
impl ClusterProvider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Kminus => "kminus",
            Self::Docker => "docker",
            Self::Eks => "eks",
            Self::Fargate => "fargate",
        }
    }

    pub(crate) fn location(self) -> Location {
        match self {
            Self::Kminus | Self::Docker => Location::Local,
            Self::Eks | Self::Fargate => Location::Cloud,
        }
    }

    pub(crate) fn workload(self) -> Workload {
        match self {
            Self::Kminus => Workload::Process,
            Self::Docker | Self::Eks | Self::Fargate => Workload::Container,
        }
    }

    pub(crate) fn lifecycle(self) -> Lifecycle {
        match self {
            Self::Kminus | Self::Docker | Self::Eks => Lifecycle::Hosted,
            Self::Fargate => Lifecycle::Serverless,
        }
    }

    pub(crate) fn platform(self) -> Platform {
        match self {
            Self::Kminus | Self::Docker => Platform::Host,
            Self::Eks | Self::Fargate => Platform::Aws,
        }
    }

    pub fn axes(self) -> &'static [Axis] {
        AXES
    }

    pub fn preferred_values(self) -> &'static [&'static str] {
        match self {
            Self::Kminus => PREFERRED_KMINUS,
            Self::Docker => PREFERRED_DOCKER,
            Self::Eks => PREFERRED_EKS,
            Self::Fargate => PREFERRED_FARGATE,
        }
    }

    pub fn validate_regular_pod(self, pod: &Pod) -> Vec<ValidationError> {
        match self.workload() {
            Workload::Process => process_pod::validate(pod),
            Workload::Container => container_pod::validate(pod),
        }
    }

    pub fn enrich_regular_pod(self, pod: &mut Pod) -> Result {
        let _ = pod;
        Ok(())
    }

    pub fn validate_init_job(self, job: &Job) -> Vec<ValidationError> {
        let _ = job;
        Vec::new()
    }

    pub fn enrich_init_job(self, job: &mut Job) -> Result {
        let _ = job;
        Ok(())
    }
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
    fn to_session(&self) -> Session;

    async fn ensure_creatable_runtime(&self) -> Result;
    fn preview_create_runtime_summary(&self);
    fn report_created_runtime(&self);

    async fn ensure_pinnable_runtime(&self) -> Result;
    fn preview_pin_runtime_summary(&self);
    fn report_pinned_runtime(&self);

    async fn ensure_startable_runtime(&self) -> Result;
    fn preview_start_runtime_summary(&self);
    async fn start_runtime(&self) -> Result {
        self.start_cluster().await?;
        handle_phase_secrets(self, SecretPhase::Start).await?;
        self.start_instance().await?;
        Ok(())
    }
    fn report_started_runtime(&self);

    async fn runtime_alive(&self) -> bool;

    async fn ensure_stoppable_runtime(&self) -> Result;
    fn preview_stop_runtime_summary(&self);
    async fn stop_runtime(&self) -> Result<bool> {
        let stopped_instance = self.stop_instance().await?;
        let stopped_cluster = self.stop_cluster().await?;
        Ok(stopped_instance || stopped_cluster)
    }
    fn report_stopped_runtime(&self);

    fn preview_restart_runtime_summary(&self);

    fn preview_apply_runtime_summary(&self);

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

    fn cluster_provider(&self) -> ClusterProvider;
    async fn kube_config(&self) -> Result<Config>;

    async fn kube_context(&self) -> Result<KubeContext> {
        let config = self.kube_config().await?;
        let namespace = config.default_namespace.clone();
        let client = build_kube_client(&config)?;
        Ok(KubeContext::new(Arc::new(client), namespace))
    }

    fn system_values(&self) -> Result<SystemValues>;

    fn secret_generators(&self, phase: SecretPhase) -> Vec<Box<dyn SecretGenerator>>;
}

async fn handle_phase_secrets(provider: &(impl Provider + ?Sized), phase: SecretPhase) -> Result {
    let generators = provider.secret_generators(phase);
    if generators.is_empty() {
        return Ok(());
    }
    let context = provider.kube_context().await?;
    if let Err(error) = SecretHandler::handle(&context, phase, &generators).await {
        cerror!("Failed to handle {phase} secrets: {error}");
        return abort(GeneralError);
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) trait ProcessProvider: Provider {}

#[allow(dead_code)]
pub(crate) trait ContainerProvider: Provider {}

#[allow(dead_code)]
pub(crate) trait HostedProvider: Provider {}

#[allow(dead_code)]
pub(crate) trait ServerlessProvider: Provider {}

#[allow(dead_code)]
pub(crate) trait LocalProvider: Provider {}

#[allow(dead_code)]
pub(crate) trait CloudProvider: Provider {}

pub async fn provider_for(
    cluster: ClusterArguments,
    instance: InstanceArguments,
    verbose: bool,
) -> Result<Box<dyn Provider>> {
    let provider = match cluster.provider {
        Some(provider) => provider,
        None => {
            cerror!("Argument --provider is required when no active context is set.");
            return abort(GeneralError);
        }
    };
    match provider {
        ClusterProvider::Kminus => {
            check_kminus_ulimit()?;
            let provider = KminusProvider::build(cluster, instance, verbose)?;
            Ok(Box::new(provider))
        }
        ClusterProvider::Docker => {
            let provider = DockerProvider::build(cluster, instance, verbose).await?;
            Ok(Box::new(provider))
        }
        ClusterProvider::Eks => {
            let provider = EksProvider::build(cluster, instance, verbose).await?;
            Ok(Box::new(provider))
        }
        ClusterProvider::Fargate => {
            let provider = FargateProvider::build(cluster, instance, verbose).await?;
            Ok(Box::new(provider))
        }
    }
}

#[cfg(unix)]
fn check_kminus_ulimit() -> Result {
    use td_common::limits::{Resource, limit};

    let nofile = match limit(Resource::NOFILE) {
        Ok(limits) => limits,
        Err(error) => {
            cerror!("Failed to read the current process's open-files limit: {error}");
            return abort(GeneralError);
        }
    };
    if nofile.soft < KMINUS_MIN_NOFILE {
        cerror!(
            "The kminus provider requires at least {KMINUS_MIN_NOFILE} open files \
             (current soft limit is {soft}, hard limit is {hard}). Raise it with \
             `ulimit -n {KMINUS_MIN_NOFILE}` and rerun this command.",
            soft = nofile.soft,
            hard = nofile.hard,
        );
        return abort(GeneralError);
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_kminus_ulimit() -> Result {
    Ok(())
}

pub async fn provider_for_or_tdkcontext(
    cluster: ClusterArguments,
    instance: InstanceArguments,
    verbose: bool,
) -> Result<Box<dyn Provider>> {
    if cluster.provider.is_some() {
        return provider_for(cluster, instance, verbose).await;
    }
    match PinContext::load()?.session {
        Some(session) => {
            let (cluster, instance) = session.to_args();
            provider_for(cluster, instance, verbose).await
        }
        None => {
            cerror!(
                "No cluster specification supplied and no active context is set. \
                 Pass the required arguments, or run 'tdkserver session pin ...' first."
            );
            abort(GeneralError)
        }
    }
}
