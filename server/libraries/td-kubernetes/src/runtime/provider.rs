//
// Copyright 2026 Tabsdata Inc.
//

pub(crate) mod axes;
pub mod docker;
pub mod eks;
pub mod fargate;
pub mod kminus;
pub mod outcome;
pub mod summary;

use std::future::Future;

use async_trait::async_trait;
use clap_derive::ValueEnum;
use indicatif::MultiProgress;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;
use kube::Config;
use serde::{Deserialize, Serialize};
use td_common::settings::DEFAULT_INSTANCE;

#[cfg(unix)]
use crate::base::constants::KMINUS_MIN_NOFILE;
use crate::base::constants::LogLevel;
use crate::base::constants::UvMode;
use crate::base::error::provider::ProviderError;
use crate::base::reporter::Reporter;
use crate::client::registry::{ClusterViews, InstanceSettings, InstanceState};
use crate::client::session::pin::Session;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::runtime::instance::ports::{
    CHECK_ALL_PORTS_FILTER, CHECK_ONLY_PUBLISHED_PORTS_FILTER, kminus_api_port,
};
use std::sync::Arc;
use td_error::TdError;

use crate::core::pod::{ValidationError, container as container_pod, process as process_pod};
use crate::core::system::exit::CommandResult;
use crate::kube::context::KubeContext;
use crate::runtime::gateway::resolver::SystemValues;
use crate::runtime::instance::cluster::{ClusterDescriptor, ClusterProperties, ClusterSettings};
use crate::runtime::instance::profile::bundle::Axis;
use crate::runtime::instance::verify_instance_version;
use crate::runtime::provider::axes::{Lifecycle, Locality, Location, Platform, Workload};
use crate::runtime::provider::docker::DockerProvider;
use crate::runtime::provider::eks::EksProvider;
use crate::runtime::provider::fargate::FargateProvider;
use crate::runtime::provider::kminus::KminusProvider;
use crate::runtime::provider::outcome::{
    ClusterRestartOutcome, ClusterStartOutcome, ClusterStopOutcome, InstanceRestartOutcome,
    InstanceStartOutcome, InstanceStopOutcome, ServerRestartOutcome, ServerStartOutcome,
    ServerStopOutcome, ServiceRestartOutcome, ServiceStartOutcome, ServiceStopOutcome,
};
use crate::runtime::provider::summary::Summary;
use crate::runtime::reporting::build_kube_client;
use crate::runtime::secrets::settler::{
    auto_fill_generated_secrets, settle_cluster_configs_and_secrets,
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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

    pub fn supported(self) -> bool {
        match self {
            Self::Kminus | Self::Docker | Self::Fargate => true,
            Self::Eks => false,
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::Kminus => "a development kminus cluster",
            Self::Docker => "the Kubernetes cluster bundled with Docker Desktop on macOS/Windows",
            Self::Eks => "AWS EKS",
            Self::Fargate => "AWS EKS-on-Fargate",
        }
    }

    pub fn supported_providers() -> impl Iterator<Item = Self> {
        <Self as clap::ValueEnum>::value_variants()
            .iter()
            .copied()
            .filter(|provider| provider.supported())
    }

    pub fn supported_descriptions() -> String {
        Self::supported_providers()
            .map(|provider| format!("'{}' for {}", provider.as_str(), provider.description()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn supported_names() -> String {
        let names: Vec<String> = Self::supported_providers()
            .map(|provider| format!("'{}'", provider.as_str()))
            .collect();
        match names.split_last() {
            None => String::new(),
            Some((last, [])) => last.clone(),
            Some((last, rest)) => format!("{} or {last}", rest.join(", ")),
        }
    }

    pub(crate) fn location(self) -> Location {
        match self {
            Self::Kminus | Self::Docker => Location::Local,
            Self::Eks | Self::Fargate => Location::Cloud,
        }
    }

    pub fn locality(self) -> Locality {
        match self {
            Self::Kminus => Locality::Local,
            Self::Docker | Self::Eks | Self::Fargate => Locality::Remote,
        }
    }

    pub fn hosts_runtimes(self) -> Result<bool, TdError> {
        match self {
            Self::Kminus => Ok(UvMode::from_environment().map_err(TdError::from)? == UvMode::Local),
            Self::Docker | Self::Eks | Self::Fargate => Ok(false),
        }
    }

    pub fn hosts_database(self) -> bool {
        match self {
            Self::Kminus => true,
            Self::Docker | Self::Eks | Self::Fargate => false,
        }
    }

    pub fn ports_to_check_on_host(self) -> Option<&'static [&'static str]> {
        match self {
            Self::Kminus => Some(CHECK_ALL_PORTS_FILTER),
            Self::Docker => Some(CHECK_ONLY_PUBLISHED_PORTS_FILTER),
            Self::Eks | Self::Fargate => None,
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

    pub fn enrich_regular_pod(self, pod: &mut Pod) -> CommandResult {
        let _ = pod;
        Ok(())
    }

    pub fn validate_init_job(self, job: &Job) -> Vec<ValidationError> {
        let _ = job;
        Vec::new()
    }

    pub fn enrich_init_job(self, job: &mut Job) -> CommandResult {
        let _ = job;
        Ok(())
    }

    pub fn validate_regular_cronjob(self, cronjob: &CronJob) -> Vec<ValidationError> {
        let Some(pod) = cronjob_pod_template(cronjob) else {
            return Vec::new();
        };
        match self.workload() {
            Workload::Process => process_pod::validate(&pod),
            Workload::Container => container_pod::validate(&pod),
        }
    }

    pub fn enrich_regular_cronjob(self, cronjob: &mut CronJob) -> CommandResult {
        let _ = cronjob;
        Ok(())
    }
}

fn cronjob_pod_template(cronjob: &CronJob) -> Option<Pod> {
    let template = cronjob
        .spec
        .as_ref()?
        .job_template
        .spec
        .as_ref()?
        .template
        .clone();
    Some(Pod {
        metadata: template.metadata.unwrap_or_default(),
        spec: template.spec,
        status: None,
    })
}

pub trait Transaction: Send {
    fn commit(self: Box<Self>);
}

impl Transaction for () {
    fn commit(self: Box<Self>) {}
}

#[allow(dead_code)]
pub(crate) trait Cluster: Send + Sync {
    fn create(&self) -> CommandResult;
    fn start(&self) -> impl Future<Output = CommandResult> + Send;
    fn stop(&self) -> impl Future<Output = CommandResult<bool>> + Send;

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

#[allow(dead_code)]
pub(crate) trait Instance: Send + Sync {
    fn start(&self, multi: &MultiProgress) -> impl Future<Output = CommandResult> + Send;
    fn stop(&self, multi: &MultiProgress) -> impl Future<Output = CommandResult<bool>> + Send;

    fn is_alive(&self) -> impl Future<Output = bool> + Send;
    fn is_dead(&self) -> impl Future<Output = bool> + Send {
        async move { !self.is_alive().await }
    }
}

// Cluster  -> only the cluster.
// Instance -> only the workload (pods/jobs).
// Server   -> cluster & workload, end-to-end (always both).
// Service  -> cluster (only if hosted+local) + workload; manages whatever the
//             topology allows. Server and Service are near-identical in shape but
//             conceptually distinct; Service is the topology-gated lifecycle the
//             public verbs drive.
#[async_trait]
pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str;
    fn display_name(&self) -> String;

    fn to_session(&self) -> Session;

    fn cluster_provider(&self) -> ClusterProvider;
    async fn kube_config(&self) -> CommandResult<Config>;
    async fn kube_context(&self) -> CommandResult<KubeContext> {
        let config = self.kube_config().await?;
        let namespace = config.default_namespace.clone();
        let client = build_kube_client(&config)?;
        Ok(KubeContext::new(Arc::new(client), namespace))
    }

    fn system_values(&self) -> CommandResult<SystemValues>;
    fn secret_generators(&self, phase: SecretPhase) -> Vec<Box<dyn SecretGenerator>>;

    fn summary(&self) -> Summary;

    fn cluster_settings(&self, provided: ClusterProperties) -> CommandResult<ClusterSettings>;
    fn cluster_descriptor(&self, settings: ClusterSettings) -> CommandResult<ClusterDescriptor>;
    fn refresh_cluster_views(&self, current: ClusterViews) -> CommandResult<ClusterViews>;

    async fn ensure_pinnable_server(&self) -> CommandResult;
    async fn ensure_creatable_server(&self) -> CommandResult;
    async fn ensure_pinnable_service(&self) -> CommandResult {
        self.ensure_pinnable_server().await
    }
    async fn ensure_creatable_service(&self) -> CommandResult {
        self.ensure_creatable_server().await
    }

    async fn create_cluster_hook(
        &self,
        reporter: &dyn Reporter,
    ) -> CommandResult<Box<dyn Transaction>>;
    async fn create_database(&self, reporter: &dyn Reporter) -> CommandResult;
    async fn create_runtimes(&self, reporter: &dyn Reporter) -> CommandResult;
    async fn create_images(&self, reporter: &dyn Reporter) -> CommandResult;

    async fn cluster_alive(&self) -> bool;
    async fn cluster_dead(&self) -> bool {
        !self.cluster_alive().await
    }

    fn staging_profile_path(&self) -> Option<std::path::PathBuf> {
        None
    }
    async fn start_cluster(&self, reporter: &dyn Reporter) -> CommandResult<ClusterStartOutcome> {
        if self.cluster_alive().await {
            return Ok(ClusterStartOutcome::AlreadyRunning);
        }
        self.start_cluster_hook(reporter).await?;
        Ok(ClusterStartOutcome::Started)
    }
    async fn start_cluster_hook(&self, reporter: &dyn Reporter) -> CommandResult;
    async fn stop_cluster(&self, reporter: &dyn Reporter) -> CommandResult<ClusterStopOutcome>;
    async fn restart_cluster(
        &self,
        reporter: &dyn Reporter,
    ) -> CommandResult<ClusterRestartOutcome> {
        let stop = if self.cluster_alive().await {
            Some(self.stop_cluster(reporter).await?)
        } else {
            None
        };
        let start = self.start_cluster(reporter).await?;
        Ok(ClusterRestartOutcome { stop, start })
    }
    async fn ensure_startable_cluster(&self) -> CommandResult;
    async fn ensure_stoppable_cluster(&self) -> CommandResult;
    async fn ensure_restartable_cluster(&self) -> CommandResult;

    async fn instance_alive(&self) -> bool;
    async fn instance_dead(&self) -> bool {
        !self.instance_alive().await
    }
    async fn start_instance(&self, reporter: &dyn Reporter) -> CommandResult<InstanceStartOutcome> {
        if self.instance_alive().await {
            return Ok(InstanceStartOutcome::AlreadyRunning);
        }
        let context = self.kube_context().await?;
        auto_fill_generated_secrets(&context).await?;
        settle_cluster_configs_and_secrets(&context).await?;
        self.start_instance_hook(reporter).await?;
        Ok(InstanceStartOutcome::Started)
    }
    async fn start_instance_hook(&self, reporter: &dyn Reporter) -> CommandResult;
    async fn stop_instance(&self, reporter: &dyn Reporter) -> CommandResult<InstanceStopOutcome>;
    async fn restart_instance(
        &self,
        reporter: &dyn Reporter,
    ) -> CommandResult<InstanceRestartOutcome> {
        let stop = if self.instance_alive().await {
            Some(self.stop_instance(reporter).await?)
        } else {
            None
        };
        let start = self.start_instance(reporter).await?;
        Ok(InstanceRestartOutcome { stop, start })
    }
    async fn ensure_startable_instance(&self) -> CommandResult;
    async fn ensure_stoppable_instance(&self) -> CommandResult;
    async fn ensure_restartable_instance(&self) -> CommandResult;

    async fn server_alive(&self) -> bool {
        self.cluster_alive().await && self.instance_alive().await
    }
    async fn server_dead(&self) -> bool {
        self.cluster_dead().await && self.instance_dead().await
    }
    async fn start_server(&self, reporter: &dyn Reporter) -> CommandResult<ServerStartOutcome> {
        let cluster = self.start_cluster(reporter).await?;
        let instance = self.start_instance(reporter).await?;
        Ok(ServerStartOutcome { cluster, instance })
    }
    async fn stop_server(&self, reporter: &dyn Reporter) -> CommandResult<ServerStopOutcome> {
        let instance = self.stop_instance(reporter).await?;
        let cluster = self.stop_cluster(reporter).await?;
        Ok(ServerStopOutcome { cluster, instance })
    }
    async fn restart_server(&self, reporter: &dyn Reporter) -> CommandResult<ServerRestartOutcome> {
        let stop = if self.server_alive().await {
            Some(self.stop_server(reporter).await?)
        } else {
            None
        };
        let start = self.start_server(reporter).await?;
        Ok(ServerRestartOutcome { stop, start })
    }
    async fn ensure_startable_server(&self) -> CommandResult;
    async fn ensure_stoppable_server(&self) -> CommandResult;
    async fn ensure_restartable_server(&self) -> CommandResult;

    async fn service_alive(&self) -> bool {
        self.server_alive().await
    }
    async fn service_dead(&self) -> bool {
        self.server_dead().await
    }
    async fn start_service(&self, reporter: &dyn Reporter) -> CommandResult<ServiceStartOutcome> {
        let ServerStartOutcome { cluster, instance } = self.start_server(reporter).await?;
        Ok(ServiceStartOutcome { cluster, instance })
    }
    async fn stop_service(&self, reporter: &dyn Reporter) -> CommandResult<ServiceStopOutcome> {
        let ServerStopOutcome { cluster, instance } = self.stop_server(reporter).await?;
        Ok(ServiceStopOutcome { cluster, instance })
    }
    async fn restart_service(
        &self,
        reporter: &dyn Reporter,
    ) -> CommandResult<ServiceRestartOutcome> {
        let stop = if self.service_alive().await {
            Some(self.stop_service(reporter).await?)
        } else {
            None
        };
        let start = self.start_service(reporter).await?;
        Ok(ServiceRestartOutcome { stop, start })
    }
    async fn ensure_startable_service(&self) -> CommandResult {
        self.ensure_startable_server().await
    }
    async fn ensure_stoppable_service(&self) -> CommandResult {
        self.ensure_stoppable_server().await
    }
    async fn ensure_restartable_service(&self) -> CommandResult {
        self.ensure_restartable_server().await
    }
}

pub async fn require_started_cluster(provider: &dyn Provider) -> Result<(), ProviderError> {
    if provider.cluster_alive().await {
        return Ok(());
    }
    Err(match provider.cluster_provider() {
        ClusterProvider::Kminus => ProviderError::ClusterNotRunningKminus,
        other => ProviderError::ClusterNotRunningExternal {
            provider: other.as_str().to_owned(),
        },
    })
}

pub(super) async fn handle_phase_secrets(
    provider: &(impl Provider + ?Sized),
    phase: SecretPhase,
) -> CommandResult {
    let generators = provider.secret_generators(phase);
    if generators.is_empty() {
        return Ok(());
    }
    let context = provider.kube_context().await?;
    SecretHandler::handle(&context, phase, &generators)
        .await
        .map_err(|error| ProviderError::SecretHandlerFailed {
            phase: phase.to_string(),
            cause: error.to_string(),
        })?;
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
    portstart: Option<u16>,
    loglevel: Option<LogLevel>,
) -> CommandResult<Box<dyn Provider>> {
    let provider = cluster
        .provider
        .ok_or(ProviderError::ProviderArgumentRequired)?;
    if !provider.supported() {
        return Err(ProviderError::NotImplemented(provider.as_str().to_owned()).into());
    }
    match provider {
        ClusterProvider::Kminus => {
            check_kminus_ulimit()?;
            let provider = KminusProvider::build(cluster, instance, verbose, portstart, loglevel)?;
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
fn check_kminus_ulimit() -> CommandResult {
    use td_common::limits::{Resource, limit};

    let nofile = limit(Resource::NOFILE)
        .map_err(|error| ProviderError::NofileLimitReadFailed(error.to_string()))?;
    if nofile.soft < KMINUS_MIN_NOFILE {
        return Err(ProviderError::NofileLimitTooLow {
            required: KMINUS_MIN_NOFILE,
            soft: nofile.soft,
            hard: nofile.hard,
        }
        .into());
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_kminus_ulimit() -> CommandResult {
    Ok(())
}

pub async fn provider_for_or_tdkcontext(
    cluster: ClusterArguments,
    instance: InstanceArguments,
    verbose: bool,
) -> CommandResult<Box<dyn Provider>> {
    if cluster.provider.is_some() {
        return provider_for(cluster, instance, verbose, None, None).await;
    }
    Err(ProviderError::NoClusterSpecification.into())
}

pub async fn provider_for_instance(
    instance: Option<String>,
    verbose: bool,
    loglevel: Option<LogLevel>,
) -> CommandResult<Box<dyn Provider>> {
    provider_for_instance_inner(instance, verbose, loglevel, true, false).await
}

pub async fn provider_for_instance_any_state(
    instance: Option<String>,
    verbose: bool,
) -> CommandResult<Box<dyn Provider>> {
    provider_for_instance_inner(instance, verbose, None, false, false).await
}

pub async fn provider_for_instance_mutating(
    instance: Option<String>,
    verbose: bool,
    loglevel: Option<LogLevel>,
) -> CommandResult<Box<dyn Provider>> {
    provider_for_instance_inner(instance, verbose, loglevel, true, true).await
}

/// Same as [`provider_for_instance_mutating`], for commands that act on an instance
/// in any state.
pub async fn provider_for_instance_mutating_any_state(
    instance: Option<String>,
    verbose: bool,
) -> CommandResult<Box<dyn Provider>> {
    provider_for_instance_inner(instance, verbose, None, false, true).await
}

pub fn cluster_arguments_for(settings: &InstanceSettings) -> ClusterArguments {
    if settings.provider == ClusterProvider::Kminus {
        return ClusterArguments {
            provider: Some(settings.provider),
            context: None,
            kubeconfig: None,
            current: false,
            namespace: None,
        };
    }
    let adjusted = &settings.cluster.adjusted;
    ClusterArguments {
        provider: Some(settings.provider),
        context: adjusted.context.clone(),
        kubeconfig: adjusted.kubeconfig.clone(),
        current: adjusted.context.is_none(),
        namespace: adjusted.namespace.clone(),
    }
}

async fn provider_for_instance_inner(
    instance: Option<String>,
    verbose: bool,
    loglevel: Option<LogLevel>,
    require_created: bool,
    require_version_match: bool,
) -> CommandResult<Box<dyn Provider>> {
    let instance = instance.unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    let mut settings = if require_created {
        InstanceSettings::load_required(&instance, &[InstanceState::Created])?
    } else {
        InstanceSettings::load(&instance)?
    };
    if require_version_match {
        verify_instance_version(&settings.name, &settings.version)?;
    }
    if let Some(loglevel) = loglevel {
        if settings.provider != ClusterProvider::Kminus {
            return Err(ProviderError::ArgumentNotValidForProvider {
                argument: "loglevel".to_owned(),
                provider: settings.provider.as_str().to_owned(),
            }
            .into());
        }
        settings.loglevel = Some(loglevel);
        settings.save()?;
    }
    let cluster = cluster_arguments_for(&settings);
    provider_for(
        cluster,
        InstanceArguments::new(Some(settings.name.clone())),
        verbose,
        kminus_api_port(&settings.ports),
        settings.loglevel,
    )
    .await
}
