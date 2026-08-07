//
// Copyright 2026 Tabsdata Inc.
//

use async_trait::async_trait;
use kube::Config;

use crate::base::error::provider::ProviderError;
use crate::base::reporter::Reporter;
use crate::client::registry::ClusterViews;
use crate::client::session::pin::Session;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::core::system::exit::CommandResult;
use crate::runtime::gateway::resolver::SystemValues;
use crate::runtime::instance::cluster::{ClusterDescriptor, ClusterProperties, ClusterSettings};
use crate::runtime::provider::outcome::{ClusterStopOutcome, ImagesOutcome, InstanceStopOutcome};
use crate::runtime::provider::summary::Summary;
use crate::runtime::provider::{
    CloudProvider, ClusterProvider, ContainerProvider, HostedProvider, Provider, Transaction,
};
use crate::runtime::secrets::{SecretGenerator, SecretPhase};

pub struct EksProvider {}

impl EksProvider {
    pub async fn build(
        _cluster: ClusterArguments,
        _instance: InstanceArguments,
        _verbose: bool,
    ) -> CommandResult<Self> {
        Err(ProviderError::NotImplemented(ClusterProvider::Eks.as_str().to_owned()).into())
    }
}

#[async_trait]
impl Provider for EksProvider {
    fn name(&self) -> &'static str {
        unreachable!("The eks provider is not yet implemented.")
    }

    fn display_name(&self) -> String {
        unreachable!("The eks provider is not yet implemented.")
    }

    fn to_session(&self) -> Session {
        unreachable!("The eks provider is not yet implemented.")
    }

    fn cluster_provider(&self) -> ClusterProvider {
        ClusterProvider::Eks
    }

    async fn kube_config(&self) -> CommandResult<Config> {
        unreachable!("The eks provider is not yet implemented.")
    }

    fn system_values(&self) -> CommandResult<SystemValues> {
        unreachable!("The eks provider is not yet implemented.")
    }

    fn secret_generators(&self, _phase: SecretPhase) -> Vec<Box<dyn SecretGenerator>> {
        unreachable!("The eks provider is not yet implemented.")
    }

    fn summary(&self) -> Summary {
        unreachable!("The eks provider is not yet implemented.")
    }

    fn cluster_settings(&self, _provided: ClusterProperties) -> CommandResult<ClusterSettings> {
        unreachable!("The eks provider is not yet implemented.")
    }

    fn cluster_descriptor(&self, _settings: ClusterSettings) -> CommandResult<ClusterDescriptor> {
        unreachable!("The eks provider is not yet implemented.")
    }

    fn refresh_cluster_views(&self, _current: ClusterViews) -> CommandResult<ClusterViews> {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn ensure_pinnable_server(&self) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn ensure_creatable_server(&self) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn create_cluster_hook(
        &self,
        _reporter: &dyn Reporter,
    ) -> CommandResult<Box<dyn Transaction>> {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn create_database(&self, _reporter: &dyn Reporter) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn create_runtimes(&self, _reporter: &dyn Reporter) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn create_images(&self, _reporter: &dyn Reporter) -> CommandResult<ImagesOutcome> {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn cluster_alive(&self) -> bool {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn instance_alive(&self) -> bool {
        unreachable!("The eks provider is not yet implemented.")
    }

    fn manages_cluster_lifecycle(&self) -> bool {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn start_cluster_hook(&self, _reporter: &dyn Reporter) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn stop_cluster(&self, _reporter: &dyn Reporter) -> CommandResult<ClusterStopOutcome> {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn ensure_startable_cluster(&self) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn ensure_stoppable_cluster(&self) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn ensure_restartable_cluster(&self) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn start_instance_hook(&self, _reporter: &dyn Reporter) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn stop_instance(&self, _reporter: &dyn Reporter) -> CommandResult<InstanceStopOutcome> {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn ensure_startable_instance(&self) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn ensure_stoppable_instance(&self) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn ensure_restartable_instance(&self) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn ensure_startable_server(&self) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn ensure_stoppable_server(&self) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }

    async fn ensure_restartable_server(&self) -> CommandResult {
        unreachable!("The eks provider is not yet implemented.")
    }
}

impl ContainerProvider for EksProvider {}

impl HostedProvider for EksProvider {}

impl CloudProvider for EksProvider {}
