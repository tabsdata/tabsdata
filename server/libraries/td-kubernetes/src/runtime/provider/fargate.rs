//
// Copyright 2026 Tabs Data Inc.
//

use async_trait::async_trait;
use kube::Config;
use td_common::status::ExitStatus::GeneralError;

use crate::base::reporter::Reporter;
use crate::cerror;
use crate::client::session::pin::Session;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::core::system::exit::{Result, abort};
use crate::runtime::gateway::resolver::SystemValues;
use crate::runtime::provider::outcome::{ClusterStopOutcome, InstanceStopOutcome};
use crate::runtime::provider::summary::Summary;
use crate::runtime::provider::{
    CloudProvider, ClusterProvider, ContainerProvider, Provider, ServerlessProvider, Transaction,
};
use crate::runtime::secrets::{SecretGenerator, SecretPhase};

pub struct FargateProvider {}

impl FargateProvider {
    pub async fn build(
        _cluster: ClusterArguments,
        _instance: InstanceArguments,
        _verbose: bool,
    ) -> Result<Self> {
        cerror!(
            "Provider '{}' is not yet implemented.",
            ClusterProvider::Fargate.as_str()
        );
        abort(GeneralError)
    }
}

#[async_trait]
impl Provider for FargateProvider {
    fn name(&self) -> &'static str {
        unreachable!("fargate provider not yet implemented")
    }

    fn display_name(&self) -> String {
        unreachable!("fargate provider not yet implemented")
    }

    fn to_session(&self) -> Session {
        unreachable!("fargate provider not yet implemented")
    }

    fn cluster_provider(&self) -> ClusterProvider {
        ClusterProvider::Fargate
    }

    async fn kube_config(&self) -> Result<Config> {
        unreachable!("fargate provider not yet implemented")
    }

    fn system_values(&self) -> Result<SystemValues> {
        unreachable!("fargate provider not yet implemented")
    }

    fn secret_generators(&self, _phase: SecretPhase) -> Vec<Box<dyn SecretGenerator>> {
        unreachable!("fargate provider not yet implemented")
    }

    fn summary(&self) -> Summary {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_pinnable_server(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_creatable_server(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn create_cluster_hook(&self, _reporter: &dyn Reporter) -> Result<Box<dyn Transaction>> {
        unreachable!("fargate provider not yet implemented")
    }

    async fn create_database(&self, _reporter: &dyn Reporter) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn create_runtimes(&self, _reporter: &dyn Reporter) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn cluster_alive(&self) -> bool {
        unreachable!("fargate provider not yet implemented")
    }

    async fn start_cluster_hook(&self, _reporter: &dyn Reporter) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn stop_cluster(&self, _reporter: &dyn Reporter) -> Result<ClusterStopOutcome> {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_startable_cluster(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_stoppable_cluster(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_restartable_cluster(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn start_instance_hook(&self, _reporter: &dyn Reporter) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn stop_instance(&self, _reporter: &dyn Reporter) -> Result<InstanceStopOutcome> {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_startable_instance(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_stoppable_instance(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_restartable_instance(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_startable_server(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_stoppable_server(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_restartable_server(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }
}

impl ContainerProvider for FargateProvider {}

impl ServerlessProvider for FargateProvider {}

impl CloudProvider for FargateProvider {}
