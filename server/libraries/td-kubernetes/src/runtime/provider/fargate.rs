//
// Copyright 2026 Tabs Data Inc.
//

use async_trait::async_trait;
use kube::Config;
use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::client::tdkcontext::TdkContext;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::core::system::exit::{Result, abort};
use crate::runtime::gateway::resolver::SystemValues;
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

    fn to_tdkcontext(&self) -> TdkContext {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_creatable_runtime(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    fn preview_create_runtime_summary(&self) {
        unreachable!("fargate provider not yet implemented")
    }

    fn report_created_runtime(&self) {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_pinnable_runtime(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    fn preview_pin_runtime_summary(&self) {
        unreachable!("fargate provider not yet implemented")
    }

    fn report_pinned_runtime(&self) {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_startable_runtime(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    fn preview_start_runtime_summary(&self) {
        unreachable!("fargate provider not yet implemented")
    }

    fn report_started_runtime(&self) {
        unreachable!("fargate provider not yet implemented")
    }

    async fn runtime_alive(&self) -> bool {
        unreachable!("fargate provider not yet implemented")
    }

    async fn ensure_stoppable_runtime(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    fn preview_stop_runtime_summary(&self) {
        unreachable!("fargate provider not yet implemented")
    }

    fn report_stopped_runtime(&self) {
        unreachable!("fargate provider not yet implemented")
    }

    fn preview_restart_runtime_summary(&self) {
        unreachable!("fargate provider not yet implemented")
    }

    async fn prepare_cluster(&self) -> Result<Box<dyn Transaction>> {
        unreachable!("fargate provider not yet implemented")
    }

    async fn create_database(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn create_runtimes(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn start_cluster(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn stop_cluster(&self) -> Result<bool> {
        unreachable!("fargate provider not yet implemented")
    }

    async fn stage_configmap(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn store_configmap(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn start_instance(&self) -> Result {
        unreachable!("fargate provider not yet implemented")
    }

    async fn stop_instance(&self) -> Result<bool> {
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
}

impl ContainerProvider for FargateProvider {}

impl ServerlessProvider for FargateProvider {}

impl CloudProvider for FargateProvider {}
