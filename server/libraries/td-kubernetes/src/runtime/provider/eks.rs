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
    CloudProvider, ClusterProvider, ContainerProvider, HostedProvider, Provider, Transaction,
};
use crate::runtime::secrets::{SecretGenerator, SecretPhase};

pub struct EksProvider {}

impl EksProvider {
    pub async fn build(
        _cluster: ClusterArguments,
        _instance: InstanceArguments,
        _verbose: bool,
    ) -> Result<Self> {
        cerror!(
            "Provider '{}' is not yet implemented.",
            ClusterProvider::Eks.as_str()
        );
        abort(GeneralError)
    }
}

#[async_trait]
impl Provider for EksProvider {
    fn name(&self) -> &'static str {
        unreachable!("eks provider not yet implemented")
    }

    fn display_name(&self) -> String {
        unreachable!("eks provider not yet implemented")
    }

    fn to_tdkcontext(&self) -> TdkContext {
        unreachable!("eks provider not yet implemented")
    }

    async fn ensure_creatable_runtime(&self) -> Result {
        unreachable!("eks provider not yet implemented")
    }

    fn preview_create_runtime_summary(&self) {
        unreachable!("eks provider not yet implemented")
    }

    fn report_created_runtime(&self) {
        unreachable!("eks provider not yet implemented")
    }

    async fn ensure_pinnable_runtime(&self) -> Result {
        unreachable!("eks provider not yet implemented")
    }

    fn preview_pin_runtime_summary(&self) {
        unreachable!("eks provider not yet implemented")
    }

    fn report_pinned_runtime(&self) {
        unreachable!("eks provider not yet implemented")
    }

    async fn ensure_startable_runtime(&self) -> Result {
        unreachable!("eks provider not yet implemented")
    }

    fn preview_start_runtime_summary(&self) {
        unreachable!("eks provider not yet implemented")
    }

    fn report_started_runtime(&self) {
        unreachable!("eks provider not yet implemented")
    }

    async fn ensure_stoppable_runtime(&self) -> Result {
        unreachable!("eks provider not yet implemented")
    }

    fn preview_stop_runtime_summary(&self) {
        unreachable!("eks provider not yet implemented")
    }

    fn report_stopped_runtime(&self) {
        unreachable!("eks provider not yet implemented")
    }

    async fn prepare_cluster(&self) -> Result<Box<dyn Transaction>> {
        unreachable!("eks provider not yet implemented")
    }

    async fn create_database(&self) -> Result {
        unreachable!("eks provider not yet implemented")
    }

    async fn create_runtimes(&self) -> Result {
        unreachable!("eks provider not yet implemented")
    }

    async fn start_cluster(&self) -> Result {
        unreachable!("eks provider not yet implemented")
    }

    async fn stop_cluster(&self) -> Result<bool> {
        unreachable!("eks provider not yet implemented")
    }

    async fn stage_configmap(&self) -> Result {
        unreachable!("eks provider not yet implemented")
    }

    async fn store_configmap(&self) -> Result {
        unreachable!("eks provider not yet implemented")
    }

    async fn start_instance(&self) -> Result {
        unreachable!("eks provider not yet implemented")
    }

    async fn stop_instance(&self) -> Result<bool> {
        unreachable!("eks provider not yet implemented")
    }

    fn cluster_provider(&self) -> ClusterProvider {
        ClusterProvider::Eks
    }

    async fn kube_config(&self) -> Result<Config> {
        unreachable!("eks provider not yet implemented")
    }

    fn system_values(&self) -> Result<SystemValues> {
        unreachable!("eks provider not yet implemented")
    }

    fn secret_generators(&self, _phase: SecretPhase) -> Vec<Box<dyn SecretGenerator>> {
        unreachable!("eks provider not yet implemented")
    }
}

impl ContainerProvider for EksProvider {}

impl HostedProvider for EksProvider {}

impl CloudProvider for EksProvider {}
