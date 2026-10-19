//
// Copyright 2026 Tabsdata Inc.
//

pub mod rules;

use crate::CONFIG_NAME;
use crate::tdmanager::rules::{RuleContext, UpgradeRule};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use td_common::status::ExitStatus;
use td_database::access::{DbPool, Global};
use td_database::config::DatabaseComponents;
use td_error::TdError;
use td_kubernetes::client::session::pod::PodContext;
use td_kubernetes::kube::api::support::client::build_pod_kube_client;
use td_kubernetes::kube::context::KubeContext;
use td_process::launcher::cli::Cli;
use tracing::{debug, error, info};

#[derive(Clone, Default, Serialize, Deserialize)]
struct TdManagerConfig {
    #[serde(default)]
    database: DatabaseComponents,
}

impl td_process::launcher::config::Config for TdManagerConfig {}

#[derive(Debug, Clone, Subcommand)]
enum Command {
    /// Apply the upgrade rules of the versions an upgrade walks through.
    Upgrade {
        /// Ids of the rules to apply, in order.
        #[clap(long, value_delimiter = ',')]
        rules: Vec<String>,
    },
}

#[derive(Debug, Clone, Args)]
struct TdManagerParameters {
    #[command(subcommand)]
    command: Option<Command>,
}

pub fn run() {
    Cli::<TdManagerConfig, TdManagerParameters>::exec_async(
        CONFIG_NAME,
        |config, parameters| async move {
            let Some(command) = parameters.command else {
                error!("No command given; see --help for the commands");
                return ExitStatus::GeneralError;
            };
            let outcome = match command {
                Command::Upgrade { rules } => upgrade(&config.database, &rules).await,
            };
            match outcome {
                Ok(()) => ExitStatus::Success,
                Err(failure) => {
                    error!("tdmanager command failed: {failure}");
                    debug!("{failure:#?}");
                    ExitStatus::GeneralError
                }
            }
        },
        None,
        None,
    );
}

async fn upgrade(database: &DatabaseComponents, ids: &[String]) -> Result<(), TdError> {
    let rules = rules::resolve(ids)?;
    if rules.is_empty() {
        info!("No upgrade rules to apply");
        return Ok(());
    }
    te_database::validate_components(database)?;
    let pools = te_database::connect_app_all(database).await?;
    let outcome = apply(&pools, &rules).await;
    pools.shutdown().await;
    outcome
}

async fn apply(
    pools: &te_database::ServingPools,
    rules: &[&'static dyn UpgradeRule],
) -> Result<(), TdError> {
    te_database::check_pools(pools).await?;
    let context = RuleContext {
        database: DbPool::new(pools.domain_pools(), Global),
        kubernetes: kubernetes_context().await?,
    };
    rules::apply(&context, rules).await
}

async fn kubernetes_context() -> Result<KubeContext, TdError> {
    let pod = PodContext::from_environment()?;
    let client = build_pod_kube_client().await?;
    Ok(KubeContext::new(Arc::new(client), pod.namespace))
}
