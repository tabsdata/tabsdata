//
// Copyright 2026 Tabsdata Inc.
//

pub mod local;

use std::process::Command;

use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
use td_common::status::ExitStatus;
use td_python::python::resolve_python_binary;

use crate::base::constants::LogLevel;
use crate::base::error::provider::ProviderError;
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments};
use crate::client::tdkserver::commands::service::{resolved_cluster, resolved_instance};
use crate::core::system::exit::{CommandError, CommandResult};

const ASSESS_PROGRAM: &str = "tdk_assess";

#[derive(Debug, Clone, Args)]
pub struct AssessArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance whose creation is assessed. \
                     Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[arg(
        long,
        name = "loglevel",
        value_enum,
        required = false,
        display_order = 20,
        long_help = "Log level for the kminus orchestrator and pod runner. \
                     Defaults to 'debug'. Only valid with the 'kminus' provider.\n\n\
                     Example:\n  \
                     tdkserver assess --provider kminus --loglevel info"
    )]
    pub loglevel: Option<LogLevel>,
}

pub async fn command_assess(
    arguments: AssessArguments,
    _globals: &GlobalArguments,
) -> CommandResult {
    let AssessArguments {
        instance,
        cluster,
        loglevel: _,
    } = arguments;

    let settings = SettingsContext::settings();
    let instance = resolved_instance(instance, &settings);
    let name = instance
        .value
        .clone()
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    let (cluster, _) = resolved_cluster(cluster, &settings);

    let binary = resolve_python_binary(ASSESS_PROGRAM)?;
    let mut command = Command::new(binary);
    command.arg("--instance").arg(&name);
    if let Some(provider) = cluster.provider {
        command.arg("--provider").arg(provider.as_str());
    }
    if let Some(context) = cluster.context.as_deref() {
        command.arg("--context").arg(context);
    }
    if let Some(kubeconfig) = cluster.kubeconfig.as_deref() {
        command.arg("--kubeconfig").arg(kubeconfig);
    }
    if let Some(namespace) = cluster.namespace.as_deref() {
        command.arg("--namespace").arg(namespace);
    }
    if cluster.current {
        command.arg("--current");
    }

    let status = command
        .status()
        .map_err(|error| ProviderError::AssessFailed(error.to_string()))?;
    if status.success() {
        return Ok(());
    }
    Err(CommandError::new(ExitStatus::NoAction))
}
