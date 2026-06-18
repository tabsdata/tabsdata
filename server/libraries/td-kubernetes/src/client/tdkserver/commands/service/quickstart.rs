//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;

use crate::base::error::provider::ProviderError;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments};
use crate::core::system::exit::{CommandError, CommandResult};

#[derive(Debug, Clone, Args)]
pub struct QuickstartArguments {
    #[arg(
        long,
        name = "instance",
        required = true,
        long_help = "Name of the instance to initialize, create and start."
    )]
    pub instance: String,

    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[arg(
        long,
        name = "save",
        required = false,
        default_value_t = false,
        long_help = "Copy the cluster settings files (kubeconfig, etc.) into the instance \
                     folder so the instance is self-contained."
    )]
    pub save: bool,
}

pub async fn command_quickstart(
    _arguments: QuickstartArguments,
    _globals: &GlobalArguments,
) -> CommandResult {
    Err(CommandError::from(ProviderError::CommandNotImplemented(
        "quickstart".to_owned(),
    )))
}
