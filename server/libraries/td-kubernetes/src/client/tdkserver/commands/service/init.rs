//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;

use crate::base::error::provider::ProviderError;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::{CommandError, CommandResult};
use crate::runtime::provider::ClusterProvider;

#[derive(Debug, Clone, Args)]
pub struct InitArguments {
    #[arg(
        long,
        name = "instance",
        required = true,
        long_help = "Name of the instance whose editable input config folder is produced."
    )]
    pub instance: String,

    #[arg(
        long,
        name = "provider",
        required = true,
        value_enum,
        long_help = "Provider of the instance: 'kminus', 'docker', 'eks' or 'fargate'. \
                     Shapes the produced input config."
    )]
    pub provider: ClusterProvider,
}

pub async fn command_init(_arguments: InitArguments, _globals: &GlobalArguments) -> CommandResult {
    Err(CommandError::from(ProviderError::CommandNotImplemented(
        "init".to_owned(),
    )))
}
