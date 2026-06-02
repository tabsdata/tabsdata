//
// Copyright 2026 Tabs Data Inc.
//

pub mod restart;
pub mod start;
pub mod stop;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::cluster::restart::{RestartArguments, command_restart};
use crate::client::tdkserver::commands::cluster::start::{StartArguments, command_start};
use crate::client::tdkserver::commands::cluster::stop::{StopArguments, command_stop};
use crate::core::system::exit::Result;

#[derive(Debug, Clone, Subcommand)]
pub enum ClusterCommands {
    #[command(about = "Restart the kubernetes cluster")]
    Restart(RestartArguments),
    #[command(about = "Start the kubernetes cluster")]
    Start(StartArguments),
    #[command(about = "Stop the kubernetes cluster")]
    Stop(StopArguments),
}

pub async fn command_cluster(command: ClusterCommands, globals: &GlobalArguments) -> Result {
    match command {
        ClusterCommands::Restart(arguments) => command_restart(arguments, globals).await,
        ClusterCommands::Start(arguments) => command_start(arguments, globals).await,
        ClusterCommands::Stop(arguments) => command_stop(arguments, globals).await,
    }
}
