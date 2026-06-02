//
// Copyright 2026 Tabs Data Inc.
//

pub mod list;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::config::list::{ListArguments, command_list};
use crate::core::system::exit::Result;

#[derive(Debug, Clone, Subcommand)]
pub enum ConfigCommands {
    #[command(about = "List tabsdata configs and their keys")]
    List(ListArguments),
}

pub async fn command_config(command: ConfigCommands, globals: &GlobalArguments) -> Result {
    match command {
        ConfigCommands::List(arguments) => command_list(arguments, globals).await,
    }
}
