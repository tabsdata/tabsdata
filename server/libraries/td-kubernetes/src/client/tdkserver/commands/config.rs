//
// Copyright 2026 Tabs Data Inc.
//

pub mod apply;
pub mod get;
pub mod list;
pub mod put;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::config::apply::{ApplyArguments, command_apply};
use crate::client::tdkserver::commands::config::get::{GetArguments, command_get};
use crate::client::tdkserver::commands::config::list::{ListArguments, command_list};
use crate::client::tdkserver::commands::config::put::{PutArguments, command_put};
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Subcommand)]
pub enum ConfigCommands {
    #[command(about = "Apply config key values from a folder and restart the instance")]
    Apply(ApplyArguments),
    #[command(about = "Get tabsdata configs into an empty folder")]
    Get(GetArguments),
    #[command(about = "List tabsdata configs and their keys")]
    List(ListArguments),
    #[command(about = "Put config key values from a folder")]
    Put(PutArguments),
}

pub async fn command_config(command: ConfigCommands, globals: &GlobalArguments) -> CommandResult {
    match command {
        ConfigCommands::Apply(arguments) => command_apply(arguments, globals).await,
        ConfigCommands::Get(arguments) => command_get(arguments, globals).await,
        ConfigCommands::List(arguments) => command_list(arguments, globals).await,
        ConfigCommands::Put(arguments) => command_put(arguments, globals).await,
    }
}
