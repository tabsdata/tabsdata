//
// Copyright 2026 Tabs Data Inc.
//

pub mod get;
pub mod list;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::secret::get::{GetArguments, command_get};
use crate::client::tdkserver::commands::secret::list::{ListArguments, command_list};
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Subcommand)]
pub enum SecretCommands {
    #[command(about = "List tabsdata secrets and their keys")]
    List(ListArguments),
    #[command(about = "Download tabsdata secret manifests")]
    Get(GetArguments),
}

pub async fn command_secret(command: SecretCommands, globals: &GlobalArguments) -> CommandResult {
    match command {
        SecretCommands::List(arguments) => command_list(arguments, globals).await,
        SecretCommands::Get(arguments) => command_get(arguments, globals).await,
    }
}
