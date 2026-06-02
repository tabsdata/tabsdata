//
// Copyright 2026 Tabs Data Inc.
//

pub mod list;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::secret::list::{ListArguments, command_list};
use crate::core::system::exit::Result;

#[derive(Debug, Clone, Subcommand)]
pub enum SecretCommands {
    #[command(about = "List tabsdata secrets and their keys")]
    List(ListArguments),
}

pub async fn command_secret(command: SecretCommands, globals: &GlobalArguments) -> Result {
    match command {
        SecretCommands::List(arguments) => command_list(arguments, globals).await,
    }
}
