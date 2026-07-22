//
// Copyright 2026 Tabsdata Inc.
//

pub mod get;
pub mod list;
pub mod put;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::secret::get::{GetArguments, command_get};
use crate::client::tdkserver::commands::secret::list::{ListArguments, command_list};
use crate::client::tdkserver::commands::secret::put::{PutArguments, command_put};
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Subcommand)]
pub enum SecretCommands {
    #[command(about = "List tabsdata secrets and their keys")]
    List(ListArguments),
    #[command(about = "Download tabsdata secret manifests")]
    Get(GetArguments),
    #[command(about = "Upload tabsdata secret manifests to the cluster")]
    Put(PutArguments),
}

pub async fn command_secret(command: SecretCommands, globals: &GlobalArguments) -> CommandResult {
    match command {
        SecretCommands::List(arguments) => command_list(arguments, globals).await,
        SecretCommands::Get(arguments) => command_get(arguments, globals).await,
        SecretCommands::Put(arguments) => command_put(arguments, globals).await,
    }
}
