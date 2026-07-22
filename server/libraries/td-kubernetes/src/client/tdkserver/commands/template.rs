//
// Copyright 2026 Tabsdata Inc.
//

pub mod get;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::template::get::{GetArguments, command_get};
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Subcommand)]
pub enum TemplateCommands {
    #[command(about = "Render tabsdata templates into an empty folder")]
    Get(GetArguments),
}

pub async fn command_template(
    command: TemplateCommands,
    globals: &GlobalArguments,
) -> CommandResult {
    match command {
        TemplateCommands::Get(arguments) => command_get(arguments, globals).await,
    }
}
