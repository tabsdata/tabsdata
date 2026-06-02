//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use colored::Colorize;

use crate::client::session::pin::PinContext;
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Args)]
pub struct ShowArguments {}

pub async fn command_show(_arguments: ShowArguments) -> CommandResult {
    match PinContext::load()?.autoconfirm {
        Some(autoconfirm) => {
            let state = if autoconfirm.enabled {
                "enabled"
            } else {
                "disabled"
            };
            tracing::info!("Autoconfirm is {state}.");
        }
        None => println!("{}", "No autoconfirm is pinned.".dimmed()),
    }
    Ok(())
}
