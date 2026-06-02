//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use colored::Colorize;

use crate::client::tdkpin::TdkPin;
use crate::core::system::exit::Result;

#[derive(Debug, Clone, Args)]
pub struct ShowArguments {}

pub async fn command_show(_arguments: ShowArguments) -> Result {
    match TdkPin::load()?.autoconfirm {
        Some(autoconfirm) => {
            let state = if autoconfirm.enabled { "enabled" } else { "disabled" };
            tracing::info!("Autoconfirm is {state}.");
        }
        None => println!("{}", "No autoconfirm is pinned.".dimmed()),
    }
    Ok(())
}
