//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use colored::Colorize;

use crate::client::tdkcontext::TdkContext;
use crate::core::system::exit::Result;

#[derive(Debug, Clone, Args)]
pub struct ShowArguments {}

pub async fn command_show(_arguments: ShowArguments) -> Result {
    match TdkContext::load()? {
        Some(tdkcontext) => tdkcontext.report_active(),
        None => println!("{}", "No active tabsdata session is set.".dimmed()),
    }
    Ok(())
}
