//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use colored::Colorize;

use crate::client::tdkcontext::TdkContext;
use crate::core::system::exit::Result;

#[derive(Debug, Clone, Args)]
pub struct UsingArguments {}

pub async fn command_using(_arguments: UsingArguments) -> Result {
    match TdkContext::load()? {
        Some(tdkcontext) => tdkcontext.report_using(),
        None => println!("{}", "No active context is set.".dimmed()),
    }
    Ok(())
}
