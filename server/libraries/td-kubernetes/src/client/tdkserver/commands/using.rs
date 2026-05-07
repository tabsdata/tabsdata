//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use colored::Colorize;
use getset::Getters;

use crate::client::tdkcontext::TdkContext;
use crate::system::exit::Outcome;

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct UsingArguments {}

pub async fn command_using(_arguments: UsingArguments) -> Outcome {
    match TdkContext::load()? {
        Some(tdkcontext) => tdkcontext.report_using(),
        None => println!("{}", "No active context is set.".dimmed()),
    }
    Ok(())
}
