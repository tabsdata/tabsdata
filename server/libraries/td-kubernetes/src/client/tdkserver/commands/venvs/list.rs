//
// Copyright 2026 Tabsdata Inc.
//

use clap_derive::Args;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::render::report;
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Copy, Args)]
pub struct ListArguments {}

pub async fn command_venv_list(
    _arguments: ListArguments,
    _globals: &GlobalArguments,
) -> CommandResult {
    report::component::venvs::venv_list()
}
