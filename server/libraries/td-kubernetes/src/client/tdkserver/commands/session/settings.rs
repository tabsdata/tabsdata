//
// Copyright 2026 Tabsdata Inc.
//

use clap_derive::Args;

use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::CommandResult;
use crate::cwarn;

#[derive(Debug, Clone, Args)]
pub struct SettingsArguments {}

pub async fn command_settings(
    _arguments: SettingsArguments,
    _globals: &GlobalArguments,
) -> CommandResult {
    let settings = SettingsContext::settings();
    if settings.is_empty() {
        cwarn!("The tabsdata session settings are empty.");
        return Ok(());
    }
    settings.report("Tabsdata session settings:");
    Ok(())
}
