//
// Copyright 2026 Tabsdata Inc.
//

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::widgets::show_banner;
use crate::core::system::exit::CommandResult;

pub async fn command_banner(_globals: &GlobalArguments) -> CommandResult {
    show_banner();
    Ok(())
}
