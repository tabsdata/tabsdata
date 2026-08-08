//
// Copyright 2026 Tabsdata Inc.
//

use td_build::version::TABSDATA_VERSION;
use td_common::about::tdabout;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::CommandResult;

pub async fn command_about(_globals: &GlobalArguments) -> CommandResult {
    tdabout(TABSDATA_VERSION);
    Ok(())
}
