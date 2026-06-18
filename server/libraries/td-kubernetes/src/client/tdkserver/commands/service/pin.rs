//
// Copyright 2026 Tabs Data Inc.
//

use crate::base::error::provider::ProviderError;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::InstanceNameArguments;
use crate::core::system::exit::{CommandError, CommandResult};

pub async fn command_pin(
    _arguments: InstanceNameArguments,
    _globals: &GlobalArguments,
) -> CommandResult {
    Err(CommandError::from(ProviderError::CommandNotImplemented(
        "pin".to_owned(),
    )))
}
