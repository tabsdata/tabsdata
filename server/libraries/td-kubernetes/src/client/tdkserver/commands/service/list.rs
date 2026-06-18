//
// Copyright 2026 Tabs Data Inc.
//

use crate::base::error::provider::ProviderError;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::{CommandError, CommandResult};

pub async fn command_list(_globals: &GlobalArguments) -> CommandResult {
    Err(CommandError::from(ProviderError::CommandNotImplemented(
        "list".to_owned(),
    )))
}
