//
// Copyright 2026 Tabs Data Inc.
//

use crate::base::error::provider::ProviderError;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::{CommandError, CommandResult};

pub async fn command_unpin(_globals: &GlobalArguments) -> CommandResult {
    Err(CommandError::from(ProviderError::CommandNotImplemented(
        "unpin".to_owned(),
    )))
}
