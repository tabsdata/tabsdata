//
// Copyright 2026 Tabs Data Inc.
//

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::session::unpin::{
    UnpinArguments, command_unpin as command_session_unpin,
};
use crate::core::system::exit::CommandResult;

pub async fn command_unpin(globals: &GlobalArguments) -> CommandResult {
    let _ = globals;
    command_session_unpin(UnpinArguments {}).await
}
