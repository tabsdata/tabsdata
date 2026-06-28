//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use td_common::status::ExitStatus::Cancelled;
use tracing::info;

use crate::client::session::pin::{PinContext, PinSelection};
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::common::prompt_yes_no;
use crate::core::system::exit::{CommandResult, abort};
use crate::cwarn;

#[derive(Debug, Clone, Args)]
pub struct UnpinArguments {
    #[arg(long, name = "instance", required = false, display_order = 10)]
    pub instance: bool,

    #[arg(long, name = "provider", required = false, display_order = 20)]
    pub provider: bool,

    #[arg(long, name = "kubeconfig", required = false, display_order = 30)]
    pub kubeconfig: bool,

    #[arg(long, name = "context", required = false, display_order = 40)]
    pub context: bool,

    #[arg(long, name = "namespace", required = false, display_order = 50)]
    pub namespace: bool,

    #[arg(long, name = "save", required = false, display_order = 100)]
    pub save: bool,

    #[arg(long, name = "preview", required = false, display_order = 110)]
    pub preview: bool,

    #[arg(long, name = "autoconfirm", required = false, display_order = 120)]
    pub autoconfirm: bool,
}

pub async fn command_unpin(arguments: UnpinArguments, _globals: &GlobalArguments) -> CommandResult {
    let UnpinArguments {
        instance,
        provider,
        kubeconfig,
        context,
        namespace,
        save,
        preview,
        autoconfirm,
    } = arguments;

    let selection = PinSelection {
        instance,
        provider,
        kubeconfig,
        context,
        namespace,
        save,
        preview,
        autoconfirm,
    };

    let cleared = if selection.any() {
        PinContext::unpin_values(&selection)?
    } else {
        if !prompt_yes_no("Unpin all pinned tabsdata values?") {
            info!("Operation cancelled.");
            return abort(Cancelled);
        }
        PinContext::unpin_all()?
    };

    if cleared {
        info!("Cleared the requested pinned tabsdata values.");
    } else {
        cwarn!("No matching pinned tabsdata values to clear.");
    }
    Ok(())
}
