//
// Copyright 2026 Tabs Data Inc.
//

use std::path::PathBuf;

use clap_derive::Args;
use td_common::status::ExitStatus::NoAction;

use crate::client::session::pin::{PinContext, Pins};
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::{CommandResult, abort};
use crate::cwarn;
use crate::runtime::provider::ClusterProvider;

#[derive(Debug, Clone, Args)]
pub struct PinArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Pin the instance name used when a command omits --instance."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "provider",
        required = false,
        value_enum,
        display_order = 20,
        long_help = "Pin the provider used when a command omits --provider."
    )]
    pub provider: Option<ClusterProvider>,

    #[arg(
        long,
        name = "kubeconfig",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        display_order = 30,
        long_help = "Pin the kubeconfig path used when a command omits --kubeconfig."
    )]
    pub kubeconfig: Option<PathBuf>,

    #[arg(
        long,
        name = "context",
        required = false,
        display_order = 40,
        long_help = "Pin the kubeconfig context used when a command omits --context."
    )]
    pub context: Option<String>,

    #[arg(
        long,
        name = "namespace",
        required = false,
        display_order = 50,
        long_help = "Pin the namespace used when a command omits --namespace."
    )]
    pub namespace: Option<String>,

    #[arg(
        long,
        name = "save",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 100,
        long_help = "Pin the --save value used when a command omits --save."
    )]
    pub save: Option<bool>,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Pin the --details value used when a command omits --details."
    )]
    pub details: Option<bool>,

    #[arg(
        long,
        name = "autoconfirm",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 120,
        long_help = "Pin the autoconfirm value (accepting 'true'/'false') used as the \
                     default for --yes when a command omits it."
    )]
    pub autoconfirm: Option<bool>,
}

pub async fn command_pin(arguments: PinArguments, _globals: &GlobalArguments) -> CommandResult {
    let PinArguments {
        instance,
        provider,
        kubeconfig,
        context,
        namespace,
        save,
        details,
        autoconfirm,
    } = arguments;

    let values = Pins {
        instance,
        provider,
        kubeconfig,
        context,
        namespace,
        save,
        details,
        autoconfirm,
    };

    if values.is_empty() {
        cwarn!("No values provided to pin.");
        return abort(NoAction);
    }

    PinContext::pin_values(values.clone())?;
    values.report("Pinned tabsdata values:");
    Ok(())
}
