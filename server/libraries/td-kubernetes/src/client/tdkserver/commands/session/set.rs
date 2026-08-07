//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;

use clap_derive::Args;
use td_common::status::ExitStatus::NoAction;

use crate::base::error::provider::ProviderError;
use crate::client::session::settings::{Settings, SettingsContext};
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::{CommandResult, abort, abort_with};
use crate::cwarn;
use crate::runtime::provider::ClusterProvider;

#[derive(Debug, Clone, Args)]
pub struct SetArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Set the instance name used when a command omits --instance."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "provider",
        required = false,
        value_enum,
        display_order = 20,
        long_help = "Set the provider used when a command omits --provider."
    )]
    pub provider: Option<ClusterProvider>,

    #[arg(
        long,
        name = "kubeconfig",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        display_order = 30,
        long_help = "Set the kubeconfig path used when a command omits --kubeconfig."
    )]
    pub kubeconfig: Option<PathBuf>,

    #[arg(
        long,
        name = "context",
        required = false,
        display_order = 40,
        long_help = "Set the kubeconfig context used when a command omits --context."
    )]
    pub context: Option<String>,

    #[arg(
        long,
        name = "namespace",
        required = false,
        display_order = 50,
        long_help = "Set the namespace used when a command omits --namespace."
    )]
    pub namespace: Option<String>,

    #[arg(
        long,
        name = "save",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 100,
        long_help = "Set the --save value used when a command omits --save."
    )]
    pub save: Option<bool>,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Set the --details value used when a command omits --details."
    )]
    pub details: Option<bool>,

    #[arg(
        long,
        name = "autoconfirm",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 120,
        long_help = "Set the autoconfirm value (accepting 'true'/'false') used as the \
                     default for --yes when a command omits it."
    )]
    pub autoconfirm: Option<bool>,
}

pub async fn command_set(arguments: SetArguments, _globals: &GlobalArguments) -> CommandResult {
    let SetArguments {
        instance,
        provider,
        kubeconfig,
        context,
        namespace,
        save,
        details,
        autoconfirm,
    } = arguments;

    if let Some(provider) = provider
        && !provider.supported()
    {
        return abort_with(
            NoAction,
            ProviderError::NotImplemented(provider.as_str().to_owned()),
        );
    }

    let values = Settings {
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
        cwarn!("No values provided to set.");
        return abort(NoAction);
    }

    SettingsContext::set_values(values.clone())?;
    values.report("Tabsdata session settings:");
    Ok(())
}
