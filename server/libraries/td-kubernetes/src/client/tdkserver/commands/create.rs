//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use getset::Getters;
use tracing::info;

use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::constants::PROFILE_PRODUCTION;
use crate::exit::Outcome;
use crate::fabric::fabric_for;

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct CreateArguments {
    #[command(flatten)]
    cluster: ClusterArguments,

    #[command(flatten)]
    instance: InstanceArguments,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        default_value_t = false,
        long_help = "Skip the interactive confirmation prompt and proceed immediately. \
                     The summary of resolved values is always shown beforehand."
    )]
    yes: bool,

    #[cfg(td_k8s_development)]
    #[arg(
        long,
        name = "profile",
        required = false,
        long_help = "kprofile to use when generating Kubernetes manifests. \
                     When unspecified, defaults to 'production'."
    )]
    profile: Option<String>,
}

#[cfg(td_k8s_development)]
fn resolve_profile(arguments: &CreateArguments) -> String {
    arguments
        .profile()
        .as_deref()
        .unwrap_or(PROFILE_PRODUCTION)
        .to_owned()
}

#[cfg(not(td_k8s_development))]
fn resolve_profile(_arguments: &CreateArguments) -> String {
    PROFILE_PRODUCTION.to_owned()
}

pub async fn command_create(arguments: CreateArguments) -> Outcome {
    let profile = resolve_profile(&arguments);
    let CreateArguments {
        cluster,
        instance,
        yes,
        ..
    } = arguments;

    let fabric = fabric_for(cluster, instance).await?;
    fabric.ensure_creatable().await?;
    fabric.preview_create_summary(&profile);

    if !yes && !prompt_yes_no("Proceed with the creation?") {
        info!("Operation cancelled.");
        return Ok(());
    }

    let cleanup = fabric.prepare_workspace().await?;
    fabric.stage_configmap(&profile).await?;
    fabric.store_configmap(&profile).await?;
    cleanup.commit();
    fabric.report_created();

    Ok(())
}
