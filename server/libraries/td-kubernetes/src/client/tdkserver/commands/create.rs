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

    #[cfg(td_k8s_development)]
    #[arg(
        long,
        name = "profile",
        required = false,
        long_help = "Profile to use when generating kubernetes manifests. \
                     When unspecified, defaults to 'production'."
    )]
    profile: Option<String>,

    #[arg(
        long,
        name = "preview",
        required = false,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values and prompt for confirmation \
                     before creating the instance. Defaults to 'true'. \
                     Pass '--preview false' to skip the summary and prompt."
    )]
    preview: bool,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        default_value_t = false,
        long_help = "Skip the interactive confirmation prompt and proceed immediately. \
                     Has no effect when --preview is 'false'."
    )]
    yes: bool,
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
        preview,
        yes,
        ..
    } = arguments;

    let fabric = fabric_for(cluster, instance).await?;
    fabric.ensure_creatable().await?;

    if preview {
        fabric.preview_create_summary(&profile);
        if !yes && !prompt_yes_no("Proceed with the creation?") {
            info!("Operation cancelled.");
            return Ok(());
        }
    }

    let cleanup = fabric.prepare_workspace().await?;
    fabric.stage_configmap(&profile).await?;
    fabric.store_configmap(&profile).await?;
    cleanup.commit();
    fabric.report_created();

    Ok(())
}
