//
// Copyright 2026 Tabs Data Inc.
//

use std::path::{Path, PathBuf};

use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
use td_common::status::ExitStatus::{Cancelled, GeneralError, NoAction};
use td_error::TdError;
use tracing::info;

use crate::base::constants::KUBECONFIG_FILE;
use crate::base::error::cli::CliError;
use crate::base::error::provider::ProviderError;
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::registry::{
    ClusterViews, InstanceSettings, InstanceState, instance_folder, instance_init_input_folder,
    instance_kube_folder,
};
use crate::client::session::pin::PinContext;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::commands::service::{
    resolved_cluster, resolved_details, resolved_instance, resolved_save, resolved_yes,
};
use crate::client::tdkserver::commands::template::get::get_templates;
use crate::client::tdkserver::common::prompt_yes_no;
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::{CommandResult, abort, abort_with};
use crate::runtime::instance::cluster::ClusterProperties;
use crate::runtime::provider::axes::Locality;
use crate::runtime::provider::{ClusterProvider, provider_for};

const DEFAULT_PROVIDER: ClusterProvider = ClusterProvider::Kminus;

#[derive(Debug, Clone, Args)]
pub struct InitArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance whose editable input config folder is produced. \
                     Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before initializing the instance. \
                     Defaults to 'true'. Pass '--details false' to skip the summary."
    )]
    pub details: Option<bool>,

    #[arg(
        long,
        name = "save",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 100,
        long_help = "Copy the cluster settings files (kubeconfig, etc.) into the instance \
                     folder so the instance is self-contained."
    )]
    pub save: Option<bool>,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 120,
        long_help = "Skip the interactive confirmation prompt and proceed immediately."
    )]
    pub yes: Option<bool>,
}

pub async fn command_init(arguments: InitArguments, globals: &GlobalArguments) -> CommandResult {
    let InitArguments {
        instance,
        cluster,
        save,
        details,
        yes,
    } = arguments;

    let pins = PinContext::pins();
    let instance = resolved_instance(instance, &pins);
    let (mut cluster, cluster_pinned) = resolved_cluster(cluster, &pins);
    let save = resolved_save(save, &pins);
    let details = resolved_details(details, &pins);
    let yes = resolved_yes(yes, &pins);
    let verbose = globals.verbose;

    let instance_pinned = instance.pinned;
    let instance_defaulted = instance.value.is_none();
    let provider_defaulted = cluster.provider.is_none() && !cluster_pinned.provider;
    let instance = instance
        .value
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    let provider = cluster.provider.unwrap_or(DEFAULT_PROVIDER);
    cluster.provider = Some(provider);

    if details.value {
        preview::init_service(
            &instance,
            instance_pinned,
            instance_defaulted,
            provider.as_str(),
            cluster_pinned.provider,
            provider_defaulted,
        );
    }
    if !yes.value && !prompt_yes_no("Proceed with the initialization?") {
        info!("Operation cancelled.");
        return abort(Cancelled);
    }

    let folder = instance_folder(&instance)?;
    if folder.exists() {
        return abort_with(
            NoAction,
            ProviderError::InstanceFolderExists(folder.display().to_string()),
        );
    }

    let provided = ClusterProperties {
        context: cluster.context.clone(),
        kubeconfig: cluster.kubeconfig.clone(),
        current: cluster.current,
        namespace: cluster.namespace.clone(),
    };

    let built = provider_for(
        cluster,
        InstanceArguments::new(Some(instance.clone())),
        verbose,
        None,
    )
    .await?;

    let mut adjusted = built.cluster_settings(provided.clone())?;
    if save.value
        && provider != ClusterProvider::Kminus
        && let Some(kubeconfig) = &adjusted.kubeconfig
    {
        adjusted.kubeconfig = Some(save_kubeconfig(&instance, kubeconfig)?);
    }
    let resolved = built.cluster_descriptor(adjusted.clone())?;

    let init_folder = instance_init_input_folder(&instance)?;

    let reporter = InteractiveReporter::new();

    let step = reporter.step("Creating tabsdata instance folder");
    if let Err(error) = std::fs::create_dir_all(&init_folder) {
        step.fail();
        return abort_with(
            GeneralError,
            CliError::FolderCreateFailed {
                folder: init_folder.display().to_string(),
                cause: error.to_string(),
            },
        );
    }
    step.done(&format!(
        "Created tabsdata instance folder at {}",
        folder.display()
    ));

    get_templates(&reporter, provider, &init_folder, verbose)?;

    let step = reporter.step("Writing tabsdata instance settings");
    let settings = InstanceSettings {
        state: InstanceState::Initialized,
        name: instance.clone(),
        provider,
        locality: Locality::Local,
        cluster: ClusterViews {
            provided,
            adjusted,
            resolved,
        },
        ports: Vec::new(),
    };
    if let Err(error) = settings.save() {
        step.fail();
        return Err(error);
    }
    step.done("Wrote tabsdata instance settings");

    report::component::service::initialized_service(&settings);

    Ok(())
}

fn save_kubeconfig(instance: &str, source: &Path) -> CommandResult<PathBuf> {
    let kube_folder = instance_kube_folder(instance)?;
    std::fs::create_dir_all(&kube_folder).map_err(|error| {
        TdError::from(CliError::FolderCreateFailed {
            folder: kube_folder.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    let file_name = source
        .file_name()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(KUBECONFIG_FILE));
    let target = kube_folder.join(file_name);
    std::fs::copy(source, &target).map_err(|error| {
        TdError::from(CliError::FileWriteFailed {
            file: target.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    Ok(target)
}
