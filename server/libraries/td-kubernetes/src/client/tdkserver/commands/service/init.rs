//
// Copyright 2026 Tabsdata Inc.
//

use std::path::{Path, PathBuf};

use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
use td_common::status::ExitStatus::{Cancelled, GeneralError, NoAction};
use td_error::TdError;
use td_python::tabsdata::tabsdata_version;
use te_system::edition::{Edition, TabsdataEdition};
use tracing::info;

use crate::base::constants::{DEFAULT_LOG_LEVEL, KUBECONFIG_FILE, LogLevel, SCHEDULER_FARGATE};
use crate::base::error::cli::CliError;
use crate::base::error::manifest::ManifestError;
use crate::base::error::provider::ProviderError;
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::registry::{
    ClusterViews, InstanceSettings, InstanceState, instance_folder, instance_init_input_folder,
    instance_kube_folder,
};
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::commands::service::{
    resolved_cluster, resolved_details, resolved_instance, resolved_save, resolved_yes,
};
use crate::client::tdkserver::commands::template::get::get_templates;
use crate::client::tdkserver::common::confirm;
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::{CommandResult, abort, abort_with};
use crate::kube::api::entity::namespace::ensure_namespace_exists;
use crate::kube::api::entity::pod::probe_scheduler;
use crate::kube::api::support::cluster::ensure_cluster_reachable;
use crate::runtime::instance::cluster::ClusterProperties;
use crate::runtime::provider::{ClusterProvider, provider_for};

// const DEFAULT_PROVIDER: ClusterProvider = ClusterProvider::Kminus;

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
        name = "loglevel",
        value_enum,
        required = false,
        display_order = 20,
        long_help = "Log level for the kminus orchestrator and pod runner. \
                     Defaults to 'debug'. Only valid with the 'kminus' provider.\n\n\
                     Example:\n  \
                     tdk init --provider kminus --loglevel info"
    )]
    pub loglevel: Option<LogLevel>,

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
    init_instance(arguments, globals, true).await
}

pub async fn init_instance(
    arguments: InitArguments,
    globals: &GlobalArguments,
    with_hint: bool,
) -> CommandResult {
    let InitArguments {
        instance,
        cluster,
        save,
        details,
        yes,
        loglevel,
    } = arguments;

    let settings = SettingsContext::settings();
    let instance = resolved_instance(instance, &settings);
    let (mut cluster, cluster_in_settings) = resolved_cluster(cluster, &settings);
    let save = resolved_save(save, &settings);
    let details = resolved_details(details, &settings);
    let yes = resolved_yes(yes, &settings);
    let verbose = globals.verbose;

    let instance_in_settings = instance.in_settings;
    let instance_defaulted = instance.value.is_none();
    // let provider_defaulted = cluster.provider.is_none() && !cluster_in_settings.provider;
    let instance = instance
        .value
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    // let provider = cluster.provider.unwrap_or(DEFAULT_PROVIDER);
    let Some(provider) = cluster.provider else {
        return abort_with(NoAction, ProviderError::ProviderNotSupplied);
    };
    cluster.provider = Some(provider);

    if !provider.supported() {
        return abort_with(
            NoAction,
            ProviderError::NotImplemented(provider.as_str().to_owned()),
        );
    }

    if provider != ClusterProvider::Kminus && loglevel.is_some() {
        return abort_with(
            NoAction,
            ProviderError::ArgumentNotValidForProvider {
                argument: "loglevel".to_owned(),
                provider: provider.as_str().to_owned(),
            },
        );
    }
    let loglevel_defaulted = loglevel.is_none();
    let loglevel =
        (provider == ClusterProvider::Kminus).then(|| loglevel.unwrap_or(DEFAULT_LOG_LEVEL));

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
        None,
    )
    .await?;

    if details.value {
        if provider == ClusterProvider::Kminus {
            preview::init_service(
                &instance,
                instance_in_settings,
                instance_defaulted,
                provider.as_str(),
                cluster_in_settings.provider,
                loglevel.map(|level| (level.as_str(), loglevel_defaulted)),
            );
        } else {
            preview::init_cluster_service(&built.view());
        }
    }
    if !confirm(yes.value, "Proceed with the initialization?")? {
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

    if provider != ClusterProvider::Kminus {
        let context = built.kube_context().await?;
        ensure_cluster_reachable(context.client()).await?;
        ensure_namespace_exists(&context).await?;
        if provider == ClusterProvider::Fargate
            && let Some(scheduler) = probe_scheduler(&context).await?
            && scheduler != SCHEDULER_FARGATE
        {
            return Err(ProviderError::FargateProfileMissing {
                namespace: context.namespace().to_owned(),
                scheduler,
                expected: SCHEDULER_FARGATE.to_owned(),
            }
            .into());
        }
        built.ensure_creatable_service().await?;
    }

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
        locality: provider.locality(),
        cluster: ClusterViews {
            provided,
            adjusted,
            resolved,
        },
        version: tabsdata_version()
            .map_err(|error| ManifestError::TabsdataVersionResolveFailed(error.to_string()))?,
        edition: TabsdataEdition.label().to_owned(),
        folder: folder.clone(),
        ports: Vec::new(),
        loglevel,
    };
    if let Err(error) = settings.save() {
        step.fail();
        return Err(error);
    }
    step.done("Wrote tabsdata instance settings");

    report::component::service::initialized_service(&settings, with_hint);

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
