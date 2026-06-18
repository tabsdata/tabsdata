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
    ClusterViews, InstanceSettings, InstanceState, instance_folder, instance_init_folder,
    instance_kube_folder,
};
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::commands::template::get::get_templates;
use crate::client::tdkserver::common::{effective_yes, prompt_yes_no};
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
        long_help = "Name of the instance whose editable input config folder is produced. \
                     Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[arg(
        long,
        name = "preview",
        required = false,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values before initializing the instance. \
                     Defaults to 'true'. Pass '--preview false' to skip the summary."
    )]
    pub preview: bool,

    #[arg(
        long,
        name = "save",
        required = false,
        default_value_t = false,
        long_help = "Copy the cluster settings files (kubeconfig, etc.) into the instance \
                     folder so the instance is self-contained."
    )]
    pub save: bool,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        default_value_t = false,
        long_help = "Skip the interactive confirmation prompt and proceed immediately."
    )]
    pub yes: bool,
}

pub async fn command_init(arguments: InitArguments, globals: &GlobalArguments) -> CommandResult {
    let InitArguments {
        instance,
        mut cluster,
        save,
        preview,
        yes,
    } = arguments;

    let instance_defaulted = instance.is_none();
    let provider_defaulted = cluster.provider.is_none();
    let instance = instance.unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    let provider = cluster.provider.unwrap_or(DEFAULT_PROVIDER);
    cluster.provider = Some(provider);

    if preview {
        preview::init_service(
            &instance,
            instance_defaulted,
            provider.as_str(),
            provider_defaulted,
        );
    }
    if !effective_yes(yes) && !prompt_yes_no("Proceed with the initialization?") {
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
        globals.verbose,
    )
    .await?;

    let mut adjusted = built.cluster_settings(provided.clone())?;
    if save
        && provider != ClusterProvider::Kminus
        && let Some(kubeconfig) = &adjusted.kubeconfig
    {
        adjusted.kubeconfig = Some(save_kubeconfig(&instance, kubeconfig)?);
    }
    let resolved = built.cluster_descriptor(adjusted.clone())?;

    let init_folder = instance_init_folder(&instance)?;

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

    get_templates(&reporter, provider, &init_folder, globals.verbose)?;

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
    };
    if let Err(error) = settings.save() {
        step.fail();
        return Err(error);
    }
    step.done("Wrote tabsdata instance settings");

    report::service::initialized_service(&settings);

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
