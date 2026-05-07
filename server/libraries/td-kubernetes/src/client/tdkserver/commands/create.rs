//
// Copyright 2026 Tabs Data Inc.
//

use std::fs;
use std::path::PathBuf;

use clap_derive::Args;
use getset::Getters;
use kube::Config;
use td_common::env::TABSDATA_HOME_DIR;
use td_common::server::{DEFAULT_INSTANCE, INSTANCES_FOLDER};
use td_common::status::ExitStatus::GeneralError;
use tracing::{error, info};

use crate::client::tdkserver::arguments::{ClusterArguments, ClusterFabric, InstanceArguments};
use crate::client::tdkserver::common::{prompt_yes_no, validate_instance_name};
use crate::constants::{FABRIC_KMINUS, FABRIC_KUBERNETES, FOLDER_CLUSTER, PROFILE_PRODUCTION};
use crate::exit::{Outcome, abort};
use crate::kminus::action::create_cluster;
use crate::kminus::model::KminusInstance;
use crate::kubernetes::action::{ensure_cluster_reachable, ensure_namespace_exists};
use crate::kubernetes::client::build_kube_client;
use crate::kubernetes::toolkit::{
    build_kubeconfig, resolve_cluster_name, resolve_current_context, resolve_kubeconfig_path,
};

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct CreateArguments {
    #[command(flatten)]
    cluster: ClusterArguments,

    #[command(flatten)]
    instance: InstanceArguments,

    #[arg(
        long,
        name = "confirm",
        required = false,
        default_value_t = false,
        long_help = "Ask for interactive confirmation before creating the instance. \
                     The summary of resolved values is always shown beforehand. \
                     Without this flag, the instance is created without prompting."
    )]
    confirm: bool,

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

struct ResolvedCluster {
    fabric: ClusterFabric,
    kubeconfig_path: Option<PathBuf>,
    context: Option<String>,
    cluster_name: Option<String>,
    config: Option<Config>,
}

struct ResolvedInstance {
    name: String,
    folder: Option<PathBuf>,
}

pub async fn command_create(arguments: CreateArguments) -> Outcome {
    let profile = resolve_profile(&arguments);
    let CreateArguments {
        cluster,
        instance,
        confirm,
        ..
    } = arguments;

    match cluster.fabric() {
        ClusterFabric::Kubernetes => validate_kubernetes_arguments(&cluster, &instance)?,
        ClusterFabric::Kminus => validate_kminus_arguments(&cluster, &instance)?,
    }

    let resolved_cluster = resolve_cluster(&cluster).await?;

    if let Some(config) = &resolved_cluster.config {
        let client = build_kube_client(config)?;
        ensure_cluster_reachable(&client).await?;
        ensure_namespace_exists(&client, &config.default_namespace).await?;
    }

    let resolved_instance = resolve_instance(&resolved_cluster, &instance)?;

    if let Err(error) = validate_instance_name(&resolved_instance.name) {
        error!("{error}");
        return abort(GeneralError);
    }

    print_summary(&profile, &resolved_cluster, &resolved_instance);

    if confirm && !prompt_yes_no("Proceed with the creation?") {
        info!("Operation cancelled.");
        return Ok(());
    }

    match resolved_cluster.fabric {
        ClusterFabric::Kubernetes => {
            let Some(config) = resolved_cluster.config else {
                error!("Failed to resolve the Kubernetes configuration.");
                return abort(GeneralError);
            };
            info!(
                "Would create Tabsdata instance '{}' in namespace '{}' (placeholder).",
                resolved_instance.name, config.default_namespace
            );
        }
        ClusterFabric::Kminus => {
            let Some(folder) = resolved_instance.folder else {
                error!("Failed to resolve the KMinus instance folder.");
                return abort(GeneralError);
            };
            if folder.exists() {
                error!("Folder {} already exists.", folder.display());
                return abort(GeneralError);
            }
            if let Err(error) = fs::create_dir_all(&folder) {
                error!(
                    "Failed to create instance folder {}: {error}",
                    folder.display()
                );
                return abort(GeneralError);
            }
            let kminus_instance = KminusInstance::new(folder.join(FOLDER_CLUSTER));
            create_cluster(&kminus_instance)?;
            info!("Created tabsdata instance at {}.", folder.display());
            info!(
                "Edit '{}' to customize ports or log settings of the instance's kminus cluster.",
                kminus_instance.sysconfig_path().display()
            );
        }
    }

    Ok(())
}

fn validate_kubernetes_arguments(
    cluster: &ClusterArguments,
    instance: &InstanceArguments,
) -> Outcome {
    let provided_clusters = [
        cluster.kubeconfig().is_some(),
        cluster.context().is_some(),
        *cluster.current(),
    ]
    .into_iter()
    .filter(|flag| *flag)
    .count();
    if provided_clusters == 0 {
        error!("Specify --kubeconfig, --context, or --current to identify the target cluster.");
        return abort(GeneralError);
    }
    if provided_clusters > 1 {
        error!("Arguments --kubeconfig, --context, and --current are mutually exclusive.");
        return abort(GeneralError);
    }
    if instance.name().is_some() {
        error!("Argument --name is not valid for the '{FABRIC_KUBERNETES}' fabric.");
        return abort(GeneralError);
    }
    if instance.path().is_some() {
        error!("Argument --path is not valid for the '{FABRIC_KUBERNETES}' fabric.");
        return abort(GeneralError);
    }
    Ok(())
}

fn validate_kminus_arguments(cluster: &ClusterArguments, instance: &InstanceArguments) -> Outcome {
    if cluster.context().is_some()
        || cluster.kubeconfig().is_some()
        || *cluster.current()
        || cluster.namespace().is_some()
    {
        error!(
            "Arguments --context, --kubeconfig, --current, and --namespace are not valid for the '{FABRIC_KMINUS}' fabric."
        );
        return abort(GeneralError);
    }
    if instance.name().is_some() && instance.path().is_some() {
        error!("Arguments --name and --path are mutually exclusive.");
        return abort(GeneralError);
    }
    Ok(())
}

fn resolve_instance(
    cluster: &ResolvedCluster,
    instance: &InstanceArguments,
) -> Outcome<ResolvedInstance> {
    match cluster.fabric {
        ClusterFabric::Kubernetes => {
            let name = cluster
                .config
                .as_ref()
                .map(|config| config.default_namespace.trim())
                .filter(|namespace| !namespace.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| DEFAULT_INSTANCE.to_string());
            Ok(ResolvedInstance { name, folder: None })
        }
        ClusterFabric::Kminus => {
            let instances_root = dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/"))
                .join(TABSDATA_HOME_DIR)
                .join(INSTANCES_FOLDER);
            let (name, folder) = match (instance.name().as_deref(), instance.path().as_deref()) {
                (None, None) => {
                    let name = DEFAULT_INSTANCE.to_string();
                    let folder = instances_root.join(&name);
                    (name, folder)
                }
                (Some(name), None) => {
                    let folder = instances_root.join(name);
                    (name.to_owned(), folder)
                }
                (None, Some(path)) => {
                    let folder = if path.is_relative() {
                        instances_root.join(path)
                    } else {
                        path.to_path_buf()
                    };
                    let name = folder
                        .file_name()
                        .and_then(|component| component.to_str())
                        .map(str::to_owned)
                        .unwrap_or_else(|| DEFAULT_INSTANCE.to_string());
                    (name, folder)
                }
                (Some(_), Some(_)) => {
                    error!("Arguments --name and --path are mutually exclusive.");
                    return abort(GeneralError);
                }
            };
            Ok(ResolvedInstance {
                name,
                folder: Some(folder),
            })
        }
    }
}

async fn resolve_cluster(cluster: &ClusterArguments) -> Outcome<ResolvedCluster> {
    match cluster.fabric() {
        ClusterFabric::Kubernetes => {
            let kubeconfig_argument = cluster.kubeconfig().as_deref();
            let context_argument = cluster.context().as_deref();
            let namespace_override = cluster.namespace().as_deref();

            let kubeconfig_path = resolve_kubeconfig_path(kubeconfig_argument);
            if kubeconfig_path.is_none() {
                error!(
                    "No kubeconfig found. Set the KUBECONFIG environment variable, create ~/.kube/config, or pass --kubeconfig."
                );
                return abort(GeneralError);
            }

            let config =
                build_kubeconfig(kubeconfig_argument, context_argument, namespace_override).await?;

            let context = cluster
                .context()
                .clone()
                .or_else(|| resolve_current_context(kubeconfig_argument));
            let cluster_name = resolve_cluster_name(kubeconfig_argument, context_argument);

            Ok(ResolvedCluster {
                fabric: *cluster.fabric(),
                kubeconfig_path,
                context,
                cluster_name,
                config: Some(config),
            })
        }
        ClusterFabric::Kminus => Ok(ResolvedCluster {
            fabric: *cluster.fabric(),
            kubeconfig_path: None,
            context: None,
            cluster_name: None,
            config: None,
        }),
    }
}

fn print_summary(profile: &str, cluster: &ResolvedCluster, instance: &ResolvedInstance) {
    let fabric_name = match cluster.fabric {
        ClusterFabric::Kubernetes => FABRIC_KUBERNETES,
        ClusterFabric::Kminus => FABRIC_KMINUS,
    };
    info!("Tabsdata instance to be created:");
    if cfg!(td_k8s_development) {
        info!("  - profile......: {profile}");
    }
    info!("  - fabric.......: {fabric_name}");
    info!("  - instance name: {}", instance.name);
    match cluster.fabric {
        ClusterFabric::Kubernetes => {
            match &cluster.kubeconfig_path {
                Some(path) => info!("  - kubeconfig...: {}", path.display()),
                None => info!("  - kubeconfig...: <not found>"),
            }
            match &cluster.context {
                Some(name) => info!("  - context......: {name}"),
                None => info!("  - context......: <unable to resolve current-context>"),
            }
            if let Some(config) = &cluster.config {
                info!("  - cluster url..: {}", config.cluster_url);
            }
            match &cluster.cluster_name {
                Some(name) => info!("  - cluster name.: {name}"),
                None => info!("  - cluster name.: <unable to resolve cluster name>"),
            }
            if let Some(config) = &cluster.config {
                info!("  - namespace....: {}", config.default_namespace);
            }
        }
        ClusterFabric::Kminus => {
            if let Some(folder) = &instance.folder {
                info!("  - instance path: {}", folder.display());
            }
        }
    }
}
