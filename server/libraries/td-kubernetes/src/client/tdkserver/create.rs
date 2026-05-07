//
// Copyright 2026 Tabs Data Inc.
//

use std::fs;
use std::path::PathBuf;
use std::process::exit;

use clap_derive::Args;
use getset::Getters;
use kube::Config;
use td_common::env::TABSDATA_HOME_DIR;
use td_common::server::{DEFAULT_INSTANCE, INSTANCES_FOLDER};
use td_common::status::ExitStatus::GeneralError;
use tracing::{error, info};

use crate::client::tdkserver::arguments::{ClusterArguments, ClusterFabric, InstanceArguments};
use crate::client::tdkserver::common::{prompt_yes_no, validate_instance_name};
use crate::constants::{FABRIC_KMINUS, FABRIC_KUBERNETES};
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
}

struct ResolvedInstance {
    name: String,
    folder: Option<PathBuf>,
}

pub async fn command_create(arguments: CreateArguments) {
    let CreateArguments {
        cluster,
        instance,
        confirm,
    } = arguments;

    match cluster.fabric() {
        ClusterFabric::Kubernetes => validate_kubernetes_arguments(&cluster, &instance),
        ClusterFabric::Kminus => validate_kminus_arguments(&cluster, &instance),
    }

    let resolved = resolve_instance(*cluster.fabric(), &instance);

    if let Err(error) = validate_instance_name(&resolved.name) {
        error!("{error}");
        exit(GeneralError.code());
    }

    let config = if matches!(cluster.fabric(), ClusterFabric::Kubernetes) {
        let kubeconfig_path = cluster.kubeconfig().as_deref();
        let context = cluster.context().as_deref();
        let namespace_override = cluster.namespace().as_deref();
        if resolve_kubeconfig_path(kubeconfig_path).is_none() {
            error!(
                "No kubeconfig found. Set the KUBECONFIG environment variable, create ~/.kube/config, or pass --kubeconfig."
            );
            exit(GeneralError.code());
        }
        let config = build_kubeconfig(kubeconfig_path, context, namespace_override).await;
        let client = build_kube_client(&config);
        ensure_cluster_reachable(&client).await;
        ensure_namespace_exists(&client, &config.default_namespace).await;
        Some(config)
    } else {
        None
    };

    print_summary(&cluster, &resolved, config.as_ref());

    if confirm && !prompt_yes_no("Proceed with the creation?") {
        info!("Operation cancelled.");
        return;
    }

    match cluster.fabric() {
        ClusterFabric::Kubernetes => {
            let config = config.expect("kubernetes config built earlier");
            info!(
                "Would create Tabsdata instance '{}' in namespace '{}' (placeholder).",
                resolved.name, config.default_namespace
            );
        }
        ClusterFabric::Kminus => {
            let folder = resolved.folder.expect("kminus resolution sets folder");
            if folder.exists() {
                error!("Folder {} already exists.", folder.display());
                exit(GeneralError.code());
            }
            if let Err(error) = fs::create_dir_all(&folder) {
                error!(
                    "Failed to create instance folder {}: {error}",
                    folder.display()
                );
                exit(GeneralError.code());
            }
            info!("Created Tabsdata instance at {}.", folder.display());
        }
    }
}

fn validate_kubernetes_arguments(cluster: &ClusterArguments, instance: &InstanceArguments) {
    if instance.path().is_some() {
        error!("Argument --path is not valid for the '{FABRIC_KUBERNETES}' fabric.");
        exit(GeneralError.code());
    }
    if cluster.context().is_some() && *cluster.current() {
        error!("Arguments --context and --current are mutually exclusive.");
        exit(GeneralError.code());
    }
    if cluster.context().is_none() && !*cluster.current() {
        error!("Specify --context or --current to identify the kubeconfig context.");
        exit(GeneralError.code());
    }
}

fn validate_kminus_arguments(cluster: &ClusterArguments, instance: &InstanceArguments) {
    if cluster.context().is_some()
        || cluster.kubeconfig().is_some()
        || *cluster.current()
        || cluster.namespace().is_some()
    {
        error!(
            "Arguments --context, --kubeconfig, --current, and --namespace are not valid for the '{FABRIC_KMINUS}' fabric."
        );
        exit(GeneralError.code());
    }
    if instance.name().is_some() && instance.path().is_some() {
        error!("Arguments --name and --path are mutually exclusive.");
        exit(GeneralError.code());
    }
}

fn resolve_instance(fabric: ClusterFabric, instance: &InstanceArguments) -> ResolvedInstance {
    match fabric {
        ClusterFabric::Kubernetes => {
            let name = instance
                .name()
                .clone()
                .unwrap_or_else(|| DEFAULT_INSTANCE.to_string());
            ResolvedInstance { name, folder: None }
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
                    unreachable!("name and path were already validated as exclusive")
                }
            };
            ResolvedInstance {
                name,
                folder: Some(folder),
            }
        }
    }
}

fn print_summary(cluster: &ClusterArguments, resolved: &ResolvedInstance, config: Option<&Config>) {
    let fabric_name = match cluster.fabric() {
        ClusterFabric::Kubernetes => FABRIC_KUBERNETES,
        ClusterFabric::Kminus => FABRIC_KMINUS,
    };
    info!("Tabsdata instance to be created:");
    info!("  fabric:        {fabric_name}");
    info!("  instance name: {}", resolved.name);
    match cluster.fabric() {
        ClusterFabric::Kubernetes => {
            let kubeconfig_path = cluster.kubeconfig().as_deref();
            let context = cluster.context().as_deref();
            match resolve_kubeconfig_path(kubeconfig_path) {
                Some(path) => info!("  kubeconfig:    {}", path.display()),
                None => info!("  kubeconfig:    <not found>"),
            }
            if let Some(context) = cluster.context() {
                info!("  context:       {context}");
            } else if *cluster.current() {
                match resolve_current_context(kubeconfig_path) {
                    Some(name) => info!("  context:       {name}"),
                    None => info!("  context:       <unable to resolve current-context>"),
                }
            }
            if let Some(config) = config {
                info!("  cluster url:   {}", config.cluster_url);
            }
            match resolve_cluster_name(kubeconfig_path, context) {
                Some(name) => info!("  cluster name:  {name}"),
                None => info!("  cluster name:  <unable to resolve cluster name>"),
            }
            if let Some(config) = config {
                info!("  namespace:     {}", config.default_namespace);
            }
        }
        ClusterFabric::Kminus => {
            if let Some(folder) = &resolved.folder {
                info!("  instance path: {}", folder.display());
            }
        }
    }
}
