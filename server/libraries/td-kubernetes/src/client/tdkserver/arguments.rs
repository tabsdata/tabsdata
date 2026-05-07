//
// Copyright 2026 Tabs Data Inc.
//

use std::path::PathBuf;

use clap::Parser;
use clap_derive::{Args, Subcommand};
use getset::Getters;

use crate::client::tdkserver::commands::create::CreateArguments;
use crate::client::tdkserver::commands::start::StartArguments;
use crate::client::tdkserver::commands::stop::StopArguments;
use crate::client::tdkserver::commands::unuse::UnuseArguments;
use crate::client::tdkserver::commands::r#use::UseArguments;
use crate::client::tdkserver::commands::using::UsingArguments;
use crate::fabric::ClusterFabric;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "Tabsdata Client for Tabsdata Server on Kubernetes",
    version = "1.10.0",
    about = "Client for the tabsdata server on kubernetes",
    long_about = "Manage tabsdata instances on kubernetes clusters."
)]
pub struct Arguments {
    #[arg(
        long,
        name = "accept-terms",
        required = false,
        global = true,
        long_help = "Accept or decline the license terms without interactive prompt [y/yes/n/no]."
    )]
    pub accept_terms: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Commands {
    #[command(about = "Open an interactive shell")]
    Shell,
    #[command(about = "Create a tabsdata instance")]
    Create(CreateArguments),
    #[command(about = "Use an existing tabsdata instance as the active context")]
    Use(UseArguments),
    #[command(about = "Show the active context")]
    Using(UsingArguments),
    #[command(about = "Clear the active context")]
    Unuse(UnuseArguments),
    #[command(about = "Start a tabsdata instance")]
    Start(StartArguments),
    #[command(about = "Stop a tabsdata instance")]
    Stop(StopArguments),
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct ClusterArguments {
    #[arg(
        long,
        name = "fabric",
        required = false,
        value_enum,
        long_help = "Fabric of the cluster: \
                     'kubernetes' for a standard kubernetes cluster, \
                     'kminus' for a development kminus cluster. \
                     When unspecified, the value from the active context is taken; \
                     when no active context is set, this argument is required."
    )]
    fabric: Option<ClusterFabric>,

    #[arg(
        long,
        name = "context",
        required = false,
        long_help = "Name of the kubeconfig context to use. \
                     Valid only for the 'kubernetes' fabric. \
                     Mutually exclusive with --current. \
                     Composes with --kubeconfig."
    )]
    context: Option<String>,

    #[arg(
        long,
        name = "kubeconfig",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Path to a kubeconfig file. \
                     Valid only for the 'kubernetes' fabric. \
                     When unspecified, the file pointed to by the KUBECONFIG environment variable, or ~/.kube/config, is used. \
                     Composes with --context and --current."
    )]
    kubeconfig: Option<PathBuf>,

    #[arg(
        long,
        name = "current",
        required = false,
        default_value_t = false,
        long_help = "Use the current-context entry of the kubeconfig file. \
                     Valid only for the 'kubernetes' fabric. \
                     Mutually exclusive with --context. \
                     Composes with --kubeconfig."
    )]
    current: bool,

    #[arg(
        long,
        name = "namespace",
        required = false,
        long_help = "Namespace for the instance. \
                     Valid only for the 'kubernetes' fabric. \
                     Overrides the namespace defined in the kubeconfig context. \
                     When neither is set, the kubernetes default namespace is used."
    )]
    namespace: Option<String>,
}

impl ClusterArguments {
    pub fn new(
        fabric: Option<ClusterFabric>,
        context: Option<String>,
        kubeconfig: Option<PathBuf>,
        current: bool,
        namespace: Option<String>,
    ) -> Self {
        Self {
            fabric,
            context,
            kubeconfig,
            current,
            namespace,
        }
    }
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct InstanceArguments {
    #[arg(
        long,
        name = "name",
        required = false,
        long_help = "Instance name. \
                     Valid only for the 'kminus' fabric. \
                     The instance is created in ~/.tabsdata/instances/<name>. \
                     Mutually exclusive with --path."
    )]
    name: Option<String>,

    #[arg(
        long,
        name = "path",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Instance path. \
                     Valid only for the 'kminus' fabric. \
                     Relative paths resolve under ~/.tabsdata/instances. \
                     Absolute paths are used as-is. \
                     Mutually exclusive with --name."
    )]
    path: Option<PathBuf>,
}

impl InstanceArguments {
    pub fn new(name: Option<String>, path: Option<PathBuf>) -> Self {
        Self { name, path }
    }
}
