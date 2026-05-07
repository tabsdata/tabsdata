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
use crate::fabric::ClusterFabric;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "Tabsdata Client for Tabsdata Server on Kubernetes",
    version = "1.10.0",
    about = "Tabsdata Client for Tabsdata Server on Kubernetes",
    long_about = "Manage Tabsdata instances on Kubernetes clusters."
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
    #[command(about = "Create a Tabsdata instance")]
    Create(CreateArguments),
    #[command(about = "Start a Tabsdata instance")]
    Start(StartArguments),
    #[command(about = "Stop a Tabsdata instance")]
    Stop(StopArguments),
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct ClusterArguments {
    #[arg(
        long,
        name = "fabric",
        required = true,
        value_enum,
        long_help = "Fabric of the Kubernetes cluster: \
                     'kubernetes' for a standard Kubernetes cluster, \
                     'kminus' for a development KMinus cluster."
    )]
    fabric: ClusterFabric,

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
        long_help = "Kubernetes namespace for the instance. \
                     Valid only for the 'kubernetes' fabric. \
                     Overrides the namespace defined in the kubeconfig context. \
                     When neither is set, the Kubernetes default namespace is used."
    )]
    namespace: Option<String>,
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
