//
// Copyright 2026 Tabs Data Inc.
//

use std::path::PathBuf;

use clap::Parser;
use clap_derive::{Args, Subcommand};

use crate::client::tdkserver::commands::create::CreateArguments;
use crate::client::tdkserver::commands::start::StartArguments;
use crate::client::tdkserver::commands::stop::StopArguments;
use crate::client::tdkserver::commands::unuse::UnuseArguments;
use crate::client::tdkserver::commands::r#use::UseArguments;
use crate::client::tdkserver::commands::using::UsingArguments;
use crate::provider::ClusterProvider;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "Tabsdata Client for Tabsdata Server on Kubernetes",
    version = "2.0.0",
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

#[derive(Debug, Clone, Args)]
pub struct ClusterArguments {
    #[arg(
        long,
        name = "provider",
        required = false,
        value_enum,
        long_help = "Provider of the cluster: \
                     'kubernetes' for a standard kubernetes cluster, \
                     'kminus' for a development kminus cluster. \
                     When unspecified, the value from the active context is taken; \
                     when no active context is set, this argument is required."
    )]
    pub provider: Option<ClusterProvider>,

    #[arg(
        long,
        name = "context",
        required = false,
        long_help = "Name of the kubeconfig context to use. \
                     Valid only for the 'kubernetes' provider. \
                     Mutually exclusive with --current. \
                     Composes with --kubeconfig."
    )]
    pub context: Option<String>,

    #[arg(
        long,
        name = "kubeconfig",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Path to a kubeconfig file. \
                     Valid only for the 'kubernetes' provider. \
                     When unspecified, the file pointed to by the KUBECONFIG environment variable, or ~/.kube/config, is used. \
                     Composes with --context and --current."
    )]
    pub kubeconfig: Option<PathBuf>,

    #[arg(
        long,
        name = "current",
        required = false,
        default_value_t = false,
        long_help = "Use the current-context entry of the kubeconfig file. \
                     Valid only for the 'kubernetes' provider. \
                     Mutually exclusive with --context. \
                     Composes with --kubeconfig."
    )]
    pub current: bool,

    #[arg(
        long,
        name = "namespace",
        required = false,
        long_help = "Namespace for the instance. \
                     Valid only for the 'kubernetes' provider. \
                     Overrides the namespace defined in the kubeconfig context. \
                     When neither is set, the kubernetes default namespace is used."
    )]
    pub namespace: Option<String>,
}

impl ClusterArguments {
    pub fn new(
        provider: Option<ClusterProvider>,
        context: Option<String>,
        kubeconfig: Option<PathBuf>,
        current: bool,
        namespace: Option<String>,
    ) -> Self {
        Self {
            provider,
            context,
            kubeconfig,
            current,
            namespace,
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct InstanceArguments {
    #[arg(
        long,
        name = "name",
        required = false,
        long_help = "Instance name. \
                     Valid only for the 'kminus' provider. \
                     The instance is created in ~/.tabsdata/instances/<name>. \
                     Mutually exclusive with --path."
    )]
    pub name: Option<String>,

    #[arg(
        long,
        name = "path",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Instance path. \
                     Valid only for the 'kminus' provider. \
                     Relative paths resolve under ~/.tabsdata/instances. \
                     Absolute paths are used as-is. \
                     Mutually exclusive with --name."
    )]
    pub path: Option<PathBuf>,
}

impl InstanceArguments {
    pub fn new(name: Option<String>, path: Option<PathBuf>) -> Self {
        Self { name, path }
    }
}
