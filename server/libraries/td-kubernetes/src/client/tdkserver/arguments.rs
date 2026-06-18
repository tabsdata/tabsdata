//
// Copyright 2026 Tabs Data Inc.
//

use std::path::PathBuf;

use clap::Parser;
use clap_derive::{Args, Subcommand};

use crate::client::tdkserver::commands::autoconfirm::AutoconfirmCommands;
use crate::client::tdkserver::commands::cluster::ClusterCommands;
use crate::client::tdkserver::commands::config::ConfigCommands;
use crate::client::tdkserver::commands::instance::InstanceCommands;
use crate::client::tdkserver::commands::secret::SecretCommands;
use crate::client::tdkserver::commands::server::ServerCommands;
use crate::client::tdkserver::commands::service::InstanceNameArguments;
use crate::client::tdkserver::commands::service::create::CreateArguments;
use crate::client::tdkserver::commands::service::init::InitArguments;
use crate::client::tdkserver::commands::service::quickstart::QuickstartArguments;
use crate::client::tdkserver::commands::service::register::RegisterArguments;
use crate::client::tdkserver::commands::service::restart::RestartArguments;
use crate::client::tdkserver::commands::service::start::StartArguments;
use crate::client::tdkserver::commands::service::stop::StopArguments;
use crate::client::tdkserver::commands::session::SessionCommands;
use crate::client::tdkserver::commands::template::TemplateCommands;
use crate::runtime::provider::ClusterProvider;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "Tabsdata Client for Tabsdata Server on Kubernetes",
    version = "2.0.0",
    about = "Client for the tabsdata server on kubernetes",
    long_about = "Manage tabsdata instances on kubernetes clusters."
)]
pub struct Arguments {
    #[command(flatten)]
    pub globals: GlobalArguments,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Debug, Clone, Args)]
pub struct GlobalArguments {
    #[arg(
        long,
        name = "accept-terms",
        required = false,
        global = true,
        long_help = "Accept or decline the license terms without interactive prompt [y/yes/n/no]."
    )]
    pub accept_terms: Option<String>,

    #[arg(
        long,
        name = "verbose",
        required = false,
        global = true,
        default_value_t = false,
        long_help = "Show the output of each step that this command runs."
    )]
    pub verbose: bool,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Commands {
    #[command(about = "Open an interactive shell")]
    Shell,
    #[command(subcommand, about = "Manage tabsdata clusters")]
    Cluster(ClusterCommands),
    #[command(subcommand, about = "Manage tabsdata instances")]
    Instance(InstanceCommands),
    #[command(subcommand, about = "Manage tabsdata servers")]
    Server(ServerCommands),
    #[command(about = "Produce an editable input config folder for a tabsdata instance")]
    Init(InitArguments),
    #[command(about = "Create a tabsdata instance")]
    Create(CreateArguments),
    #[command(about = "Register an existing tabsdata instance")]
    Register(RegisterArguments),
    #[command(about = "Unregister a tabsdata instance")]
    Unregister(InstanceNameArguments),
    #[command(about = "List registered tabsdata instances")]
    List,
    #[command(about = "Pin a tabsdata instance as the default")]
    Pin(InstanceNameArguments),
    #[command(about = "Unpin the default tabsdata instance")]
    Unpin,
    #[command(about = "Start a tabsdata instance")]
    Start(StartArguments),
    #[command(about = "Stop a tabsdata instance")]
    Stop(StopArguments),
    #[command(about = "Restart a tabsdata instance")]
    Restart(RestartArguments),
    #[command(about = "Show the status of a tabsdata instance")]
    Status(InstanceNameArguments),
    #[command(about = "Initialize, create and start a tabsdata instance in one step")]
    Quickstart(QuickstartArguments),
    #[command(subcommand, about = "Manage tabsdata templates")]
    Template(TemplateCommands),
    #[command(subcommand, about = "Manage tabsdata configs")]
    Config(ConfigCommands),
    #[command(subcommand, about = "Manage tabsdata secrets")]
    Secret(SecretCommands),
    #[command(subcommand, about = "Manage tabsdata sessions")]
    Session(SessionCommands),
    #[command(subcommand, about = "Manage the autoconfirm preference")]
    Autoconfirm(AutoconfirmCommands),
}

#[derive(Debug, Clone, Args)]
pub struct ClusterArguments {
    #[arg(
        long,
        name = "provider",
        required = false,
        value_enum,
        long_help = "Provider of the cluster: \
                     'kminus' for a development kminus cluster, \
                     'docker' for the Kubernetes cluster bundled with Docker Desktop on macOS/Windows, \
                     'eks' for AWS EKS, \
                     'fargate' for AWS EKS-on-Fargate. \
                     When unspecified, the value from the active tabsdata session is taken; \
                     when no active tabsdata session is set, this argument is required."
    )]
    pub provider: Option<ClusterProvider>,

    #[arg(
        long,
        name = "context",
        required = false,
        long_help = "Name of the kubeconfig context to use. \
                     Valid only for the 'docker' provider. \
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
                     Valid only for the 'docker' provider. \
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
                     Valid only for the 'docker' provider. \
                     Mutually exclusive with --context. \
                     Composes with --kubeconfig."
    )]
    pub current: bool,

    #[arg(
        long,
        name = "namespace",
        required = false,
        long_help = "Namespace for the instance. \
                     Valid only for the 'docker' provider. \
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
