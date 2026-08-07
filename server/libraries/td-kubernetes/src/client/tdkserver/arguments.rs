//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;
use std::sync::LazyLock;

use clap::Parser;
use clap_derive::{Args, Subcommand};

// use crate::client::tdkserver::commands::cluster::ClusterCommands;
// use crate::client::tdkserver::commands::config::ConfigCommands;
// use crate::client::tdkserver::commands::instance::InstanceCommands;
// use crate::client::tdkserver::commands::secret::SecretCommands;
// use crate::client::tdkserver::commands::server::ServerCommands;
use crate::client::tdkserver::commands::service::InstanceNameArguments;
use crate::client::tdkserver::commands::service::create::CreateArguments;
use crate::client::tdkserver::commands::service::get::GetArguments;
use crate::client::tdkserver::commands::service::init::InitArguments;
use crate::client::tdkserver::commands::service::put::PutArguments;
use crate::client::tdkserver::commands::service::quickstart::QuickstartArguments;
// use crate::client::tdkserver::commands::service::register::RegisterArguments;
use crate::client::tdkserver::commands::service::restart::RestartArguments;
use crate::client::tdkserver::commands::service::start::StartArguments;
use crate::client::tdkserver::commands::service::stop::StopArguments;
use crate::client::tdkserver::commands::service::trash::TrashArguments;
use crate::client::tdkserver::commands::service::view::ViewArguments;
use crate::client::tdkserver::commands::session::set::SetArguments;
use crate::client::tdkserver::commands::session::settings::SettingsArguments;
use crate::client::tdkserver::commands::session::unset::UnpinArguments;
// use crate::client::tdkserver::commands::session::SessionCommands;
// use crate::client::tdkserver::commands::template::TemplateCommands;
use crate::runtime::provider::ClusterProvider;

static PROVIDER_LONG_HELP: LazyLock<String> = LazyLock::new(|| {
    format!(
        "Provider of the cluster: {}. \
         When unspecified, the value from the active tabsdata session is taken; \
         when no active tabsdata session is set, this argument is required.",
        ClusterProvider::supported_descriptions()
    )
});

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
        display_order = 140,
        long_help = "Accept or decline the license terms without interactive prompt [y/yes/n/no]."
    )]
    pub accept_terms: Option<String>,

    #[arg(
        long,
        name = "verbose",
        required = false,
        global = true,
        default_value_t = false,
        display_order = 130,
        long_help = "Show the output of each step that this command runs."
    )]
    pub verbose: bool,

    #[arg(
        long,
        name = "keep-progress",
        required = false,
        global = true,
        default_value_t = true,
        action = clap::ArgAction::Set,
        display_order = 135,
        long_help = "Keep the progress line of each step that this command runs, instead of \
                     clearing it when the step finishes [true/false]."
    )]
    pub keep_progress: bool,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Commands {
    #[command(about = "Initialize, create and start a tabsdata instance in one step")]
    Quickstart(QuickstartArguments),
    #[command(about = "Produce an editable input config folder for a tabsdata instance")]
    Init(InitArguments),
    #[command(about = "Create a tabsdata instance")]
    Create(CreateArguments),
    #[command(about = "Start a tabsdata instance")]
    Start(StartArguments),
    #[command(about = "Stop a tabsdata instance")]
    Stop(StopArguments),
    #[command(about = "Restart a tabsdata instance")]
    Restart(RestartArguments),
    #[command(about = "Show the running workloads of a tabsdata instance")]
    Status(InstanceNameArguments),
    #[command(about = "Show the configuration and state of a tabsdata instance")]
    Info(InstanceNameArguments),
    #[command(about = "Trash a local kminus tabsdata instance")]
    Trash(TrashArguments),
    // #[command(about = "Register an existing tabsdata instance")]
    // Register(RegisterArguments),
    // #[command(about = "Unregister a tabsdata instance")]
    // Unregister(InstanceNameArguments),
    #[command(
        name = "get-cfg",
        about = "Download the configmaps of a tabsdata instance for editing"
    )]
    Get(GetArguments),
    #[command(
        name = "put-cfg",
        about = "Replace the configmaps of a tabsdata instance from edited files"
    )]
    Put(PutArguments),
    #[command(
        name = "view-cfg",
        about = "View the configmaps of a tabsdata instance"
    )]
    View(ViewArguments),
    #[command(about = "Set a session setting used when commands omit it")]
    Set(SetArguments),
    #[command(about = "Unset a session setting")]
    Unset(UnpinArguments),
    #[command(about = "Show the tabsdata session settings")]
    Settings(SettingsArguments),
    #[command(about = "Open an interactive shell")]
    Shell,
    #[command(about = "List registered tabsdata instances")]
    List,
    // #[command(subcommand, about = "Manage tabsdata templates")]
    // Template(TemplateCommands),
    // #[command(subcommand, about = "Manage tabsdata clusters")]
    // Cluster(ClusterCommands),
    // #[command(subcommand, about = "Manage tabsdata instances")]
    // Instance(InstanceCommands),
    // #[command(subcommand, about = "Manage tabsdata servers")]
    // Server(ServerCommands),
    // #[command(subcommand, about = "Manage tabsdata configs")]
    // Config(ConfigCommands),
    // #[command(subcommand, about = "Manage tabsdata secrets")]
    // Secret(SecretCommands),
    // #[command(subcommand, about = "Manage tabsdata sessions")]
    // Session(SessionCommands),
}

#[derive(Debug, Clone, Args)]
pub struct ClusterArguments {
    #[arg(
        long,
        name = "provider",
        required = false,
        value_enum,
        display_order = 20,
        long_help = PROVIDER_LONG_HELP.as_str()
    )]
    pub provider: Option<ClusterProvider>,

    #[arg(
        long,
        name = "context",
        required = false,
        display_order = 40,
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
        display_order = 30,
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
        display_order = 60,
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
        display_order = 50,
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
        long_help = "Instance name. The instance lives in ~/.tabsdata/instances/<name>. \
                     For the 'kminus' provider it is initialized, created and registered \
                     there; for every other provider it is initialized and registered there, \
                     while its cluster is managed externally."
    )]
    pub name: Option<String>,
}

impl InstanceArguments {
    pub fn new(name: Option<String>) -> Self {
        Self { name }
    }
}
