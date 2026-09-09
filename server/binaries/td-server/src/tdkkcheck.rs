//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use serde::Serialize;

use td_kubernetes::client::tdkserver::arguments::ClusterArguments;
use td_kubernetes::client::tdkserver::commands::service::assess::{host, local};
use td_kubernetes::client::tdkserver::render::report::helper::assess::AssessCheck;
use td_kubernetes::runtime::provider::ClusterProvider;

#[derive(Debug, Parser)]
#[command(
    name = "tdk_kcheck",
    about = "Run one assess check and write its outcome as JSON",
    disable_help_subcommand = true
)]
struct Arguments {
    #[arg(value_enum)]
    check: Checked,

    #[arg(long, required = true)]
    instance: String,

    #[arg(long, value_enum)]
    provider: Option<ClusterProvider>,

    #[arg(long)]
    context: Option<String>,

    #[arg(long)]
    kubeconfig: Option<PathBuf>,

    #[arg(long)]
    current: bool,

    #[arg(long)]
    namespace: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Checked {
    InstanceState,
    DefinitionMatch,
    MandatoryValues,
    InstanceVersion,
    InstanceEdition,
    InitFolder,
    HostPorts,
    OpenFiles,
    RuntimesLock,
}

#[derive(Serialize)]
struct Failure {
    error: String,
}

fn cluster_of(arguments: &Arguments) -> ClusterArguments {
    ClusterArguments {
        provider: arguments.provider,
        context: arguments.context.clone(),
        kubeconfig: arguments.kubeconfig.clone(),
        current: arguments.current,
        namespace: arguments.namespace.clone(),
    }
}

fn run(arguments: &Arguments) -> Result<AssessCheck, String> {
    let cluster = cluster_of(arguments);
    let name = &arguments.instance;

    let (state, persisted) =
        local::assess_instance_state(name).map_err(|error| format!("{error:?}"))?;
    let settings = persisted.as_ref();

    match arguments.check {
        Checked::InstanceState => Ok(state),
        Checked::DefinitionMatch => Ok(local::assess_definition_match(name, &cluster, settings)),
        Checked::MandatoryValues => {
            local::assess_pending_values(name, &cluster, settings).map_err(|e| format!("{e:?}"))
        }
        Checked::InstanceVersion => {
            local::assess_instance_version(name, settings).map_err(|e| format!("{e:?}"))
        }
        Checked::InstanceEdition => {
            local::assess_instance_edition(name, settings).map_err(|e| format!("{e:?}"))
        }
        Checked::InitFolder => {
            local::assess_init_folder(name, &cluster, settings).map_err(|e| format!("{e:?}"))
        }
        Checked::HostPorts => {
            let provider = arguments
                .provider
                .or_else(|| settings.map(|settings| settings.provider))
                .unwrap_or(ClusterProvider::Kminus);
            host::assess_host_ports(provider, settings).map_err(|e| format!("{e:?}"))
        }
        Checked::OpenFiles => {
            let provider = arguments
                .provider
                .or_else(|| settings.map(|settings| settings.provider))
                .unwrap_or(ClusterProvider::Kminus);
            host::assess_open_files(provider).map_err(|e| format!("{e:?}"))
        }
        Checked::RuntimesLock => {
            let provider = arguments
                .provider
                .or_else(|| settings.map(|settings| settings.provider))
                .unwrap_or(ClusterProvider::Kminus);
            host::assess_runtimes_lock(provider, settings).map_err(|e| format!("{e:?}"))
        }
    }
}

fn main() -> ExitCode {
    let arguments = Arguments::parse();
    match run(&arguments) {
        Ok(check) => match serde_json::to_string(&check) {
            Ok(payload) => {
                println!("{payload}");
                ExitCode::SUCCESS
            }
            Err(error) => fail(&error.to_string()),
        },
        Err(error) => fail(&error),
    }
}

fn fail(error: &str) -> ExitCode {
    let failure = Failure {
        error: error.to_owned(),
    };
    match serde_json::to_string(&failure) {
        Ok(payload) => eprintln!("{payload}"),
        Err(_) => eprintln!("{{\"error\":\"The failure could not be serialized\"}}"),
    }
    ExitCode::FAILURE
}
