//
// Copyright 2024 Tabs Data Inc.
//

use async_trait::async_trait;
use std::error::Error;
use std::sync::Arc;
use std::{env, process};
use td_bridge::Orchestrator;
use td_bridge::{BridgeProvider, BridgeProviderFactory, SupervisorBridgeProvider};
use td_common::attach::attach;
use td_common::{about, logging};
use td_kubernetes::kube::KubernetesBridgeProvider;
use td_process::launcher::hooks;
use tracing::info;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LOG_CONFIG_LONG: &str = "--log-config";
const LOG_CONFIG_SHORT: &str = "-c";

#[attach(signal = "td_apiserver")]
fn main() {
    hooks::panic();

    if env::args().any(|arg| arg == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    match log_config_arg() {
        Some(path) => logging::start_with_config_file(&path),
        None => logging::start(),
    }

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running apiserver with command: \n{}", command);

    td_apiserver::run(Arc::new(ServerBridgeFactory));
}

/// Builds the bridge for whichever orchestrator the apiserver config names —
/// the binary is the one place that sees every backend crate.
struct ServerBridgeFactory;

#[async_trait]
impl BridgeProviderFactory for ServerBridgeFactory {
    async fn build(
        &self,
        orchestrator: Orchestrator,
    ) -> Result<Arc<dyn BridgeProvider>, Box<dyn Error + Send + Sync>> {
        match orchestrator {
            Orchestrator::Supervisor => Ok(Arc::new(SupervisorBridgeProvider::new().await?)),
            Orchestrator::Kubernetes => Ok(Arc::new(
                KubernetesBridgeProvider::from_pod_environment().await?,
            )),
        }
    }
}

fn log_config_arg() -> Option<String> {
    let args: Vec<String> = env::args().collect();
    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        if arg == LOG_CONFIG_LONG || arg == LOG_CONFIG_SHORT {
            return iter.next().cloned();
        }
        if let Some(value) = arg.strip_prefix(&format!("{LOG_CONFIG_LONG}=")) {
            return Some(value.to_owned());
        }
        if let Some(value) = arg.strip_prefix(&format!("{LOG_CONFIG_SHORT}=")) {
            return Some(value.to_owned());
        }
    }
    None
}
