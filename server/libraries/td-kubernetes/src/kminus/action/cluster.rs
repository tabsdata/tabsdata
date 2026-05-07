//
// Copyright 2026 Tabs Data Inc.
//

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as SystemCommand, Stdio};
use std::time::{Duration, Instant};

use kube::Client;
use kube::config::{Config, KubeConfigOptions, Kubeconfig};
use sysinfo::Signal;
use td_common::os::terminate_process;
use td_common::status::ExitStatus::GeneralError;
use td_python::python::resolve_python_binary;
use tracing::error;

use crate::exit::{Outcome, abort};
use crate::kminus::model::KminusInstance;

const KM_BINARY: &str = "km";
const KM_CREATE_SUBCOMMAND: &str = "create";
const KM_START_SUBCOMMAND: &str = "start";
const KM_WORKDIR_ARGUMENT: &str = "--workdir";

const KM_READINESS_TIMEOUT: Duration = Duration::from_secs(30);
const KM_READINESS_POLL: Duration = Duration::from_millis(250);

pub fn create_cluster(instance: &KminusInstance) -> Outcome {
    let km = match resolve_python_binary(KM_BINARY) {
        Ok(path) => path,
        Err(error) => {
            error!("Failed to locate the '{KM_BINARY}' binary: {error}");
            return abort(GeneralError);
        }
    };
    let mut command = SystemCommand::new(&km);
    command
        .arg(KM_CREATE_SUBCOMMAND)
        .arg(KM_WORKDIR_ARGUMENT)
        .arg(instance.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let status = match command.status() {
        Ok(status) => status,
        Err(error) => {
            error!(
                "Failed to run '{} {KM_CREATE_SUBCOMMAND}': {error}",
                km.display()
            );
            return abort(GeneralError);
        }
    };
    if !status.success() {
        error!(
            "'{} {KM_CREATE_SUBCOMMAND}' failed with status {status}.",
            km.display()
        );
        return abort(GeneralError);
    }
    Ok(())
}

pub struct KminusRunningGuard {
    child: Option<Child>,
}

impl Drop for KminusRunningGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            terminate_process(child.id() as i32, Signal::Term).ok();
            child.wait().ok();
        }
    }
}

pub struct InstanceFolderGuard {
    folder: Option<PathBuf>,
}

impl InstanceFolderGuard {
    pub fn new(folder: PathBuf) -> Self {
        Self {
            folder: Some(folder),
        }
    }

    pub fn commit(mut self) {
        self.folder = None;
    }
}

impl Drop for InstanceFolderGuard {
    fn drop(&mut self) {
        if let Some(folder) = self.folder.take() {
            fs::remove_dir_all(&folder).ok();
        }
    }
}

pub async fn start_cluster(instance: &KminusInstance) -> Outcome<KminusRunningGuard> {
    let km = match resolve_python_binary(KM_BINARY) {
        Ok(path) => path,
        Err(error) => {
            error!("Failed to locate the '{KM_BINARY}' binary: {error}");
            return abort(GeneralError);
        }
    };
    let mut command = SystemCommand::new(&km);
    command
        .arg(KM_START_SUBCOMMAND)
        .arg(KM_WORKDIR_ARGUMENT)
        .arg(instance.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            error!(
                "Failed to spawn '{} {KM_START_SUBCOMMAND}': {error}",
                km.display()
            );
            return abort(GeneralError);
        }
    };
    let guard = KminusRunningGuard { child: Some(child) };

    let kubeconfig_path = instance.kubeconfig_path();
    let started = Instant::now();
    loop {
        if try_connect(&kubeconfig_path).await {
            return Ok(guard);
        }
        if started.elapsed() > KM_READINESS_TIMEOUT {
            error!(
                "The kminus cluster did not become ready within {} seconds.",
                KM_READINESS_TIMEOUT.as_secs()
            );
            drop(guard);
            return abort(GeneralError);
        }
        tokio::time::sleep(KM_READINESS_POLL).await;
    }
}

async fn try_connect(kubeconfig_path: &Path) -> bool {
    if !kubeconfig_path.exists() {
        return false;
    }
    let Ok(kubeconfig) = Kubeconfig::read_from(kubeconfig_path) else {
        return false;
    };
    let Ok(config) =
        Config::from_custom_kubeconfig(kubeconfig, &KubeConfigOptions::default()).await
    else {
        return false;
    };
    let Ok(client) = Client::try_from(config) else {
        return false;
    };
    client.apiserver_version().await.is_ok()
}
