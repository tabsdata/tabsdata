//
// Copyright 2026 Tabs Data Inc.
//

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command as SystemCommand, Stdio};
use std::time::{Duration, Instant};

use kube::Client;
use kube::config::{Config, KubeConfigOptions, Kubeconfig};
use td_common::status::ExitStatus::GeneralError;
use td_python::python::resolve_python_binary;
use tracing::info;

use crate::exit::{Outcome, abort};
use crate::fabric::Commit;
use crate::kminus::model::KminusInstance;
use crate::subprocess::{apply_detached_flags, apply_subprocess_flags, terminate_process};
use crate::{cerror, cwarn};

const KM_BINARY: &str = "km";
const KM_CREATE_SUBCOMMAND: &str = "create";
const KM_START_SUBCOMMAND: &str = "start";
const KM_WORKDIR_ARGUMENT: &str = "--workdir";

const KM_READINESS_TIMEOUT: Duration = Duration::from_secs(30);
const KM_READINESS_POLL: Duration = Duration::from_millis(250);

const KM_STOP_TIMEOUT: Duration = Duration::from_secs(10);
const KM_STOP_POLL: Duration = Duration::from_millis(250);

pub fn create_cluster(instance: &KminusInstance) -> Outcome {
    let km = match resolve_python_binary(KM_BINARY) {
        Ok(path) => path,
        Err(error) => {
            cerror!("Failed to locate the '{KM_BINARY}' binary: {error}");
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
    apply_detached_flags(&mut command);
    let status = match command.status() {
        Ok(status) => status,
        Err(error) => {
            cerror!(
                "Failed to run '{} {KM_CREATE_SUBCOMMAND}': {error}",
                km.display()
            );
            return abort(GeneralError);
        }
    };
    if !status.success() {
        cerror!(
            "'{} {KM_CREATE_SUBCOMMAND}' failed with status {status}.",
            km.display()
        );
        return abort(GeneralError);
    }
    Ok(())
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
}

impl Commit for InstanceFolderGuard {
    fn commit(mut self: Box<Self>) {
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

pub async fn start_cluster(instance: &KminusInstance) -> Outcome {
    let km = match resolve_python_binary(KM_BINARY) {
        Ok(path) => path,
        Err(error) => {
            cerror!("Failed to locate the '{KM_BINARY}' binary: {error}");
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
    apply_subprocess_flags(&mut command);
    if let Err(error) = command.spawn() {
        cerror!(
            "Failed to spawn '{} {KM_START_SUBCOMMAND}': {error}",
            km.display()
        );
        return abort(GeneralError);
    }

    let kubeconfig_path = instance.kubeconfig_path();
    let started = Instant::now();
    loop {
        if try_connect(&kubeconfig_path).await {
            return Ok(());
        }
        if started.elapsed() > KM_READINESS_TIMEOUT {
            cerror!(
                "The kminus cluster did not become ready within {} seconds.",
                KM_READINESS_TIMEOUT.as_secs()
            );
            return abort(GeneralError);
        }
        tokio::time::sleep(KM_READINESS_POLL).await;
    }
}

pub async fn stop_cluster(instance: &KminusInstance) -> Outcome {
    let pid = match read_km_pid(&instance.pids_path()) {
        Some(pid) => pid,
        None => {
            cwarn!("No running kminus cluster found for instance.");
            return Ok(());
        }
    };
    terminate_process(pid, KM_STOP_TIMEOUT);
    let kubeconfig_path = instance.kubeconfig_path();
    let started = Instant::now();
    while started.elapsed() < KM_STOP_TIMEOUT {
        if !try_connect(&kubeconfig_path).await {
            return Ok(());
        }
        tokio::time::sleep(KM_STOP_POLL).await;
    }
    info!("Kminus cluster has been signalled; apiserver still reachable after the timeout.");
    Ok(())
}

fn read_km_pid(pids_folder: &Path) -> Option<u32> {
    let entries = fs::read_dir(pids_folder).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some((pid_part, suffix)) = name.split_once('.')
            && suffix == KM_BINARY
            && let Ok(pid) = pid_part.parse::<u32>()
        {
            return Some(pid);
        }
    }
    None
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
