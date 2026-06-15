//
// Copyright 2026 Tabs Data Inc.
//

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command as SystemCommand, Stdio};
use std::time::Duration;

use kube::Client;
use kube::config::{Config, KubeConfigOptions, Kubeconfig};
use td_python::python::resolve_python_binary;

use crate::base::constants::{FOLDER_PROFILE, FOLDER_STAGING};
use crate::base::error::provider::ProviderError;
use crate::core::system::exit::CommandResult;
use crate::core::system::stream::Silencer;
use crate::core::system::subprocess::{
    self, apply_detached_flags, apply_subprocess_flags, is_alive as process_is_alive,
    terminate_process,
};
use crate::runtime::provider::{Cluster, Transaction};

const KM_BINARY: &str = "km";
const KM_CREATE_SUBCOMMAND: &str = "create";
const KM_START_SUBCOMMAND: &str = "start";
const KM_WORKDIR_ARGUMENT: &str = "--workdir";
const KM_IMAGESDIR_ARGUMENT: &str = "--imagesdir";

const KM_CONFIG_FOLDER: &str = "config";
const KM_DATA_FOLDER: &str = "data";
const KM_PIDS_FOLDER: &str = "pids";
const KM_SYSCONFIG_FILE: &str = "kmsysconfig.yaml";
const KM_KUBECONFIG_FILE: &str = "kubeconfig.yaml";

const KM_POLL_INTERVAL: Duration = Duration::from_millis(250);
const KM_POLL_ALIVE_TIMEOUT: Duration = Duration::from_secs(30);
const KM_POLL_DEAD_TIMEOUT: Duration = Duration::from_secs(10);
const KM_TERMINATE_GRACE: Duration = Duration::from_secs(10);

pub struct KminusCluster {
    path: PathBuf,
    images_path: PathBuf,
    verbose: bool,
}

impl KminusCluster {
    pub fn new(path: impl Into<PathBuf>, images_path: impl Into<PathBuf>, verbose: bool) -> Self {
        Self {
            path: path.into(),
            images_path: images_path.into(),
            verbose,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn images_path(&self) -> &Path {
        &self.images_path
    }

    pub fn sysconfig_path(&self) -> PathBuf {
        self.path.join(KM_CONFIG_FOLDER).join(KM_SYSCONFIG_FILE)
    }

    pub fn kubefile(&self) -> PathBuf {
        self.path.join(KM_CONFIG_FOLDER).join(KM_KUBECONFIG_FILE)
    }

    pub async fn responds(&self) -> bool {
        try_connect(&self.kubefile()).await
    }

    pub fn staging_path(&self) -> PathBuf {
        self.path.join(FOLDER_STAGING)
    }

    pub fn staging_profile_path(&self) -> PathBuf {
        self.staging_path().join(FOLDER_PROFILE)
    }

    pub fn pids_path(&self) -> PathBuf {
        self.path
            .join(KM_DATA_FOLDER)
            .join(KM_BINARY)
            .join(KM_PIDS_FOLDER)
    }

    fn spawn_km(&self) -> CommandResult {
        let km = resolve_python_binary(KM_BINARY).map_err(|error| {
            ProviderError::BinaryNotFound(KM_BINARY.to_owned(), error.to_string())
        })?;
        let mut command = SystemCommand::new(&km);
        command
            .arg(KM_START_SUBCOMMAND)
            .arg(KM_WORKDIR_ARGUMENT)
            .arg(self.path())
            .arg(KM_IMAGESDIR_ARGUMENT)
            .arg(self.images_path())
            .stdin(Stdio::null());
        if self.verbose {
            command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        } else {
            command.stdout(Stdio::null()).stderr(Stdio::null());
        }
        apply_subprocess_flags(&mut command);
        command.spawn().map_err(|error| {
            ProviderError::KminusStartFailed(format!(
                "spawn '{} {KM_START_SUBCOMMAND}': {error}",
                km.display()
            ))
        })?;
        Ok(())
    }

    async fn poll_ready(&self) -> bool {
        let kubefile = self.kubefile();
        tokio::time::timeout(KM_POLL_ALIVE_TIMEOUT, async {
            loop {
                let ready = {
                    let _silencer = Silencer::new().ok();
                    try_connect(&kubefile).await
                };
                if ready {
                    return;
                }
                tokio::time::sleep(KM_POLL_INTERVAL).await;
            }
        })
        .await
        .is_ok()
    }

    async fn poll_process_dead(&self, pid: u32) -> bool {
        tokio::time::timeout(KM_POLL_DEAD_TIMEOUT, async {
            while process_is_alive(pid) {
                tokio::time::sleep(KM_POLL_INTERVAL).await;
            }
        })
        .await
        .is_ok()
    }
}

impl Cluster for KminusCluster {
    fn create(&self) -> CommandResult {
        let km = resolve_python_binary(KM_BINARY).map_err(|error| {
            ProviderError::BinaryNotFound(KM_BINARY.to_owned(), error.to_string())
        })?;
        let mut command = SystemCommand::new(&km);
        command
            .arg(KM_CREATE_SUBCOMMAND)
            .arg(KM_WORKDIR_ARGUMENT)
            .arg(self.path())
            .stdin(Stdio::null());
        apply_detached_flags(&mut command);
        let output = subprocess::run(&mut command, KM_BINARY, self.verbose).map_err(|error| {
            ProviderError::KminusStartFailed(format!(
                "run '{} {KM_CREATE_SUBCOMMAND}': {error}",
                km.display()
            ))
        })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            return Err(ProviderError::KminusStartFailed(format!(
                "'{} {KM_CREATE_SUBCOMMAND}' failed with status {}: {stderr}",
                km.display(),
                output.status,
            ))
            .into());
        }
        Ok(())
    }

    async fn start(&self) -> CommandResult {
        if self.is_alive().await {
            return Ok(());
        }
        self.spawn_km()?;
        if !self.poll_alive().await {
            return Err(ProviderError::KminusStartFailed(format!(
                "process did not start within {} seconds",
                KM_POLL_ALIVE_TIMEOUT.as_secs()
            ))
            .into());
        }
        if !self.poll_ready().await {
            return Err(ProviderError::KminusStartFailed(format!(
                "kubernetes apiserver did not become ready within {} seconds",
                KM_POLL_ALIVE_TIMEOUT.as_secs()
            ))
            .into());
        }
        Ok(())
    }

    async fn stop(&self) -> CommandResult<bool> {
        let Some(pid) = read_km_pid(&self.pids_path()) else {
            return Ok(false);
        };
        if !process_is_alive(pid) {
            return Ok(false);
        }
        terminate_process(pid, KM_TERMINATE_GRACE);
        if !self.poll_process_dead(pid).await {
            return Err(ProviderError::KminusStopFailed(format!(
                "process still alive after {} seconds",
                KM_POLL_DEAD_TIMEOUT.as_secs()
            ))
            .into());
        }
        Ok(true)
    }

    async fn is_alive(&self) -> bool {
        let Some(pid) = read_km_pid(&self.pids_path()) else {
            return false;
        };
        process_is_alive(pid) && try_connect(&self.kubefile()).await
    }

    async fn poll_alive(&self) -> bool {
        tokio::time::timeout(KM_POLL_ALIVE_TIMEOUT, async {
            while !self.is_alive().await {
                tokio::time::sleep(KM_POLL_INTERVAL).await;
            }
        })
        .await
        .is_ok()
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
}

impl Transaction for InstanceFolderGuard {
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

async fn try_connect(kubefile: &Path) -> bool {
    if !kubefile.exists() {
        return false;
    }
    let Ok(kubeconfig) = Kubeconfig::read_from(kubefile) else {
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
