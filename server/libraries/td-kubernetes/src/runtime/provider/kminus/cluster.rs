//
// Copyright 2026 Tabs Data Inc.
//

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command as SystemCommand, Stdio};
use std::time::Duration;

use kube::Client;
use kube::config::{Config, KubeConfigOptions, Kubeconfig};
use td_common::status::ExitStatus::GeneralError;
use td_python::python::resolve_python_binary;

use crate::base::constants::{FOLDER_PROFILE, FOLDER_STAGING};
use crate::cerror;
use crate::core::system::exit::{Result, abort};
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
    verbose: bool,
}

impl KminusCluster {
    pub fn new(path: impl Into<PathBuf>, verbose: bool) -> Self {
        Self {
            path: path.into(),
            verbose,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sysconfig_path(&self) -> PathBuf {
        self.path.join(KM_CONFIG_FOLDER).join(KM_SYSCONFIG_FILE)
    }

    pub fn kubefile(&self) -> PathBuf {
        self.path.join(KM_CONFIG_FOLDER).join(KM_KUBECONFIG_FILE)
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

    fn spawn_km(&self) -> Result {
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
            .arg(self.path())
            .stdin(Stdio::null());
        if self.verbose {
            command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        } else {
            command.stdout(Stdio::null()).stderr(Stdio::null());
        }
        apply_subprocess_flags(&mut command);
        if let Err(error) = command.spawn() {
            cerror!(
                "Failed to spawn '{} {KM_START_SUBCOMMAND}': {error}",
                km.display()
            );
            return abort(GeneralError);
        }
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
}

impl Cluster for KminusCluster {
    fn create(&self) -> Result {
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
            .arg(self.path())
            .stdin(Stdio::null());
        apply_detached_flags(&mut command);
        let output = match subprocess::run(&mut command, KM_BINARY, self.verbose) {
            Ok(output) => output,
            Err(error) => {
                cerror!(
                    "Failed to run '{} {KM_CREATE_SUBCOMMAND}': {error}",
                    km.display()
                );
                return abort(GeneralError);
            }
        };
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            cerror!(
                "'{} {KM_CREATE_SUBCOMMAND}' failed with status {}: {}",
                km.display(),
                output.status,
                stderr.trim()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    async fn start(&self) -> Result {
        if self.is_alive().await {
            return Ok(());
        }
        self.spawn_km()?;
        if !self.poll_alive().await {
            cerror!(
                "The kminus cluster process did not start within {} seconds.",
                KM_POLL_ALIVE_TIMEOUT.as_secs()
            );
            return abort(GeneralError);
        }
        if !self.poll_ready().await {
            cerror!(
                "The kminus cluster kubernetes apiserver did not become ready within {} seconds.",
                KM_POLL_ALIVE_TIMEOUT.as_secs()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    async fn stop(&self) -> Result<bool> {
        if self.is_dead().await {
            return Ok(false);
        }
        let Some(pid) = read_km_pid(&self.pids_path()) else {
            return Ok(false);
        };
        terminate_process(pid, KM_TERMINATE_GRACE);
        if !self.poll_dead().await {
            cerror!(
                "The kminus cluster process is still alive after {} seconds.",
                KM_POLL_DEAD_TIMEOUT.as_secs()
            );
            return abort(GeneralError);
        }
        Ok(true)
    }

    async fn is_alive(&self) -> bool {
        let Some(pid) = read_km_pid(&self.pids_path()) else {
            return false;
        };
        process_is_alive(pid)
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

    async fn poll_dead(&self) -> bool {
        tokio::time::timeout(KM_POLL_DEAD_TIMEOUT, async {
            while !self.is_dead().await {
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
