//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command as SystemCommand, Stdio};
use std::time::Duration;

use tokio::time::Instant as TokioInstant;

use chrono::Utc;
use kube::Client;
use kube::config::{Config, KubeConfigOptions, Kubeconfig};
use td_python::python::resolve_python_binary;
use tracing::debug;

use crate::base::constants::{
    DEFAULT_LOG_LEVEL, FOLDER_PROFILE, FOLDER_STAGING, KM_IO_LOG_PREFIX, KM_IO_LOG_STAMP,
    LOG_COUNT_WARNING, LOG_FOLDER, LogLevel,
};
use crate::base::error::provider::ProviderError;
use crate::client::tdkserver::widgets::show_caution;
use crate::core::system::capture;
use crate::core::system::exit::CommandResult;
use crate::core::system::stream::Silencer;
use crate::core::system::subprocess::{
    self, apply_detached_flags, apply_subprocess_flags, force_terminate_process_tree,
    is_alive as process_is_alive, seal_own_stdio, terminate_single_process,
};
use crate::cwarn;
use crate::runtime::provider::{Cluster, Transaction};

const KM_BINARY: &str = "km";
const KM_CREATE_SUBCOMMAND: &str = "create";
const KM_START_SUBCOMMAND: &str = "start";
const KM_STATUS_SUBCOMMAND: &str = "status";
const KM_STOP_SUBCOMMAND: &str = "stop";
const KM_WORKDIR_ARGUMENT: &str = "--workdir";
const KM_IMAGESDIR_ARGUMENT: &str = "--imagesdir";
const KM_PORTSTART_ARGUMENT: &str = "--portstart";
const KM_LOGLEVEL_ARGUMENT: &str = "--loglevel";
const KM_DRYRUN_ARGUMENT: &str = "--dryrun";
const KM_PURGE_ARGUMENT: &str = "--purge";

const KM_PROBE_WORKDIR: &str = "tdk-port-probe";

const KM_PORT_BUSY_MARKER: &str = "IN USE";

pub const PORT_BOUND_REASON: &str = "already bound";

const KM_STATUS_STARTED: i32 = 0;
const KM_STATUS_STARTING: i32 = 1;
const KM_STATUS_DEAD: i32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterState {
    Started,
    Starting,
    Dead,
    Unknown,
}

impl ClusterState {
    fn from_exit_code(code: Option<i32>) -> Self {
        match code {
            Some(KM_STATUS_STARTED) => Self::Started,
            Some(KM_STATUS_STARTING) => Self::Starting,
            Some(KM_STATUS_DEAD) => Self::Dead,
            _ => Self::Unknown,
        }
    }
}

const KM_CONFIG_FOLDER: &str = "config";
const KM_DATA_FOLDER: &str = "data";
const KM_PIDS_FOLDER: &str = "pids";
const KM_SYSCONFIG_FILE: &str = "kmsysconfig.yaml";
const KM_KUBECONFIG_FILE: &str = "kubeconfig.yaml";

const KM_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[cfg(not(windows))]
const KM_POLL_ALIVE_TIMEOUT: Duration = Duration::from_secs(300);
#[cfg(windows)]
const KM_POLL_ALIVE_TIMEOUT: Duration = Duration::from_secs(300);

#[cfg(not(windows))]
const KM_POLL_DEAD_TIMEOUT: Duration = Duration::from_secs(300);
#[cfg(windows)]
const KM_POLL_DEAD_TIMEOUT: Duration = Duration::from_secs(600);

#[cfg(not(windows))]
const KM_STOP_TIMEOUT: Duration = Duration::from_secs(120);
#[cfg(windows)]
const KM_STOP_TIMEOUT: Duration = Duration::from_secs(240);

#[cfg(not(windows))]
const KM_GRACEFUL_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(windows)]
const KM_GRACEFUL_TIMEOUT: Duration = Duration::from_secs(120);

#[cfg(not(windows))]
const KM_FORCE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(windows)]
const KM_FORCE_TIMEOUT: Duration = Duration::from_secs(60);

pub struct BoundPort {
    pub port: u16,
    pub label: String,
    pub reason: String,
}

pub fn probe_cluster_ports(portstart: u16) -> CommandResult<Vec<BoundPort>> {
    let km = resolve_python_binary(KM_BINARY)
        .map_err(|error| ProviderError::BinaryNotFound(KM_BINARY.to_owned(), error.to_string()))?;
    let probe = std::env::temp_dir().join(KM_PROBE_WORKDIR);
    let mut command = SystemCommand::new(&km);
    command
        .arg(KM_CREATE_SUBCOMMAND)
        .arg(KM_WORKDIR_ARGUMENT)
        .arg(&probe)
        .arg(KM_PORTSTART_ARGUMENT)
        .arg(portstart.to_string())
        .arg(KM_DRYRUN_ARGUMENT)
        .stdin(Stdio::null());
    apply_detached_flags(&mut command);
    let output = subprocess::run(&mut command, KM_BINARY, false).map_err(|error| {
        ProviderError::KminusStartFailed(format!(
            "run '{} {KM_CREATE_SUBCOMMAND} {KM_DRYRUN_ARGUMENT}': {error}",
            km.display()
        ))
    })?;
    if output.status.success() {
        return Ok(Vec::new());
    }
    let report = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(parse_bound_ports(&report))
}

fn reported(output: &std::process::Output) -> String {
    let report = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report = report.trim();
    if report.is_empty() {
        return String::new();
    }
    format!("\n{report}")
}

fn parse_bound_ports(report: &str) -> Vec<BoundPort> {
    report
        .lines()
        .filter_map(|line| parse_bound_port(line.trim()))
        .collect()
}

fn parse_bound_port(line: &str) -> Option<BoundPort> {
    let (head, _) = line.split_once(KM_PORT_BUSY_MARKER)?;
    let head = head.trim_end();
    let (label, port) = head.rsplit_once(char::is_whitespace)?;
    let port = port.parse().ok()?;
    let label = label.trim().trim_start_matches("ports").trim().to_owned();
    Some(BoundPort {
        port,
        label,
        reason: PORT_BOUND_REASON.to_owned(),
    })
}

pub struct KminusCluster {
    path: PathBuf,
    images_path: PathBuf,
    verbose: bool,
    portstart: Option<u16>,
    loglevel: Option<LogLevel>,
}

impl KminusCluster {
    pub fn new(
        path: impl Into<PathBuf>,
        images_path: impl Into<PathBuf>,
        verbose: bool,
        portstart: Option<u16>,
        loglevel: Option<LogLevel>,
    ) -> Self {
        Self {
            path: path.into(),
            images_path: images_path.into(),
            verbose,
            portstart,
            loglevel,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn images_path(&self) -> &Path {
        &self.images_path
    }

    pub fn loglevel(&self) -> LogLevel {
        self.loglevel.unwrap_or(DEFAULT_LOG_LEVEL)
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

    fn warn_on_log_count(&self) {
        let Some(root) = self.path.parent() else {
            return;
        };
        let held = capture::log_file_count(root);
        if held <= LOG_COUNT_WARNING {
            return;
        }
        show_caution(
            &format!(
                "The instance log folder holds {held} files, above the \
                 notice threshold of {LOG_COUNT_WARNING}.\n\n{}\n\nYou may \
                 want to delete the ones you no longer need.",
                root.join(LOG_FOLDER).display()
            ),
            80,
            true,
            &[],
        );
    }

    fn stdio_log_path(&self) -> Option<PathBuf> {
        let root = self.path.parent()?;
        let stamp = Utc::now().format(KM_IO_LOG_STAMP).to_string();
        capture::stdio_log(root, KM_IO_LOG_PREFIX, &stamp)
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
            .arg(KM_PURGE_ARGUMENT)
            .stdin(Stdio::null());
        if let Some(loglevel) = self.loglevel {
            command.arg(KM_LOGLEVEL_ARGUMENT).arg(loglevel.as_str());
        }
        let stdio = self.stdio_log_path();
        match stdio.as_deref().and_then(capture::writer_for) {
            Some(file) => {
                let duplicate = file.try_clone().ok();
                command.stdout(Stdio::from(file));
                match duplicate {
                    Some(errors) => command.stderr(Stdio::from(errors)),
                    None => command.stderr(Stdio::null()),
                };
            }
            None => {
                command.stdout(Stdio::null()).stderr(Stdio::null());
            }
        }
        apply_subprocess_flags(&mut command);
        seal_own_stdio();
        let mut child = command.spawn().map_err(|error| {
            ProviderError::KminusStartFailed(format!(
                "spawn '{} {KM_START_SUBCOMMAND}': {error}",
                km.display()
            ))
        })?;
        if let Some(path) = stdio {
            debug!("Kminus stdio is captured in '{}'.", path.display());
            capture::relay(&path, KM_BINARY, &mut child, self.verbose);
        }
        self.warn_on_log_count();
        Ok(())
    }

    async fn await_started(&self) -> CommandResult {
        let deadline = TokioInstant::now() + KM_POLL_ALIVE_TIMEOUT;
        loop {
            match self.cluster_state() {
                ClusterState::Started => return Ok(()),
                ClusterState::Dead => {
                    return Err(ProviderError::KminusStartFailed(
                        "the cluster stopped while it was starting; see the kminus logs \
                         under 'cluster/log' for the component that failed"
                            .to_owned(),
                    )
                    .into());
                }
                ClusterState::Starting | ClusterState::Unknown => {
                    if TokioInstant::now() >= deadline {
                        return Err(ProviderError::KminusStartFailed(format!(
                            "the cluster did not report started within {} seconds",
                            KM_POLL_ALIVE_TIMEOUT.as_secs()
                        ))
                        .into());
                    }
                    tokio::time::sleep(KM_POLL_INTERVAL).await;
                }
            }
        }
    }

    fn cluster_state(&self) -> ClusterState {
        let Ok(km) = resolve_python_binary(KM_BINARY) else {
            return ClusterState::Unknown;
        };
        let mut command = SystemCommand::new(&km);
        command
            .arg(KM_STATUS_SUBCOMMAND)
            .arg(KM_WORKDIR_ARGUMENT)
            .arg(self.path())
            .stdin(Stdio::null());
        apply_detached_flags(&mut command);
        match subprocess::run(&mut command, KM_BINARY, false) {
            Ok(output) => ClusterState::from_exit_code(output.status.code()),
            Err(_) => ClusterState::Unknown,
        }
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

    fn stop_through_km(&self) -> bool {
        let Ok(km) = resolve_python_binary(KM_BINARY) else {
            cwarn!("Cannot locate the kminus binary. Signalling the process instead.");
            return false;
        };
        let mut command = SystemCommand::new(&km);
        command
            .arg(KM_STOP_SUBCOMMAND)
            .arg(KM_WORKDIR_ARGUMENT)
            .arg(self.path())
            .stdin(Stdio::null());
        match subprocess::run_with_timeout(&mut command, KM_BINARY, self.verbose, KM_STOP_TIMEOUT) {
            Ok(Some(output)) if output.status.success() => true,
            Ok(Some(output)) => {
                cwarn!(
                    "Kminus stop failed with status {}. Signalling the process instead.{}",
                    output.status,
                    reported(&output)
                );
                false
            }
            Ok(None) => {
                cwarn!(
                    "Kminus stop did not finish within {} seconds. Signalling the process instead.",
                    KM_STOP_TIMEOUT.as_secs()
                );
                false
            }
            Err(error) => {
                cwarn!(
                    "Cannot run the kminus stop command: {error}. Signalling the process instead."
                );
                false
            }
        }
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
        if let Some(portstart) = self.portstart {
            command
                .arg(KM_PORTSTART_ARGUMENT)
                .arg(portstart.to_string());
        }
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
        self.await_started().await?;
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
        if self.stop_through_km() {
            return Ok(true);
        }
        if terminate_single_process(pid, KM_GRACEFUL_TIMEOUT) {
            return Ok(true);
        }
        if force_terminate_process_tree(pid, KM_FORCE_TIMEOUT) {
            return Ok(true);
        }
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
            let _ = folder;
            // fs::remove_dir_all(&folder).ok();
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

#[cfg(test)]
mod tests;
