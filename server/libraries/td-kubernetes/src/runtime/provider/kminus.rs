//
// Copyright 2026 Tabs Data Inc.
//

pub mod cluster;
pub mod instance;

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use async_trait::async_trait;
use indicatif::MultiProgress;
use once_cell::sync::OnceCell;
use td_common::env::TABSDATA_HOME_DIR;
use td_common::os::name_program;
use td_common::server::{DEFAULT_INSTANCE, INSTANCES_FOLDER};
use td_common::status::ExitStatus::{GeneralError, NoAction};
use td_python::python::{get_python_scripts_folder, resolve_python_binary};
use tracing::info;

use crate::base::constants::{
    CONFIGMAP_TABSDATA_CONFIGS, CONFIGMAP_TABSDATA_GLOBALS, CONFIGMAP_TABSDATA_PODS,
    FOLDER_CLUSTER, FOLDER_DATABASE, FOLDER_PYENV_AG, FOLDER_PYENV_TD, FOLDER_PYTHON,
    FOLDER_REPOSITORY, FOLDER_RUNTIMES, FOLDER_STORAGE, KPROFILE_CONFIGS_FOLDER,
    KPROFILE_GLOBALS_FOLDER, KPROFILE_PODS_FOLDER,
};
use crate::base::reporter::Reporter;
use crate::cerror;
use crate::client::session::pin::{KminusInputs, Session};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::validate_instance_name;
use crate::core::system::exit::{Result, abort};
use crate::core::system::subprocess;
use crate::core::system::subprocess::apply_detached_flags;
use crate::kube::api::require_configmap;
use crate::kube::kubeconfig::{
    build_kubeconfig, resolve_cluster_name, resolve_cluster_url, resolve_current_context,
    resolve_namespace,
};
use crate::runtime::gateway::resolver::{ConfigurationResolver, SystemValues};
use crate::runtime::instance::launcher::{start_workload, stop_workload};
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::profile::{
    Profile, load_profile, parse_from_configmaps, render_for_configmaps, resolve_profile_entry,
};
use crate::runtime::instance::verify_manifest_compatibility;
use crate::runtime::provider::kminus::cluster::{InstanceFolderGuard, KminusCluster};
use crate::runtime::provider::kminus::instance::KminusInstance;
use crate::runtime::provider::outcome::{ClusterStopOutcome, InstanceStopOutcome};
use crate::runtime::provider::summary::Summary;
use crate::runtime::provider::{
    Cluster, ClusterProvider, HostedProvider, Instance, LocalProvider, ProcessProvider, Provider,
    Transaction, handle_phase_secrets,
};
use crate::runtime::secrets::{KubeconfigSecretGenerator, SecretGenerator, SecretPhase};

const APISERVER_BINARY: &str = "td_apiserver";
const PYVENV_BINARY: &str = "td_pyvenv";
const APISERVER_CONFIG_KEY: &str = "apiserver.yaml";

pub struct KminusProvider {
    name: String,
    folder: PathBuf,
    cluster: KminusCluster,
    instance: KminusInstance,
    inputs: KminusInputs,
    verbose: bool,
    profile: OnceCell<Profile>,
}

impl KminusProvider {
    pub fn build(
        cluster: ClusterArguments,
        instance: InstanceArguments,
        verbose: bool,
    ) -> Result<Self> {
        if cluster.context.is_some()
            || cluster.kubeconfig.is_some()
            || cluster.current
            || cluster.namespace.is_some()
        {
            cerror!(
                "Arguments --context, --kubeconfig, --current, and --namespace are not valid for the '{}' provider.",
                ClusterProvider::Kminus.as_str()
            );
            return abort(GeneralError);
        }
        if instance.name.is_some() && instance.path.is_some() {
            cerror!("Arguments --name and --path are mutually exclusive.");
            return abort(GeneralError);
        }

        let inputs = KminusInputs {
            name: instance.name.clone(),
            path: instance.path.clone(),
        };

        let instances_root = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("/"))
            .join(TABSDATA_HOME_DIR)
            .join(INSTANCES_FOLDER);
        let (name, folder) = match (instance.name.as_deref(), instance.path.as_deref()) {
            (None, None) => {
                let name = DEFAULT_INSTANCE.to_string();
                let folder = instances_root.join(&name);
                (name, folder)
            }
            (Some(name), None) => {
                let folder = instances_root.join(name);
                (name.to_owned(), folder)
            }
            (None, Some(path)) => {
                let folder = if path.is_relative() {
                    instances_root.join(path)
                } else {
                    path.to_path_buf()
                };
                let name = folder
                    .file_name()
                    .and_then(|component| component.to_str())
                    .map(str::to_owned)
                    .unwrap_or_else(|| DEFAULT_INSTANCE.to_string());
                (name, folder)
            }
            (Some(_), Some(_)) => {
                cerror!("Arguments --name and --path are mutually exclusive.");
                return abort(GeneralError);
            }
        };

        if let Err(error) = validate_instance_name(&name) {
            cerror!("{error}");
            return abort(GeneralError);
        }

        let kminus_cluster = KminusCluster::new(folder.join(FOLDER_CLUSTER), verbose);
        let kminus_instance =
            KminusInstance::new(kminus_cluster.kubefile(), kminus_cluster.staging_path());

        Ok(Self {
            name,
            folder,
            cluster: kminus_cluster,
            instance: kminus_instance,
            inputs,
            verbose,
            profile: OnceCell::new(),
        })
    }

    fn profile(&self) -> Result<&Profile> {
        self.profile
            .get_or_try_init(|| load_profile(self.cluster_provider(), self.verbose))
    }

    fn bootstrap_cluster(&self) -> Result<Box<dyn Transaction>> {
        if let Err(error) = fs::create_dir_all(&self.folder) {
            cerror!(
                "Failed to create instance folder {}: {error}",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        let guard = InstanceFolderGuard::new(self.folder.clone());
        let repository = self.folder.join(FOLDER_REPOSITORY);
        for subfolder in [
            &repository,
            &repository.join(FOLDER_DATABASE),
            &repository.join(FOLDER_STORAGE),
        ] {
            if let Err(error) = fs::create_dir_all(subfolder) {
                cerror!(
                    "Failed to create repository folder {}: {error}",
                    subfolder.display()
                );
                return abort(GeneralError);
            }
        }
        self.cluster.create()?;
        Ok(Box::new(guard))
    }

    fn kube_summary(&self) -> KubeSummary {
        let kubefile = self.cluster.kubefile();
        let context = resolve_current_context(Some(&kubefile)).unwrap_or_default();
        let cluster = resolve_cluster_name(Some(&kubefile), Some(&context)).unwrap_or_default();
        let cluster_url = resolve_cluster_url(Some(&kubefile), Some(&context)).unwrap_or_default();
        let namespace =
            resolve_namespace(Some(&kubefile), Some(&context)).unwrap_or_else(|| "default".into());
        KubeSummary {
            kubefile,
            context,
            cluster,
            cluster_url,
            namespace,
        }
    }
}

struct KubeSummary {
    kubefile: PathBuf,
    context: String,
    cluster: String,
    cluster_url: String,
    namespace: String,
}

#[async_trait]
impl Provider for KminusProvider {
    fn name(&self) -> &'static str {
        ClusterProvider::Kminus.as_str()
    }

    fn display_name(&self) -> String {
        format!("{}:{}", ClusterProvider::Kminus.as_str(), self.name)
    }

    fn to_session(&self) -> Session {
        let summary = self.kube_summary();
        Session::Kminus {
            path: self.folder.clone(),
            kubeconfig: summary.kubefile,
            context: summary.context,
            cluster: summary.cluster,
            namespace: summary.namespace,
            inputs: self.inputs.clone(),
        }
    }

    fn cluster_provider(&self) -> ClusterProvider {
        ClusterProvider::Kminus
    }

    async fn kube_config(&self) -> Result<kube::Config> {
        build_kubeconfig(Some(&self.cluster.kubefile()), None, None).await
    }

    fn system_values(&self) -> Result<SystemValues> {
        let repository_folder = self.folder.join(FOLDER_REPOSITORY);
        let repository_url = url::Url::from_directory_path(&repository_folder)
            .ok()
            .map(|url| url.to_string());
        let repository_path = Some(repository_folder.to_string_lossy().to_string());
        let database_folder = repository_folder.join(FOLDER_DATABASE);
        let database_url = url::Url::from_directory_path(&database_folder)
            .ok()
            .map(|url| url.to_string());
        let database_path = Some(database_folder.to_string_lossy().to_string());
        let storage_folder = repository_folder.join(FOLDER_STORAGE);
        let storage_url = url::Url::from_directory_path(&storage_folder)
            .ok()
            .map(|url| url.to_string());
        let storage_path = Some(storage_folder.to_string_lossy().to_string());
        let runtimes_python = self.folder.join(FOLDER_RUNTIMES).join(FOLDER_PYTHON);
        let pyenv_td_root = Some(
            runtimes_python
                .join(FOLDER_PYENV_TD)
                .to_string_lossy()
                .to_string(),
        );
        let pyenv_ag_root = Some(
            runtimes_python
                .join(FOLDER_PYENV_AG)
                .to_string_lossy()
                .to_string(),
        );
        let scripts_folder = match get_python_scripts_folder() {
            Ok(path) => path,
            Err(error) => {
                cerror!("Failed to resolve the Python scripts folder: {error}");
                return abort(GeneralError);
            }
        };
        let Some(scripts_name) = scripts_folder.file_name().and_then(|name| name.to_str()) else {
            cerror!(
                "Unable to extract the Python scripts folder name from '{}'.",
                scripts_folder.display()
            );
            return abort(GeneralError);
        };
        let pyenv_scripts_folder = Some(scripts_name.to_string());
        let instance_name = self
            .folder
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.to_owned());
        Ok(SystemValues {
            repository_url,
            repository_path,
            database_url,
            database_path,
            storage_url,
            storage_path,
            pyenv_td_root,
            pyenv_ag_root,
            pyenv_scripts_folder,
            instance_name,
        })
    }

    fn secret_generators(&self, phase: SecretPhase) -> Vec<Box<dyn SecretGenerator>> {
        match phase {
            SecretPhase::ClusterStart => vec![Box::new(KubeconfigSecretGenerator::new(
                self.cluster.kubefile(),
            ))],
            SecretPhase::InstanceStart => Vec::new(),
        }
    }

    fn summary(&self) -> Summary {
        let kube = self.kube_summary();
        Summary {
            provider: ClusterProvider::Kminus,
            instance_name: Some(self.name.clone()),
            instance_path: Some(self.folder.clone()),
            kubeconfig_path: Some(kube.kubefile),
            context: Some(kube.context),
            cluster_url: Some(kube.cluster_url),
            cluster_name: Some(kube.cluster),
            namespace: Some(kube.namespace),
        }
    }

    async fn ensure_pinnable_server(&self) -> Result {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist. Run 'tdkserver server create ...' first.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        if !self.cluster.sysconfig_path().exists() {
            cerror!(
                "Folder '{}' is not a valid kminus instance (missing '{}').",
                self.folder.display(),
                self.cluster.sysconfig_path().display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    async fn ensure_creatable_server(&self) -> Result {
        if self.folder.exists() {
            info!("Folder {} already exists.", self.folder.display());
            return abort(NoAction);
        }
        Ok(())
    }

    async fn create_cluster_hook(&self, reporter: &dyn Reporter) -> Result<Box<dyn Transaction>> {
        let step = reporter.step("Preparing kminus cluster");
        let result = self.bootstrap_cluster()?;
        step.done("Kminus cluster prepared");
        let step = reporter.step("Staging tabsdata configmap");
        self.stage_configmaps().await?;
        step.done("Tabsdata configmap staged");
        Ok(result)
    }

    async fn create_database(&self, reporter: &dyn Reporter) -> Result {
        let loaded = self.profile()?;
        let resolver = ConfigurationResolver::new(self.system_values()?);
        let apiserver_config = resolve_profile_entry(
            loaded,
            &resolver,
            KPROFILE_CONFIGS_FOLDER,
            APISERVER_CONFIG_KEY,
        )
        .await?;
        let mut config_file = match tempfile::Builder::new()
            .prefix("td-apiserver-")
            .suffix(".yaml")
            .tempfile()
        {
            Ok(file) => file,
            Err(error) => {
                cerror!("Failed to create temporary apiserver config file: {error}");
                return abort(GeneralError);
            }
        };
        if let Err(error) = config_file.write_all(apiserver_config.as_bytes()) {
            cerror!(
                "Failed to write temporary apiserver config '{}': {error}",
                config_file.path().display()
            );
            return abort(GeneralError);
        }
        if let Err(error) = config_file.flush() {
            cerror!(
                "Failed to flush temporary apiserver config '{}': {error}",
                config_file.path().display()
            );
            return abort(GeneralError);
        }
        let apiserver = name_program(&PathBuf::from(APISERVER_BINARY));
        let step = reporter.step("Creating tabsdata database schema");
        let mut command = std::process::Command::new(&apiserver);
        command
            .arg("--config-file")
            .arg(config_file.path())
            .arg("--db-schema")
            .arg("create");
        apply_detached_flags(&mut command);
        let result = subprocess::run(&mut command, APISERVER_BINARY, self.verbose);
        match result {
            Ok(output) => {
                if !output.status.success() {
                    step.fail();
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    cerror!(
                        "'{} --db-schema create' failed with status {}: {}",
                        apiserver.display(),
                        output.status,
                        stderr.trim()
                    );
                    return abort(GeneralError);
                }
                step.done("Tabsdata database schema created");
                Ok(())
            }
            Err(error) => {
                step.fail();
                cerror!(
                    "Failed to run '{} --db-schema create': {error}",
                    apiserver.display()
                );
                abort(GeneralError)
            }
        }
    }

    async fn create_runtimes(&self, reporter: &dyn Reporter) -> Result {
        let runtimes_python = self.folder.join(FOLDER_RUNTIMES).join(FOLDER_PYTHON);
        if let Err(error) = fs::create_dir_all(&runtimes_python) {
            cerror!(
                "Failed to create runtimes folder '{}': {error}",
                runtimes_python.display()
            );
            return abort(GeneralError);
        }
        let pyvenv = match resolve_python_binary(PYVENV_BINARY) {
            Ok(path) => path,
            Err(error) => {
                cerror!("Failed to locate the '{PYVENV_BINARY}' binary: {error}");
                return abort(GeneralError);
            }
        };
        let step = reporter.step("Creating Python virtual environment templates");
        let mut command = std::process::Command::new(&pyvenv);
        if self.verbose {
            command.arg("--verbose");
        }
        command.arg("create").arg("--target").arg(&runtimes_python);
        apply_detached_flags(&mut command);
        let result = subprocess::run(&mut command, PYVENV_BINARY, self.verbose);
        match result {
            Ok(output) => {
                if !output.status.success() {
                    step.fail();
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    cerror!(
                        "Failed to create Python virtual environment templates under '{}': {}",
                        runtimes_python.display(),
                        stderr.trim()
                    );
                    return abort(GeneralError);
                }
                step.done("Python virtual environment templates created");
                info!("Current Python environment tabsdata metadata:");
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    info!("  {line}");
                }
                Ok(())
            }
            Err(error) => {
                step.fail();
                cerror!(
                    "Failed to create Python virtual environment templates under '{}': {error}",
                    runtimes_python.display()
                );
                abort(GeneralError)
            }
        }
    }

    async fn cluster_alive(&self) -> bool {
        self.cluster.is_alive().await
    }

    async fn start_cluster_hook(&self, reporter: &dyn Reporter) -> Result {
        let step = reporter.step("Starting kminus cluster");
        self.cluster.start().await?;
        step.done("Kminus cluster started");
        self.instance.start().await?;
        handle_phase_secrets(self, SecretPhase::ClusterStart).await?;
        Ok(())
    }

    async fn stop_cluster(&self, reporter: &dyn Reporter) -> Result<ClusterStopOutcome> {
        let step = reporter.step("Stopping kminus cluster");
        let stopped = self.cluster.stop().await?;
        if stopped {
            step.done("Kminus cluster stopped");
            Ok(ClusterStopOutcome::Stopped)
        } else {
            step.fail();
            Ok(ClusterStopOutcome::AlreadyStopped)
        }
    }

    async fn ensure_startable_cluster(&self) -> Result {
        Ok(())
    }

    async fn ensure_stoppable_cluster(&self) -> Result {
        Ok(())
    }

    async fn ensure_restartable_cluster(&self) -> Result {
        Ok(())
    }

    async fn start_instance_hook(&self, reporter: &dyn Reporter) -> Result {
        handle_phase_secrets(self, SecretPhase::InstanceStart).await?;
        let step = reporter.step("Starting tabsdata instance");
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        self.apply_workload(&multi).await?;
        step.done("Tabsdata instance started");
        Ok(())
    }

    async fn stop_instance(&self, reporter: &dyn Reporter) -> Result<InstanceStopOutcome> {
        let step = reporter.step("Stopping tabsdata instance");
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        let stopped = self.teardown_workload(&multi).await?;
        if stopped {
            step.done("Tabsdata instance stopped");
            Ok(InstanceStopOutcome::Stopped)
        } else {
            step.fail();
            Ok(InstanceStopOutcome::AlreadyStopped)
        }
    }

    async fn ensure_startable_instance(&self) -> Result {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    async fn ensure_stoppable_instance(&self) -> Result {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    async fn ensure_restartable_instance(&self) -> Result {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    async fn ensure_startable_server(&self) -> Result {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist. Run 'create' first.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        if cfg!(td_k8s_development) {
            let manifest_path = self.cluster.staging_profile_path().join(MANIFEST_FILE);
            if manifest_path.exists() {
                let yaml = match fs::read_to_string(&manifest_path) {
                    Ok(yaml) => yaml,
                    Err(error) => {
                        cerror!(
                            "Failed to read staged manifest '{}': {error}",
                            manifest_path.display()
                        );
                        return abort(GeneralError);
                    }
                };
                verify_manifest_compatibility(&yaml)?;
            }
        }
        Ok(())
    }

    async fn ensure_stoppable_server(&self) -> Result {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    async fn ensure_restartable_server(&self) -> Result {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }
}

impl ProcessProvider for KminusProvider {}

impl HostedProvider for KminusProvider {}

impl LocalProvider for KminusProvider {}

impl KminusProvider {
    async fn stage_configmaps(&self) -> Result {
        let loaded = self.profile()?;
        let staging_profile = self.cluster.staging_profile_path();
        if let Err(error) = fs::create_dir_all(&staging_profile) {
            cerror!(
                "Failed to create staging folder '{}': {error}",
                staging_profile.display()
            );
            return abort(GeneralError);
        }
        let resolver = ConfigurationResolver::new(self.system_values()?);
        let rendered = render_for_configmaps(loaded, &resolver).await?;
        for (folder, data) in [
            (KPROFILE_GLOBALS_FOLDER, rendered.globals),
            (KPROFILE_PODS_FOLDER, rendered.pods),
            (KPROFILE_CONFIGS_FOLDER, rendered.configs),
        ] {
            let folder_path = staging_profile.join(folder);
            if let Err(error) = fs::create_dir_all(&folder_path) {
                cerror!(
                    "Failed to create staging folder '{}': {error}",
                    folder_path.display()
                );
                return abort(GeneralError);
            }
            for (key, value) in data {
                let file_path = folder_path.join(&key);
                if let Err(error) = fs::write(&file_path, value.as_bytes()) {
                    cerror!("Failed to write '{}': {error}", file_path.display());
                    return abort(GeneralError);
                }
            }
        }
        Ok(())
    }

    async fn apply_workload(&self, multi: &MultiProgress) -> Result {
        let context = self.kube_context().await?;
        let globals = require_configmap(&context, CONFIGMAP_TABSDATA_GLOBALS).await?;
        let pods = require_configmap(&context, CONFIGMAP_TABSDATA_PODS).await?;
        let configs = require_configmap(&context, CONFIGMAP_TABSDATA_CONFIGS).await?;
        let profile = parse_from_configmaps(&globals, &pods, &configs)?;
        start_workload(self, &profile, multi).await
    }

    async fn teardown_workload(&self, multi: &MultiProgress) -> Result<bool> {
        let stopped_workload = stop_workload(self, multi).await?;
        let stopped_instance = self.instance.stop().await?;
        Ok(stopped_workload || stopped_instance)
    }
}
