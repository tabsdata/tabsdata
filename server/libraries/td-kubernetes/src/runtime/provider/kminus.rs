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
use td_common::os::name_program;
use td_common::settings::DEFAULT_INSTANCE;
use td_common::status::ExitStatus::NoAction;
use td_python::python::{get_python_scripts_folder, resolve_python_binary};
use tracing::info;

use crate::base::constants::{
    FOLDER_CLUSTER, FOLDER_DATABASE, FOLDER_IMAGES, FOLDER_PYENV_AG, FOLDER_PYENV_TD,
    FOLDER_PYTHON, FOLDER_REPOSITORY, FOLDER_RUNTIMES, FOLDER_STORAGE, KPROFILE_CONFIGS_FAMILY,
    KPROFILE_PODS_FAMILY, LIBEXEC_TABSDATA,
};
use crate::base::error::provider::ProviderError;
use crate::base::reporter::Reporter;
use crate::client::registry::instances_root;
use crate::client::session::pin::{KminusInputs, Session};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::validate_instance_name;
use crate::core::system::exit::{CommandError, CommandResult, abort_with};
use crate::core::system::file::copy_tree;
use crate::core::system::subprocess;
use crate::core::system::subprocess::apply_detached_flags;
use crate::kube::api::get_secret;
use crate::kube::kubeconfig::{
    build_kubeconfig, resolve_cluster_name, resolve_cluster_url, resolve_current_context,
    resolve_namespace,
};
use crate::runtime::gateway::resolver::{ConfigurationResolver, SystemValues};
use crate::runtime::instance::cluster::{ClusterDescriptor, ClusterProperties, ClusterSettings};
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::profile::{
    Profile, load_profile, render_for_configmaps, resolve_kprofile_path, resolve_profile_entry,
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
use crate::runtime::secrets::reconciliation::auto_fill_generated_secrets;
use crate::runtime::secrets::source::to_secret_name;
use crate::runtime::secrets::{KubeconfigSecretGenerator, SecretGenerator, SecretPhase};

const DBMANAGER_BINARY: &str = "td_dbmanager";
const PYVENV_BINARY: &str = "td_pyvenv";
const APISERVER_SERVICE_CONFIG_KEY: &str = "apiserver.service.yaml";
const APISERVER_DATABASE_CONFIG_KEY: &str = "apiserver.database.yaml";
const APISERVER_STORAGE_CONFIG_KEY: &str = "apiserver.storage.yaml";
const APISERVER_CONFIG_KEYS: &[&str] = &[
    APISERVER_SERVICE_CONFIG_KEY,
    APISERVER_DATABASE_CONFIG_KEY,
    APISERVER_STORAGE_CONFIG_KEY,
];

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
    ) -> CommandResult<Self> {
        if cluster.context.is_some()
            || cluster.kubeconfig.is_some()
            || cluster.current
            || cluster.namespace.is_some()
        {
            return Err(ProviderError::ArgumentNotValidForProvider {
                argument: "context | kubeconfig | current | namespace".to_owned(),
                provider: ClusterProvider::Kminus.as_str().to_owned(),
            }
            .into());
        }
        let inputs = KminusInputs {
            name: instance.name.clone(),
        };

        let name = instance
            .name
            .clone()
            .unwrap_or_else(|| DEFAULT_INSTANCE.to_string());
        let folder = instances_root()?.join(&name);

        if let Err(error) = validate_instance_name(&name) {
            return Err(ProviderError::InvalidInstanceName {
                name: name.clone(),
                reason: error.to_string(),
            }
            .into());
        }

        let kminus_cluster = KminusCluster::new(
            folder.join(FOLDER_CLUSTER),
            folder.join(FOLDER_IMAGES),
            verbose,
        );
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

    fn profile(&self) -> CommandResult<&Profile> {
        self.profile.get_or_try_init(|| {
            load_profile(self.cluster_provider(), self.verbose).map_err(CommandError::from)
        })
    }

    fn bootstrap_cluster(&self) -> CommandResult<Box<dyn Transaction>> {
        fs::create_dir_all(&self.folder).map_err(|error| {
            ProviderError::InstanceFolderCreateFailed(
                self.folder.display().to_string(),
                error.to_string(),
            )
        })?;
        let guard = InstanceFolderGuard::new(self.folder.clone());
        let repository = self.folder.join(FOLDER_REPOSITORY);
        for subfolder in [
            &repository,
            &repository.join(FOLDER_DATABASE),
            &repository.join(FOLDER_STORAGE),
        ] {
            fs::create_dir_all(subfolder).map_err(|error| {
                ProviderError::InstanceFolderCreateFailed(
                    subfolder.display().to_string(),
                    error.to_string(),
                )
            })?;
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

    async fn kube_config(&self) -> CommandResult<kube::Config> {
        Ok(build_kubeconfig(Some(&self.cluster.kubefile()), None, None).await?)
    }

    fn system_values(&self) -> CommandResult<SystemValues> {
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
        let scripts_folder = get_python_scripts_folder()
            .map_err(|error| ProviderError::PythonScriptsFolderResolveFailed(error.to_string()))?;
        let Some(scripts_name) = scripts_folder.file_name().and_then(|name| name.to_str()) else {
            return Err(ProviderError::PythonScriptsFolderNameInvalid(
                scripts_folder.display().to_string(),
            )
            .into());
        };
        let pyenv_scripts_folder = Some(scripts_name.to_string());
        let pyenv_libexec_folder = Some(LIBEXEC_TABSDATA.to_owned());
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
            pyenv_libexec_folder,
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

    fn cluster_settings(&self, _provided: ClusterProperties) -> CommandResult<ClusterSettings> {
        Ok(ClusterSettings::default())
    }

    fn cluster_descriptor(&self, settings: ClusterSettings) -> CommandResult<ClusterDescriptor> {
        Ok(ClusterDescriptor {
            kubeconfig: settings.kubeconfig,
            context: settings.context,
            namespace: settings.namespace,
            cluster: None,
            url: None,
        })
    }

    async fn ensure_pinnable_server(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        if !self.cluster.sysconfig_path().exists() {
            return Err(ProviderError::InvalidKminusInstance {
                folder: self.folder.display().to_string(),
                marker: self.cluster.sysconfig_path().display().to_string(),
            }
            .into());
        }
        Ok(())
    }

    async fn ensure_creatable_server(&self) -> CommandResult {
        if self.folder.exists() {
            return abort_with(
                NoAction,
                ProviderError::InstanceFolderExists(self.folder.display().to_string()),
            );
        }
        Ok(())
    }

    async fn create_cluster_hook(
        &self,
        reporter: &dyn Reporter,
    ) -> CommandResult<Box<dyn Transaction>> {
        let step = reporter.step("Preparing kminus cluster");
        let result = self.bootstrap_cluster()?;
        step.done("Kminus cluster prepared");
        let step = reporter.step("Staging tabsdata configmap");
        self.stage_configmaps().await?;
        step.done("Tabsdata configmap staged");
        self.start_cluster_hook(reporter).await?;
        Ok(result)
    }

    async fn create_database(&self, reporter: &dyn Reporter) -> CommandResult {
        let loaded = self.profile()?;
        let resolver = ConfigurationResolver::new(self.system_values()?);
        let context = self.kube_context().await?;
        auto_fill_generated_secrets(&context).await?;

        let mut config_files: Vec<tempfile::NamedTempFile> = Vec::new();
        let mut secret_env: Vec<(String, String)> = Vec::new();
        for config_key in APISERVER_CONFIG_KEYS {
            let config =
                resolve_profile_entry(loaded, &resolver, &KPROFILE_CONFIGS_FAMILY, config_key)
                    .await?;
            let mut config_file = tempfile::Builder::new()
                .prefix("td-apiserver-")
                .suffix(".yaml")
                .tempfile()
                .map_err(|error| {
                    ProviderError::ApiserverTempConfigCreateFailed(error.to_string())
                })?;
            config_file.write_all(config.as_bytes()).map_err(|error| {
                ProviderError::ApiserverTempConfigWriteFailed(
                    config_file.path().display().to_string(),
                    error.to_string(),
                )
            })?;
            config_file.flush().map_err(|error| {
                ProviderError::ApiserverTempConfigFlushFailed(
                    config_file.path().display().to_string(),
                    error.to_string(),
                )
            })?;
            config_files.push(config_file);

            let secret_name = to_secret_name(config_key)?;
            if let Some(data) = get_secret(&context, &secret_name).await? {
                for (key, value) in data {
                    if let Ok(text) = String::from_utf8(value) {
                        secret_env.push((key, text));
                    }
                }
            }
        }

        let dbmanager = name_program(&PathBuf::from(DBMANAGER_BINARY));
        let command_label = format!("{} create", dbmanager.display());
        let step = reporter.step("Creating tabsdata database schema");
        let mut command = std::process::Command::new(&dbmanager);
        command.arg("create");
        for config_file in &config_files {
            command.arg("--config-file").arg(config_file.path());
        }
        for (key, value) in &secret_env {
            command.env(key, value);
        }
        apply_detached_flags(&mut command);
        let output = match subprocess::run(&mut command, DBMANAGER_BINARY, self.verbose) {
            Ok(output) => output,
            Err(error) => {
                step.fail();
                return Err(ProviderError::ApiserverInvocationFailed {
                    command: command_label,
                    cause: error.to_string(),
                }
                .into());
            }
        };
        if !output.status.success() {
            step.fail();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            return Err(ProviderError::ApiserverFailedStatus {
                command: command_label,
                status: output.status.to_string(),
                stderr,
            }
            .into());
        }
        step.done("Tabsdata database schema created");
        Ok(())
    }

    async fn create_runtimes(&self, reporter: &dyn Reporter) -> CommandResult {
        let runtimes_python = self.folder.join(FOLDER_RUNTIMES).join(FOLDER_PYTHON);
        fs::create_dir_all(&runtimes_python).map_err(|error| {
            ProviderError::RuntimesFolderCreateFailed(
                runtimes_python.display().to_string(),
                error.to_string(),
            )
        })?;
        let pyvenv = resolve_python_binary(PYVENV_BINARY).map_err(|error| {
            ProviderError::BinaryNotFound(PYVENV_BINARY.to_owned(), error.to_string())
        })?;
        let step = reporter.step("Creating Python virtual environment templates");
        let mut command = std::process::Command::new(&pyvenv);
        if self.verbose {
            command.arg("--verbose");
        }
        command.arg("create").arg("--target").arg(&runtimes_python);
        apply_detached_flags(&mut command);
        let output = match subprocess::run(&mut command, PYVENV_BINARY, self.verbose) {
            Ok(output) => output,
            Err(error) => {
                step.fail();
                return Err(ProviderError::PyvenvCreateFailed {
                    folder: runtimes_python.display().to_string(),
                    cause: error.to_string(),
                }
                .into());
            }
        };
        if !output.status.success() {
            step.fail();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            return Err(ProviderError::PyvenvCreateFailed {
                folder: runtimes_python.display().to_string(),
                cause: stderr,
            }
            .into());
        }
        step.done("Python virtual environment templates created");
        info!("Current Python environment tabsdata metadata:");
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            info!("  {line}");
        }
        Ok(())
    }

    async fn create_images(&self, reporter: &dyn Reporter) -> CommandResult {
        let images_target = self.folder.join(FOLDER_IMAGES);
        fs::create_dir_all(&images_target).map_err(|error| {
            ProviderError::ImagesFolderCreateFailed(
                images_target.display().to_string(),
                error.to_string(),
            )
        })?;
        let kprofile_root = resolve_kprofile_path(self.verbose)?;
        let images_source = kprofile_root.join(FOLDER_IMAGES);
        if !images_source.is_dir() {
            return Ok(());
        }
        let step = reporter.step("Copying worker image trees");
        if let Err(cause) = copy_tree(&images_source, &images_target) {
            step.fail();
            return Err(ProviderError::ImagesCopyFailed {
                from: images_source.display().to_string(),
                to: images_target.display().to_string(),
                cause,
            }
            .into());
        }
        step.done("Worker image trees copied");
        Ok(())
    }

    async fn cluster_alive(&self) -> bool {
        self.cluster.is_alive().await
    }

    async fn instance_alive(&self) -> bool {
        self.cluster_alive().await && self.instance.is_alive().await
    }

    fn staging_profile_path(&self) -> Option<PathBuf> {
        Some(self.cluster.staging_profile_path())
    }

    async fn start_cluster_hook(&self, reporter: &dyn Reporter) -> CommandResult {
        let step = reporter.step("Starting kminus cluster");
        self.cluster.start().await?;
        step.done("Kminus cluster started");
        self.instance.promote_stage().await?;
        handle_phase_secrets(self, SecretPhase::ClusterStart).await?;
        Ok(())
    }

    async fn stop_cluster(&self, reporter: &dyn Reporter) -> CommandResult<ClusterStopOutcome> {
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

    async fn ensure_startable_cluster(&self) -> CommandResult {
        Ok(())
    }

    async fn ensure_stoppable_cluster(&self) -> CommandResult {
        Ok(())
    }

    async fn ensure_restartable_cluster(&self) -> CommandResult {
        Ok(())
    }

    async fn start_instance_hook(&self, reporter: &dyn Reporter) -> CommandResult {
        handle_phase_secrets(self, SecretPhase::InstanceStart).await?;
        let step = reporter.step("Starting tabsdata instance");
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        self.apply_workload(&multi).await?;
        step.done("Tabsdata instance started");
        Ok(())
    }

    async fn stop_instance(&self, reporter: &dyn Reporter) -> CommandResult<InstanceStopOutcome> {
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

    async fn ensure_startable_instance(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        Ok(())
    }

    async fn ensure_stoppable_instance(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        Ok(())
    }

    async fn ensure_restartable_instance(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        Ok(())
    }

    async fn ensure_startable_server(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        if cfg!(td_k8s_development) {
            let manifest_path = self.cluster.staging_profile_path().join(MANIFEST_FILE);
            if manifest_path.exists() {
                let yaml = fs::read_to_string(&manifest_path).map_err(|error| {
                    ProviderError::StagingReadFailed(
                        manifest_path.display().to_string(),
                        error.to_string(),
                    )
                })?;
                verify_manifest_compatibility(&yaml)?;
            }
        }
        Ok(())
    }

    async fn ensure_stoppable_server(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        Ok(())
    }

    async fn ensure_restartable_server(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        Ok(())
    }
}

impl ProcessProvider for KminusProvider {}

impl HostedProvider for KminusProvider {}

impl LocalProvider for KminusProvider {}

impl KminusProvider {
    async fn stage_configmaps(&self) -> CommandResult {
        let loaded = self.profile()?;
        let staging_profile = self.cluster.staging_profile_path();
        fs::create_dir_all(&staging_profile).map_err(|error| {
            ProviderError::StagingFolderCreateFailed(
                staging_profile.display().to_string(),
                error.to_string(),
            )
        })?;
        let resolver = ConfigurationResolver::new(self.system_values()?);
        let rendered = render_for_configmaps(loaded, &resolver).await?;
        for (family, data) in [
            (&KPROFILE_PODS_FAMILY, rendered.pods),
            (&KPROFILE_CONFIGS_FAMILY, rendered.configs),
        ] {
            let family_path = staging_profile.join(family.folder);
            fs::create_dir_all(&family_path).map_err(|error| {
                ProviderError::StagingFolderCreateFailed(
                    family_path.display().to_string(),
                    error.to_string(),
                )
            })?;
            for (key, value) in data {
                let file_path = family_path.join(&key);
                fs::write(&file_path, value.as_bytes()).map_err(|error| {
                    ProviderError::StagingWriteFailed(
                        file_path.display().to_string(),
                        error.to_string(),
                    )
                })?;
            }
        }
        let manifest_path = staging_profile.join(MANIFEST_FILE);
        fs::write(&manifest_path, rendered.manifest.as_bytes()).map_err(|error| {
            ProviderError::StagingWriteFailed(
                manifest_path.display().to_string(),
                error.to_string(),
            )
        })?;
        Ok(())
    }

    async fn apply_workload(&self, multi: &MultiProgress) -> CommandResult {
        self.instance.start(multi).await
    }

    async fn teardown_workload(&self, multi: &MultiProgress) -> CommandResult<bool> {
        self.instance.stop(multi).await
    }
}
