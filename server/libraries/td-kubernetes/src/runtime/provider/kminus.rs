//
// Copyright 2026 Tabsdata Inc.
//

pub mod cluster;
pub mod instance;

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use fslock::LockFile;
use indicatif::MultiProgress;
use once_cell::sync::OnceCell;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::time::Duration;
use td_common::os::EXE_SUFFIX;
use td_common::settings::DEFAULT_INSTANCE;
use td_common::status::ExitStatus::NoAction;
use td_error::TdError;
use td_python::python::{get_python_scripts_folder, resolve_python_binary};
use tracing::{info, warn};

use crate::base::constants::{
    CONFIGMAP_TABSDATA_MANIFEST, DATABASE_HOST_LOOPBACK, DATABASE_NAME_POSTGRES,
    DATABASE_SSL_MODE_PREFER, DEFAULT_NAMESPACE, DbManagerMode, ENV_TD_KUBE_UV_MODE,
    ENV_UV_PYTHON_INSTALL_DIR, FOLDER_CLUSTER, FOLDER_DATABASE, FOLDER_IMAGES, FOLDER_KENVS,
    FOLDER_PYENV_AG, FOLDER_PYENV_FN, FOLDER_PYENV_TD, FOLDER_PYTHON, FOLDER_REPOSITORY,
    FOLDER_RUNTIMES, FOLDER_STORAGE, FOLDER_UV, FOREIGN_ASSETS, IndexReach, KPROFILE_ASSETS_FAMILY,
    KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY, LIBEXEC_TABSDATA, LogLevel, RUNTIMES_LOCK_FILE,
    UV_MODE_LINKED, UV_MODE_LOCAL, UvMode, VENVS, VENVS_IMAGE, Venv, YAML_NULL,
};
use td_python::tabsdata::tabsdata_version;

use crate::base::error::provider::ProviderError;
use crate::base::reporter::Reporter;
use crate::base::symlink::{is_symlink, relink};
use crate::client::registry::{
    ClusterViews, InstanceSettings, init_stage_folder_in, instances_root,
};
use crate::client::session::settings::{KminusInputs, Session};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::validate_instance_name;
use crate::client::tdkserver::render::view::InstanceView;
use crate::core::system::exit::{CommandError, CommandResult, abort_with};
use crate::core::system::file::copy_tree;
use crate::core::system::subprocess;
use crate::core::system::subprocess::apply_detached_flags;
use crate::kube::api::entity::configmap::require_configmap;
use crate::kube::api::entity::secret::get_secret;
use crate::kube::context::KubeContext;
use crate::kube::kubeconfig::{
    build_kubeconfig, resolve_cluster_name, resolve_cluster_url, resolve_current_context,
    resolve_namespace,
};
use crate::runtime::gateway::resolver::{ConfigurationResolver, SystemValues};
use crate::runtime::instance::cluster::{ClusterDescriptor, ClusterProperties, ClusterSettings};
use crate::runtime::instance::image::write_venvs_image;
use crate::runtime::instance::launcher::{
    provision_database, provision_pyvenvs, start_database, stop_database,
};
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::ports::{
    CHECK_ALL_PORTS_FILTER, PORT_RELEASE_AFTER_KILL_TIMEOUT, PORT_RELEASE_TIMEOUT, apply_ports,
    database_port, ensure_ports_free, ensure_ports_unclaimed_on_host, kill_port_holders,
    wait_for_ports_released, wait_for_ports_released_within,
};
use crate::runtime::instance::profile::{
    Profile, RenderedConfigmap, load_profile, parse_from_configmaps, read_family_configmaps,
    render_for_configmaps, resolve_foreign_path, resolve_kprofile_path,
};
use crate::runtime::instance::upgrade::guard::ensure_upgraded;
use crate::runtime::instance::verify_manifest_compatibility;
use crate::runtime::provider::kminus::cluster::{InstanceFolderGuard, KminusCluster};
use crate::runtime::provider::kminus::instance::KminusInstance;
use crate::runtime::provider::outcome::{ClusterStopOutcome, ImagesOutcome, InstanceStopOutcome};
use crate::runtime::provider::{
    Cluster, ClusterProvider, HostedProvider, Instance, LocalProvider, ProcessProvider, Provider,
    Transaction, handle_phase_secrets,
};
use crate::runtime::secrets::settler::auto_fill_generated_secrets;
use crate::runtime::secrets::source::to_secret_name;
use crate::runtime::secrets::{KubeconfigSecretGenerator, SecretGenerator, SecretPhase};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbManagerCommand {
    Start,
    Create,
    Stop,
}

impl DbManagerCommand {
    fn args(self) -> [&'static str; 2] {
        match self {
            DbManagerCommand::Start => ["server", "start"],
            DbManagerCommand::Create => ["db", "create"],
            DbManagerCommand::Stop => ["server", "stop"],
        }
    }

    fn step_labels(self) -> (&'static str, &'static str) {
        match self {
            DbManagerCommand::Start => (
                "Starting tabsdata database server",
                "Tabsdata database server started",
            ),
            DbManagerCommand::Create => (
                "Creating tabsdata database schema",
                "Tabsdata database schema created",
            ),
            DbManagerCommand::Stop => (
                "Stopping tabsdata database server",
                "Tabsdata database server stopped",
            ),
        }
    }
}

const DBMANAGER_BINARY: &str = "td_dbmanager";
const PYVENV_BINARY: &str = "td_pyvenv";
const POSTMASTER_PID_FILE: &str = "postmaster.pid";
const DATABASE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const APISERVER_SERVICE_CONFIG_KEY: &str = "apiserver.service.yaml";
const APISERVER_DATABASE_CONFIG_KEY: &str = "apiserver.database.yaml";
const APISERVER_STORAGE_CONFIG_KEY: &str = "apiserver.storage.yaml";
const DBMANAGER_DATABASE_CONFIG_KEY: &str = "dbmanager.database.yaml";
const APISERVER_CONFIG_KEYS: &[&str] = &[
    APISERVER_SERVICE_CONFIG_KEY,
    APISERVER_DATABASE_CONFIG_KEY,
    APISERVER_STORAGE_CONFIG_KEY,
    DBMANAGER_DATABASE_CONFIG_KEY,
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
        portstart: Option<u16>,
        loglevel: Option<LogLevel>,
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
            portstart,
            loglevel,
        );
        let kminus_instance = KminusInstance::new(
            kminus_cluster.kubefile(),
            kminus_cluster.staging_path(),
            folder.clone(),
        );

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
            &self.folder.join(FOLDER_RUNTIMES),
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
        let namespace = resolve_namespace(Some(&kubefile), Some(&context))
            .unwrap_or_else(|| DEFAULT_NAMESPACE.to_owned());
        KubeSummary {
            kubefile,
            context,
            cluster,
            cluster_url,
            namespace,
        }
    }

    fn ensure_ports_available(&self) -> CommandResult {
        let settings = InstanceSettings::load(&self.name)?;
        ensure_ports_free(
            &settings.ports,
            CHECK_ALL_PORTS_FILTER,
            PORT_RELEASE_TIMEOUT,
        )?;
        Ok(ensure_ports_unclaimed_on_host(&settings.ports)?)
    }

    fn clear_stale_database_pid(&self) {
        let pid_file = self
            .folder
            .join(FOLDER_REPOSITORY)
            .join(FOLDER_DATABASE)
            .join(POSTMASTER_PID_FILE);
        if !pid_file.is_file() {
            return;
        }
        if let Ok(settings) = InstanceSettings::load(&self.name)
            && let Some(port) = database_port(&settings.ports)
        {
            let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
            if TcpStream::connect_timeout(&address, DATABASE_PROBE_TIMEOUT).is_ok() {
                return;
            }
        }
        match fs::remove_file(&pid_file) {
            Ok(()) => warn!("Removed stale database pid file at {}.", pid_file.display()),
            Err(error) => warn!(
                "Could not remove stale database pid file at {}: {error}.",
                pid_file.display()
            ),
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
        system_values_for(&self.folder)
    }

    fn secret_generators(&self, phase: SecretPhase) -> Vec<Box<dyn SecretGenerator>> {
        match phase {
            SecretPhase::ClusterStart => vec![Box::new(KubeconfigSecretGenerator::new(
                self.cluster.kubefile(),
            ))],
            SecretPhase::InstanceStart => Vec::new(),
        }
    }

    fn view(&self) -> InstanceView {
        let kube = self.kube_summary();
        InstanceView {
            instance_version: None,
            instance_edition: None,
            provider: ClusterProvider::Kminus,
            instance_name: Some(self.name.clone()),
            instance_path: Some(self.folder.clone()),
            kubeconfig_path: Some(kube.kubefile),
            context: Some(kube.context),
            cluster_url: Some(kube.cluster_url),
            cluster_name: Some(kube.cluster),
            namespace: Some(kube.namespace),
            loglevel: Some(self.cluster.loglevel()),
        }
    }

    fn cluster_settings(&self, _provided: ClusterProperties) -> CommandResult<ClusterSettings> {
        Ok(ClusterSettings {
            kubeconfig: Some(self.cluster.kubefile()),
            context: None,
            namespace: Some(DEFAULT_NAMESPACE.to_owned()),
        })
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

    fn refresh_cluster_views(&self, current: ClusterViews) -> CommandResult<ClusterViews> {
        let kube = self.kube_summary();
        let adjusted = ClusterSettings {
            kubeconfig: Some(kube.kubefile.clone()),
            context: Some(kube.context.clone()),
            namespace: Some(kube.namespace.clone()),
        };
        let resolved = ClusterDescriptor {
            kubeconfig: Some(kube.kubefile),
            context: Some(kube.context),
            namespace: Some(kube.namespace),
            cluster: Some(kube.cluster),
            url: Some(kube.cluster_url),
        };
        Ok(ClusterViews {
            provided: current.provided,
            adjusted,
            resolved,
        })
    }

    async fn ensure_settable_server(&self) -> CommandResult {
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
        let from_stage = init_stage_folder_in(&self.folder).is_dir();
        if !from_stage {
            let step = reporter.step("Staging tabsdata configmap");
            self.stage_configmaps().await?;
            step.done("Tabsdata configmap staged");
        }
        self.start_cluster_hook(reporter).await?;
        if from_stage {
            let step = reporter.step("Loading tabsdata configmaps");
            self.instance.load_stage_to_kube().await?;
            step.done("Loaded tabsdata configmaps");
        }
        Ok(result)
    }

    async fn create_database(&self, reporter: &dyn Reporter) -> CommandResult {
        self.create_database_as_pod(reporter, DbManagerMode::Create)
            .await
    }

    async fn upgrade_database(&self, reporter: &dyn Reporter) -> CommandResult {
        self.create_database_as_pod(reporter, DbManagerMode::Upgrade)
            .await
    }

    async fn create_runtimes(&self, reporter: &dyn Reporter) -> CommandResult {
        match UvMode::from_environment().map_err(TdError::from)? {
            UvMode::Linked => self.create_runtimes_as_proc(reporter).await,
            UvMode::Local => self.create_runtimes_as_proc(reporter).await,
            UvMode::Devpi => self.create_runtimes_as_pod(reporter).await,
            UvMode::Pypi => self.create_runtimes_as_pod(reporter).await,
            UvMode::Unset => self.create_runtimes_as_pod(reporter).await,
        }
    }

    async fn create_images(&self, reporter: &dyn Reporter) -> CommandResult<ImagesOutcome> {
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
            return Ok(ImagesOutcome::Created);
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
        Ok(ImagesOutcome::Created)
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

    fn manages_cluster_lifecycle(&self) -> bool {
        true
    }

    async fn start_cluster_hook(&self, reporter: &dyn Reporter) -> CommandResult {
        self.ensure_ports_available()?;
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
        if !stopped {
            step.fail();
            return Ok(ClusterStopOutcome::AlreadyStopped);
        }
        if let Ok(settings) = InstanceSettings::load(&self.name)
            && wait_for_ports_released(&settings.ports).is_some()
        {
            kill_port_holders(&settings.ports);
            if let Some(entry) =
                wait_for_ports_released_within(&settings.ports, PORT_RELEASE_AFTER_KILL_TIMEOUT)
            {
                step.fail();
                return Err(ProviderError::PortReleaseTimeout {
                    port: entry.port,
                    caption: entry.caption.clone(),
                }
                .into());
            }
        }
        self.clear_stale_database_pid();
        step.done("Kminus cluster stopped");
        Ok(ClusterStopOutcome::Stopped)
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

    async fn stop_instance(&self, _reporter: &dyn Reporter) -> CommandResult<InstanceStopOutcome> {
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        let stopped = self.teardown_workload(&multi).await?;
        if stopped {
            Ok(InstanceStopOutcome::Stopped)
        } else {
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
        if self.cluster_alive().await {
            let context = self.kube_context().await?;
            let instance = self.view().instance_name.unwrap_or_default();
            ensure_upgraded(&context, &instance).await?;
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
            (&KPROFILE_ASSETS_FAMILY, rendered.assets),
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
        let assets_path = staging_profile.join(KPROFILE_ASSETS_FAMILY.folder);
        for foreign in FOREIGN_ASSETS {
            let source = resolve_foreign_path(*foreign, false)?;
            let content = fs::read_to_string(&source).map_err(|error| {
                ProviderError::StagingReadFailed(source.display().to_string(), error.to_string())
            })?;
            let file_path = assets_path.join(foreign.asset());
            fs::write(&file_path, content.as_bytes()).map_err(|error| {
                ProviderError::StagingWriteFailed(
                    file_path.display().to_string(),
                    error.to_string(),
                )
            })?;
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

    async fn database_context(&self) -> CommandResult<(KubeContext, SystemValues, Profile)> {
        let context = self.kube_context().await?;
        let pods = read_family_configmaps(&context, &KPROFILE_PODS_FAMILY).await?;
        let assets = read_family_configmaps(&context, &KPROFILE_ASSETS_FAMILY).await?;
        let configs = read_family_configmaps(&context, &KPROFILE_CONFIGS_FAMILY).await?;
        let mut manifest_data = require_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await?;
        let Some(manifest_yaml) = manifest_data.remove(MANIFEST_FILE) else {
            return Err(ProviderError::ConfigmapEntryMissing(
                CONFIGMAP_TABSDATA_MANIFEST.to_owned(),
                MANIFEST_FILE.to_owned(),
            )
            .into());
        };
        let profile = parse_from_configmaps(&pods, &assets, &configs, &manifest_yaml)?;

        let mut system = self.system_values()?;
        let settings = InstanceSettings::load(&self.name)?;
        apply_ports(&mut system, &settings.ports);

        Ok((context, system, profile))
    }

    async fn create_database_as_pod(
        &self,
        _reporter: &dyn Reporter,
        mode: DbManagerMode,
    ) -> CommandResult {
        let (context, system, profile) = self.database_context().await?;
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        provision_database(
            &context,
            ClusterProvider::Kminus,
            &system,
            &profile,
            &multi,
            mode,
        )
        .await
    }

    #[allow(dead_code)]
    async fn start_database_as_pod(&self) -> CommandResult {
        let (context, system, profile) = self.database_context().await?;
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        start_database(&context, ClusterProvider::Kminus, &system, &profile, &multi).await
    }

    #[allow(dead_code)]
    async fn stop_database_as_pod(&self) -> CommandResult {
        let (context, _system, profile) = self.database_context().await?;
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        stop_database(&context, &profile, &multi).await
    }

    async fn create_runtimes_as_pod(&self, _reporter: &dyn Reporter) -> CommandResult {
        let (context, system, profile) = self.database_context().await?;
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        provision_pyvenvs(&context, ClusterProvider::Kminus, &system, &profile, &multi).await
    }

    async fn create_runtimes_as_proc(&self, reporter: &dyn Reporter) -> CommandResult {
        let runtimes_folder = self.folder.join(FOLDER_RUNTIMES);
        let runtimes_kenvs = runtimes_folder.join(FOLDER_UV).join(FOLDER_KENVS);
        fs::create_dir_all(&runtimes_kenvs).map_err(|error| {
            ProviderError::RuntimesFolderCreateFailed(
                runtimes_kenvs.display().to_string(),
                error.to_string(),
            )
        })?;
        let _guard = claim_runtimes(&runtimes_folder)?;
        let pyvenv = resolve_python_binary(PYVENV_BINARY).map_err(|error| {
            ProviderError::BinaryNotFound(PYVENV_BINARY.to_owned(), error.to_string())
        })?;
        self.verify_host_environment(&pyvenv, reporter)?;
        match UvMode::from_environment().map_err(TdError::from)? {
            UvMode::Linked => self.link_runtimes(&pyvenv, &runtimes_kenvs, reporter),
            UvMode::Local | UvMode::Devpi | UvMode::Pypi | UvMode::Unset => {
                let context = self.kube_context().await?;
                let assets = read_family_configmaps(&context, &KPROFILE_ASSETS_FAMILY).await?;
                let folder = tempfile::tempdir().map_err(|error| {
                    ProviderError::RuntimesFolderCreateFailed(
                        std::env::temp_dir().display().to_string(),
                        error.to_string(),
                    )
                })?;
                let requirements = write_requirements(folder.path(), &assets)?;
                self.build_runtimes(
                    &pyvenv,
                    &runtimes_folder,
                    &runtimes_kenvs,
                    &requirements,
                    reporter,
                )
            }
        }
    }

    fn verify_host_environment(&self, pyvenv: &Path, reporter: &dyn Reporter) -> CommandResult {
        let step = reporter.step("Verifying the host Python environment");
        let mut command = std::process::Command::new(pyvenv);
        if self.verbose {
            command.arg("--verbose");
        }
        command.arg("verify");
        apply_detached_flags(&mut command);
        let output = match subprocess::run(&mut command, PYVENV_BINARY, self.verbose) {
            Ok(output) => output,
            Err(error) => {
                step.fail();
                return Err(ProviderError::HostEnvironmentResolveFailed {
                    cause: error.to_string(),
                }
                .into());
            }
        };
        if !output.status.success() {
            step.fail();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            let cause = if stderr.is_empty() { stdout } else { stderr };
            return Err(ProviderError::HostEnvironmentIncomplete { cause }.into());
        }
        step.done("Host Python environment verified");
        Ok(())
    }

    fn link_runtimes(
        &self,
        pyvenv: &Path,
        runtimes_kenvs: &Path,
        reporter: &dyn Reporter,
    ) -> CommandResult {
        let step = reporter.step("Linking the Python virtual environments");
        let target = match self.host_environment_root(pyvenv) {
            Ok(root) => root,
            Err(error) => {
                step.fail();
                return Err(error);
            }
        };
        for venv in VENVS {
            let link = runtimes_kenvs.join(venv.as_str());
            if let Err(error) = relink(&link, &target) {
                step.fail();
                return Err(ProviderError::PyvenvLinkFailed {
                    link: link.display().to_string(),
                    target: target.display().to_string(),
                    cause: error,
                }
                .into());
            }
            info!(
                "Linked the '{}' virtual environment to {}",
                venv.as_str(),
                target.display()
            );
        }
        step.done("Python virtual environments linked");
        self.compile_runtimes(pyvenv, &target, reporter);
        Ok(())
    }

    fn compile_runtimes(&self, pyvenv: &Path, target: &Path, reporter: &dyn Reporter) {
        let step = reporter.step("Compiling the Python virtual environments");
        let mut command = std::process::Command::new(pyvenv);
        if self.verbose {
            command.arg("--verbose");
        }
        command.arg("compile").arg("--target").arg(target);
        apply_detached_flags(&mut command);
        match subprocess::run(&mut command, PYVENV_BINARY, self.verbose) {
            Ok(output) if output.status.success() => {
                step.done("Python virtual environments compiled");
                info!(
                    "Compiled the Python virtual environment {}",
                    target.display()
                );
            }
            Ok(output) => {
                step.done("Python virtual environments not compiled");
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
                warn!(
                    "Precompiling {} failed ({stderr}); skipping",
                    target.display()
                );
            }
            Err(error) => {
                step.done("Python virtual environments not compiled");
                warn!(
                    "Precompiling {} failed ({error}); skipping",
                    target.display()
                );
            }
        }
    }

    fn host_environment_root(&self, pyvenv: &Path) -> CommandResult<PathBuf> {
        let mut command = std::process::Command::new(pyvenv);
        command.arg("root");
        apply_detached_flags(&mut command);
        let output =
            subprocess::run(&mut command, PYVENV_BINARY, self.verbose).map_err(|error| {
                ProviderError::HostEnvironmentResolveFailed {
                    cause: error.to_string(),
                }
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            return Err(ProviderError::HostEnvironmentResolveFailed { cause: stderr }.into());
        }
        let root = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if root.is_empty() {
            return Err(ProviderError::HostEnvironmentResolveFailed {
                cause: format!("'{PYVENV_BINARY} root' printed no path"),
            }
            .into());
        }
        Ok(PathBuf::from(root))
    }

    fn build_runtimes(
        &self,
        pyvenv: &Path,
        runtimes_folder: &Path,
        runtimes_kenvs: &Path,
        requirements: &[(Venv, PathBuf)],
        reporter: &dyn Reporter,
    ) -> CommandResult {
        ensure_no_linked_venvs(runtimes_kenvs)?;
        let runtimes_python = runtimes_folder.join(FOLDER_UV).join(FOLDER_PYTHON);
        fs::create_dir_all(&runtimes_python).map_err(|error| {
            ProviderError::RuntimesFolderCreateFailed(
                runtimes_python.display().to_string(),
                error.to_string(),
            )
        })?;
        let step = reporter.step("Creating Python virtual environment templates");
        let mut command = std::process::Command::new(pyvenv);
        if self.verbose {
            command.arg("--verbose");
        }
        command.arg("create").arg("--target").arg(runtimes_kenvs);
        for (venv, file) in requirements {
            command
                .arg(format!("--requirements-{}", venv.as_str()))
                .arg(file);
        }
        command.env(ENV_UV_PYTHON_INSTALL_DIR, &runtimes_python);
        apply_detached_flags(&mut command);
        let output = match subprocess::run(&mut command, PYVENV_BINARY, self.verbose) {
            Ok(output) => output,
            Err(error) => {
                step.fail();
                return Err(ProviderError::PyvenvCreateFailed {
                    folder: runtimes_kenvs.display().to_string(),
                    cause: error.to_string(),
                }
                .into());
            }
        };
        if !output.status.success() {
            step.fail();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            return Err(ProviderError::PyvenvCreateFailed {
                folder: runtimes_kenvs.display().to_string(),
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
        let uv_home = runtimes_folder.join(FOLDER_UV);
        let image = runtimes_folder.join(VENVS_IMAGE);
        let step = reporter.step("Compressing the Python virtual environments");
        match write_venvs_image(&uv_home, &image) {
            Ok(()) => step.done("Python virtual environments compressed"),
            Err(error) => {
                step.fail();
                return Err(error.into());
            }
        }
        info!(
            "Python virtual environments image written to {}",
            image.display()
        );
        Ok(())
    }

    #[allow(dead_code)]
    async fn create_database_as_proc(&self, reporter: &dyn Reporter) -> CommandResult {
        let (config_files, secret_env) = self.prepare_dbmanager().await?;
        self.execute_dbmanager(
            DbManagerCommand::Start,
            &config_files,
            &secret_env,
            reporter,
        )?;
        self.execute_dbmanager(
            DbManagerCommand::Create,
            &config_files,
            &secret_env,
            reporter,
        )?;
        self.execute_dbmanager(DbManagerCommand::Stop, &config_files, &secret_env, reporter)?;
        Ok(())
    }

    async fn prepare_dbmanager(
        &self,
    ) -> CommandResult<(Vec<tempfile::NamedTempFile>, Vec<(String, String)>)> {
        let stage_configs = init_stage_folder_in(&self.folder).join(KPROFILE_CONFIGS_FAMILY.folder);
        let context = self.kube_context().await?;
        auto_fill_generated_secrets(&context).await?;

        let mut config_files: Vec<tempfile::NamedTempFile> = Vec::new();
        let mut secret_env: Vec<(String, String)> = Vec::new();
        for config_key in APISERVER_CONFIG_KEYS {
            let stage_file = stage_configs.join(config_key);
            let config = fs::read_to_string(&stage_file).map_err(|error| {
                ProviderError::StagingReadFailed(
                    stage_file.display().to_string(),
                    error.to_string(),
                )
            })?;
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
        Ok((config_files, secret_env))
    }

    fn execute_dbmanager(
        &self,
        subcommand: DbManagerCommand,
        config_files: &[tempfile::NamedTempFile],
        secret_env: &[(String, String)],
        reporter: &dyn Reporter,
    ) -> CommandResult {
        let (step_label, done_label) = subcommand.step_labels();
        let dbmanager = resolve_python_binary(DBMANAGER_BINARY).map_err(|error| {
            ProviderError::BinaryNotFound(DBMANAGER_BINARY.to_owned(), error.to_string())
        })?;
        let command_label = format!("{} {}", dbmanager.display(), subcommand.args().join(" "));
        let step = reporter.step(step_label);
        let mut command = std::process::Command::new(&dbmanager);
        command.args(subcommand.args());
        for config_file in config_files {
            command.arg("--config-file").arg(config_file.path());
        }
        for (key, value) in secret_env {
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
        step.done(done_label);
        Ok(())
    }
}

fn ensure_no_linked_venvs(runtimes_kenvs: &Path) -> CommandResult {
    for venv in VENVS {
        if is_symlink(&runtimes_kenvs.join(venv.as_str())) {
            return Err(ProviderError::RuntimesLinkedNotRebuildable {
                folder: runtimes_kenvs.display().to_string(),
                variable: ENV_TD_KUBE_UV_MODE.to_owned(),
                linked: UV_MODE_LINKED.to_owned(),
                local: UV_MODE_LOCAL.to_owned(),
            }
            .into());
        }
    }
    Ok(())
}

fn write_requirements(
    folder: &Path,
    assets: &RenderedConfigmap,
) -> CommandResult<Vec<(Venv, PathBuf)>> {
    let mut requirements = Vec::new();
    for venv in VENVS {
        let asset = venv.requirements_asset();
        let Some(contents) = assets.get(asset) else {
            continue;
        };
        let file = folder.join(asset);
        fs::write(&file, contents).map_err(|error| {
            ProviderError::RuntimesRequirementsWriteFailed {
                file: file.display().to_string(),
                cause: error.to_string(),
            }
        })?;
        requirements.push((*venv, file));
    }
    Ok(requirements)
}

fn claim_runtimes(runtimes_folder: &std::path::Path) -> CommandResult<LockFile> {
    let lock_path = runtimes_folder.join(RUNTIMES_LOCK_FILE);
    let lock_failed = |error: fslock::Error| ProviderError::RuntimesLockFailed {
        folder: runtimes_folder.display().to_string(),
        cause: error.to_string(),
    };
    let mut lock = LockFile::open(&lock_path).map_err(lock_failed)?;
    if !lock.try_lock_with_pid().map_err(lock_failed)? {
        let holder = match fs::read_to_string(&lock_path) {
            Ok(pid) if !pid.trim().is_empty() => format!(" (process {})", pid.trim()),
            _ => String::new(),
        };
        return Err(ProviderError::RuntimesProvisionInProgress {
            folder: runtimes_folder.display().to_string(),
            holder,
        }
        .into());
    }
    Ok(lock)
}

pub(crate) fn system_values_for(folder: &std::path::Path) -> CommandResult<SystemValues> {
    let repository_folder = folder.join(FOLDER_REPOSITORY);
    let repository_url = url::Url::from_directory_path(&repository_folder)
        .ok()
        .map(|url| url.to_string());
    let repository_path = Some(repository_folder.to_string_lossy().to_string());
    let database_folder = repository_folder.join(FOLDER_DATABASE);
    let database_path = Some(database_folder.to_string_lossy().to_string());
    let storage_folder = repository_folder.join(FOLDER_STORAGE);
    let storage_url = url::Url::from_directory_path(&storage_folder)
        .ok()
        .map(|url| url.to_string());
    let storage_path = Some(storage_folder.to_string_lossy().to_string());
    let runtimes_folder = folder.join(FOLDER_RUNTIMES);
    let runtimes_root = Some(runtimes_folder.to_string_lossy().to_string());
    let kenvs = runtimes_folder.join(FOLDER_UV).join(FOLDER_KENVS);
    let pyenv_td_root = Some(kenvs.join(FOLDER_PYENV_TD).to_string_lossy().to_string());
    let pyenv_ag_root = Some(kenvs.join(FOLDER_PYENV_AG).to_string_lossy().to_string());
    let pyenv_fn_root = Some(kenvs.join(FOLDER_PYENV_FN).to_string_lossy().to_string());
    let scripts_folder = get_python_scripts_folder()
        .map_err(|error| ProviderError::PythonScriptsFolderResolveFailed(error.to_string()))?;
    let Some(scripts_name) = scripts_folder.file_name().and_then(|name| name.to_str()) else {
        return Err(ProviderError::PythonScriptsFolderNameInvalid(
            scripts_folder.display().to_string(),
        )
        .into());
    };
    let pyenv_scripts_folder = Some(scripts_name.to_string());
    let Some(pyenv_base) = scripts_folder.parent() else {
        return Err(ProviderError::PythonScriptsFolderNameInvalid(
            scripts_folder.display().to_string(),
        )
        .into());
    };
    let pyenv_base_root = Some(pyenv_base.to_string_lossy().to_string());
    let pyenv_libexec_folder = Some(LIBEXEC_TABSDATA.to_owned());
    let exe_suffix = Some(EXE_SUFFIX.to_owned());
    let instance_name = folder
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_owned());
    Ok(SystemValues {
        repository_url,
        repository_path,
        database_path,
        storage_url,
        storage_path,
        runtimes_root,
        pyenv_base_root,
        pyenv_td_root,
        pyenv_ag_root,
        pyenv_fn_root,
        pyenv_scripts_folder,
        pyenv_libexec_folder,
        exe_suffix,
        instance_name,
        tabsdata_version: Some(tabsdata_version()?),
        namespace_name: Some(DEFAULT_NAMESPACE.to_owned()),
        database_host: Some(DATABASE_HOST_LOOPBACK.to_owned()),
        database_name: Some(DATABASE_NAME_POSTGRES.to_owned()),
        database_ssl_mode: Some(DATABASE_SSL_MODE_PREFER.to_owned()),
        database_ssl_root_cert: Some(YAML_NULL.to_owned()),
        uv_default_index: Some(SystemValues::uv_default_index(IndexReach::Host)?),
        uv_index: Some(SystemValues::uv_index(IndexReach::Host)?),
        uv_index_strategy: Some(SystemValues::uv_index_strategy()?),
        ..SystemValues::default()
    })
}

#[cfg(test)]
mod tests;
