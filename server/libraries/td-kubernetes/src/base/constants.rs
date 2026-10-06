//
// Copyright 2026 Tabsdata Inc.
//

use std::time::Duration;

use clap_derive::ValueEnum;
use const_format::concatcp;
use serde::{Deserialize, Serialize};

use crate::base::error::cli::CliError;

pub const ENV_KUBECONFIG: &str = "KUBECONFIG";
pub const ENV_TD_KUBE_UV_MODE: &str = "TD_KUBE_UV_MODE";
pub const ENV_TD_KUBE_UV_MODE_NOWARN: &str = "TD_KUBE_UV_MODE_NOWARN";

pub const ENV_UV_DEFAULT_INDEX: &str = "UV_DEFAULT_INDEX";
pub const ENV_UV_INDEX: &str = "UV_INDEX";
pub const ENV_PIP_INDEX_URL: &str = "PIP_INDEX_URL";
pub const ENV_UV_INDEX_STRATEGY: &str = "UV_INDEX_STRATEGY";
pub const ENV_UV_PYTHON_INSTALL_DIR: &str = "UV_PYTHON_INSTALL_DIR";

pub const DEVPI_INDEX_URL_HOST: &str = "http://localhost:14159/tabsdata/td/+simple/";
pub const DEVPI_INDEX_URL_CONTAINER: &str =
    "http://host.docker.internal:14159/tabsdata/td/+simple/";
pub const DEVPI_INDEX_URL_CLUSTER: &str =
    "http://devpi.devpi.svc.cluster.local:14159/tabsdata/td/+simple/";

pub const CONTAINER_HOST_GATEWAY: &str = "host.docker.internal";
const LOOPBACK_HOSTS: [&str; 4] = ["localhost", "127.0.0.1", "::1", "[::1]"];

pub const PYPI_INDEX_URL: &str = "https://pypi.org/simple/";

pub const UV_INDEX_STRATEGY_BEST_MATCH: &str = "unsafe-best-match";
pub const UV_INDEX_STRATEGY_FIRST_INDEX: &str = "first-index";

pub const UV_MODE_LINKED: &str = "linked";
pub const UV_MODE_LOCAL: &str = "local";
pub const UV_MODE_DEVPI: &str = "devpi";
pub const UV_MODE_PYPI: &str = "pypi";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UvMode {
    Linked,
    Local,
    Devpi,
    Pypi,
    Unset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexReach {
    Host,
    HostContainer,
    Cluster,
}

impl IndexReach {
    pub fn propagates_local_settings(self) -> bool {
        matches!(self, Self::Host | Self::HostContainer)
    }
}

impl UvMode {
    pub fn from_environment() -> Result<Self, CliError> {
        let value = std::env::var(ENV_TD_KUBE_UV_MODE).unwrap_or_default();
        match value.trim().to_lowercase().as_str() {
            UV_MODE_LINKED => Ok(Self::Linked),
            UV_MODE_LOCAL => Ok(Self::Local),
            UV_MODE_DEVPI => Ok(Self::Devpi),
            UV_MODE_PYPI => Ok(Self::Pypi),
            "" => Ok(Self::Unset),
            other => Err(CliError::UvModeUnsupported {
                variable: ENV_TD_KUBE_UV_MODE.to_owned(),
                value: other.to_owned(),
                supported: [UV_MODE_LINKED, UV_MODE_LOCAL, UV_MODE_DEVPI, UV_MODE_PYPI].join(", "),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Linked => UV_MODE_LINKED,
            Self::Local => UV_MODE_LOCAL,
            Self::Devpi => UV_MODE_DEVPI,
            Self::Pypi => UV_MODE_PYPI,
            Self::Unset => "",
        }
    }

    pub fn uses_host_environment(self) -> bool {
        match self {
            Self::Linked => true,
            Self::Local => true,
            Self::Devpi => false,
            Self::Pypi => false,
            Self::Unset => false,
        }
    }

    pub fn uv_default_index(self, reach: IndexReach) -> String {
        match self {
            Self::Linked => String::new(),
            Self::Local => String::new(),
            Self::Devpi | Self::Pypi => PYPI_INDEX_URL.to_owned(),
            Self::Unset => {
                if reach.propagates_local_settings() {
                    environment_index(ENV_UV_DEFAULT_INDEX)
                        .unwrap_or_else(|| PYPI_INDEX_URL.to_owned())
                } else {
                    PYPI_INDEX_URL.to_owned()
                }
            }
        }
    }

    pub fn uv_index(self, reach: IndexReach) -> String {
        match self {
            Self::Linked => String::new(),
            Self::Local => String::new(),
            Self::Devpi => match reach {
                IndexReach::Host => DEVPI_INDEX_URL_HOST.to_owned(),
                IndexReach::HostContainer => DEVPI_INDEX_URL_CONTAINER.to_owned(),
                IndexReach::Cluster => DEVPI_INDEX_URL_CLUSTER.to_owned(),
            },
            Self::Pypi => String::new(),
            Self::Unset => match reach {
                IndexReach::Host => environment_index(ENV_UV_INDEX).unwrap_or_default(),
                IndexReach::HostContainer => environment_index(ENV_UV_INDEX)
                    .map(|index| reach_from_container(&index))
                    .unwrap_or_default(),
                IndexReach::Cluster => String::new(),
            },
        }
    }

    pub fn uv_index_strategy(self) -> String {
        UV_INDEX_STRATEGY_BEST_MATCH.to_owned()
    }
}

fn reach_from_container(index: &str) -> String {
    let Ok(mut url) = url::Url::parse(index) else {
        return index.to_owned();
    };
    let Some(host) = url.host_str() else {
        return index.to_owned();
    };
    if !LOOPBACK_HOSTS.contains(&host) {
        return index.to_owned();
    }
    if url.set_host(Some(CONTAINER_HOST_GATEWAY)).is_err() {
        return index.to_owned();
    }
    url.to_string()
}

pub fn environment_index(variable: &str) -> Option<String> {
    std::env::var(variable)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

pub const FOLDER_CLUSTER: &str = "cluster";
pub const FOLDER_REPOSITORY: &str = "repository";
pub const FOLDER_DATABASE: &str = "database";
pub const FOLDER_STORAGE: &str = "storage";
pub const FOLDER_RUNTIMES: &str = "runtimes";
pub const FOLDER_UV: &str = ".uv";
pub const FOLDER_KENVS: &str = "kenvs";
pub const RUNTIMES_LOCK_FILE: &str = ".provision.lock";
pub const VENVS_IMAGE: &str = "tdvenvs.tar.zst";
pub const CONTAINER_RUNTIMES_ROOT: &str = "/tabsdata/runtimes";
pub const CONTAINER_PYENV_TD_ROOT: &str = concatcp!(
    CONTAINER_RUNTIMES_ROOT,
    "/",
    FOLDER_UV,
    "/",
    FOLDER_KENVS,
    "/",
    FOLDER_PYENV_TD
);
pub const CONTAINER_PYENV_AG_ROOT: &str = concatcp!(
    CONTAINER_RUNTIMES_ROOT,
    "/",
    FOLDER_UV,
    "/",
    FOLDER_KENVS,
    "/",
    FOLDER_PYENV_AG
);
pub const CONTAINER_PYENV_FN_ROOT: &str = concatcp!(
    CONTAINER_RUNTIMES_ROOT,
    "/",
    FOLDER_UV,
    "/",
    FOLDER_KENVS,
    "/",
    FOLDER_PYENV_FN
);
pub const FOLDER_IMAGES: &str = "images";
pub const FOLDER_PYTHON: &str = "python";
pub const FOLDER_PYENV_TD: &str = "td";
pub const FOLDER_PYENV_AG: &str = "ag";
pub const FOLDER_PYENV_FN: &str = "fn";
pub const CONTAINER_PYENV_SCRIPTS_FOLDER: &str = "bin";
pub const FOLDER_STAGING: &str = "staging";
pub const FOLDER_PROFILE: &str = "profile";
pub const FOLDER_KUBE: &str = ".kube";
pub const FILE_KUBECONFIG: &str = "config";

pub const LIBEXEC_TABSDATA: &str = "libexec/tabsdata";

pub const RESOURCE_KPROFILE: &str = "resources/kprofile";
pub const HIDDEN_VALUE: &str = "*****";
pub const UNDEFINED_FIELD: &str = "<undefined>";

pub const SECRET_OPTIONS: &[&str] = &["--anthropic-key", "--openai-key"];

pub const RESOURCE_KUPGRADE: &str = "resources/kupgrade";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    System,
    Application,
    Function,
    Connection,
}

impl Scope {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Scope::System => "sys",
            Scope::Application => "app",
            Scope::Function => "fn",
            Scope::Connection => "conn",
        }
    }
}

pub struct Family {
    pub folder: &'static str,
    pub scope: Scope,
    pub kind: &'static str,
}

pub const SCHEMA_EXTENSION: &str = ".yamls";
pub const SCHEMA_SOURCE_EXTENSION: &str = ".schema.yaml";

pub const KPROFILE_PODS_FAMILY: Family = Family {
    folder: "pods",
    scope: Scope::System,
    kind: "pod",
};
pub const KPROFILE_CONFIGS_FAMILY: Family = Family {
    folder: "configs",
    scope: Scope::Application,
    kind: "config",
};
pub const KPROFILE_ASSETS_FAMILY: Family = Family {
    folder: "assets",
    scope: Scope::Application,
    kind: "asset",
};

pub const REQUIREMENTS_TD_ASSET: &str = "requirements-td.txt";
pub const REQUIREMENTS_FN_ASSET: &str = "requirements-fn.txt";
#[cfg(feature = "enterprise")]
pub const REQUIREMENTS_AG_ASSET: &str = "requirements-ag.txt";
pub const TABSDATA_REQUIREMENTS_LOCK_ASSET: &str = "tabsdata-requirements.txt";

pub const VENV_TD: &str = "td";
pub const VENV_FN: &str = "fn";
#[cfg(feature = "enterprise")]
pub const VENV_AG: &str = "ag";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "lower")]
pub enum Venv {
    Td,
    Fn,
    #[cfg(feature = "enterprise")]
    Ag,
}

impl Venv {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Td => VENV_TD,
            Self::Fn => VENV_FN,
            #[cfg(feature = "enterprise")]
            Self::Ag => VENV_AG,
        }
    }

    pub const fn requirements_asset(self) -> &'static str {
        match self {
            Self::Td => REQUIREMENTS_TD_ASSET,
            Self::Fn => REQUIREMENTS_FN_ASSET,
            #[cfg(feature = "enterprise")]
            Self::Ag => REQUIREMENTS_AG_ASSET,
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::Td => "Virtual environment for internal tasks of pods & jobs",
            Self::Fn => "Virtual environment to run user functions",
            #[cfg(feature = "enterprise")]
            Self::Ag => "Virtual environment to run tabsdata ai tasks",
        }
    }
}

pub const VENVS: &[Venv] = &[
    Venv::Td,
    Venv::Fn,
    #[cfg(feature = "enterprise")]
    Venv::Ag,
];

pub const RESOURCE_TABSDATA_REQUIREMENTS_LOCK: &str =
    concatcp!("resources/lock/", TABSDATA_REQUIREMENTS_LOCK_ASSET);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignAssetOrigin {
    TabsdataResource(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignAsset {
    TabsdataRequirementLock,
}

impl ForeignAsset {
    pub const fn asset(self) -> &'static str {
        match self {
            Self::TabsdataRequirementLock => TABSDATA_REQUIREMENTS_LOCK_ASSET,
        }
    }

    pub const fn origin(self) -> ForeignAssetOrigin {
        match self {
            Self::TabsdataRequirementLock => {
                ForeignAssetOrigin::TabsdataResource(RESOURCE_TABSDATA_REQUIREMENTS_LOCK)
            }
        }
    }
}

pub const FOREIGN_ASSETS: &[ForeignAsset] = &[ForeignAsset::TabsdataRequirementLock];

#[cfg(not(feature = "enterprise"))]
pub const CUSTOMIZABLE_ASSETS: &[&str] = &[
    REQUIREMENTS_TD_ASSET,
    REQUIREMENTS_FN_ASSET,
    TABSDATA_REQUIREMENTS_LOCK_ASSET,
];

#[cfg(feature = "enterprise")]
pub const CUSTOMIZABLE_ASSETS: &[&str] = &[
    REQUIREMENTS_TD_ASSET,
    REQUIREMENTS_FN_ASSET,
    REQUIREMENTS_AG_ASSET,
    TABSDATA_REQUIREMENTS_LOCK_ASSET,
];

pub const SERVICE_TYPE_LOAD_BALANCER: &str = "LoadBalancer";

pub const KIND_SECRET: &str = "secret";
pub const KIND_SECRET_CONFIG: &str = "secret-config";
pub const KIND_MANIFEST: &str = "manifest";

pub const SINGLETON_ID_TABSDATA: &str = "tabsdata";
pub const SINGLETON_NAME_MANIFEST: &str = "manifest.yaml";

pub const INIT_FOLDER: &str = "init";
pub const EDIT_FOLDER: &str = "edit";
pub const INPUT_FOLDER: &str = "input";
pub const STAGE_FOLDER: &str = "stage";
pub const TRASH_FOLDER: &str = "trash";
pub const LOG_FOLDER: &str = "log";
pub const TDKSERVER_LOG_FILE: &str = "tdkserver.log";
pub const KM_IO_LOG_PREFIX: &str = "km.io";
pub const KM_IO_LOG_STAMP: &str = "%Y%m%dT%H%M%S";
pub const KM_IO_CAPTURE_TIMEOUT: Duration = Duration::from_secs(30);
pub const KM_IO_CAPTURE_QUIET: Duration = Duration::from_secs(15);
pub const KM_IO_CAPTURE_POLL: Duration = Duration::from_millis(200);
pub const KM_STARTUP_MARKER_PREFIX: &str = "K- startup complete:";
pub const LOG_SIZE_WARNING: u64 = 128 * 1024 * 1024;
pub const LOG_COUNT_WARNING: usize = 128;
pub const VERBOSE_FLAG: &str = "--verbose";
pub const PROMPT_REMINDER: Duration = Duration::from_secs(60);
pub const WORKLOAD_LOG_TIMEOUT: Duration = Duration::from_secs(30);
pub const WORKLOAD_LOG_WAIT: Duration = Duration::from_secs(10);
pub const SETTINGS_FILE: &str = ".settings.json";
pub const VAULT_FILE: &str = ".vault.json";
pub const CLUSTER_FOLDER: &str = "cluster";
pub const KUBE_FOLDER: &str = "kube";
pub const CLUSTER_CONFIG_FOLDER: &str = "config";
pub const KUBECONFIG_FILE: &str = "kubeconfig.yaml";
pub const DEFAULT_NAMESPACE: &str = "default";
pub const VENV_ACTIVATE_FILE: &str = "activate";

pub const PROGRAM_TD_RESOURCE: &str = "td_resource";
pub const PROGRAM_TDK: &str = "tdk";

pub const DEFAULT_INSTANCE_NAME: &str = "tabsdata";

pub const DATABASE_HOST_LOOPBACK: &str = "127.0.0.1";
pub const DATABASE_NAME_POSTGRES: &str = "postgres";
pub const YAML_NULL: &str = "null";
pub const DATABASE_SSL_MODE_PREFER: &str = "prefer";
pub const DATABASE_SSL_MODE_VERIFY_CA: &str = "verify-ca";
pub const CONTAINER_CERTS_ROOT: &str = "/tabsdata/certs";

pub const DATABASE_CA_BUNDLE_FILE_AWS: &str = "ca-aws.pem";
pub const DATABASE_CA_BUNDLE_FILE_AZURE: &str = "ca-azure.pem";
pub const DATABASE_CA_BUNDLE_URL_AWS: &str =
    "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem";
pub const DATABASE_CA_BUNDLE_URL_AZURE: &str =
    "https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem";
pub const DATABASE_SSL_ROOT_CERT_AWS: &str =
    concatcp!(CONTAINER_CERTS_ROOT, "/", DATABASE_CA_BUNDLE_FILE_AWS);
pub const DATABASE_SSL_ROOT_CERT_AZURE: &str =
    concatcp!(CONTAINER_CERTS_ROOT, "/", DATABASE_CA_BUNDLE_FILE_AZURE);

pub const TABSDATA_LABEL_NAMESPACE: &str = "tabsdata.com/";
pub const KUBERNETES_LABEL_NAMESPACE: &str = "app.kubernetes.io/";

pub const TIER_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "tier");
#[cfg(feature = "enterprise")]
pub const AIAGENT_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "aiagent");
pub const SEQUENCE_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "sequence");
pub const CONFIGMAP_FAMILY_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "configmap-family");
pub const SECRET_FAMILY_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "secret-family");
pub const SECRET_FAMILY_CONFIG: &str = "secret-config";

pub const SECRET_GENERATOR_JWT_SIGNING_KEY: &str = "jwt_signing_key";
pub const SECRET_GENERATOR_POSTGRES_SYSTEM_USER: &str = "postgres_system_user";
pub const SECRET_GENERATOR_POSTGRES_SYSTEM_PASSWORD: &str = "postgres_system_password";
pub const SECRET_GENERATOR_POSTGRES_APISERVER_PASSWORD: &str = "postgres_apiserver_password";
#[cfg(feature = "enterprise")]
pub const SECRET_GENERATOR_POSTGRES_AIAGENT_PASSWORD: &str = "postgres_aiagent_password";
pub const SECRET_GENERATOR_SERVICE_TOKEN: &str = "service_token";

pub const PART_OF_LABEL: &str = concatcp!(KUBERNETES_LABEL_NAMESPACE, "part-of");
pub const PART_OF_TABSDATA: &str = "tabsdata";
pub const TD_VERSION_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "version");
pub const LEGACY_VERSION: &str = "2.0.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbManagerMode {
    Create,
    Upgrade,
}

impl DbManagerMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Upgrade => "upgrade",
        }
    }
}

pub const POD_YAML_COMPONENT_LABEL: &str = concatcp!(KUBERNETES_LABEL_NAMESPACE, "component");

pub const RESOURCE_CPU: &str = "cpu";
pub const RESOURCE_MEMORY: &str = "memory";
pub const RESOURCE_EPHEMERAL_STORAGE: &str = "ephemeral-storage";
pub const POD_YAML_DECORATOR_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "decorator");
pub const POD_YAML_WORKER_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "worker");

pub const DECORATOR_PUBLISHER: &str = "publisher";
pub const DECORATOR_SUBSCRIBER: &str = "subscriber";
pub const DECORATOR_TRANSFORMER: &str = "transformer";
pub const DECORATOR_CRON_PUBLISHER: &str = "cron_publisher";
pub const DECORATOR_STREAM_PUBLISHER: &str = "stream_publisher";

pub const TABSDATA_NAME_PREFIX: &str = "td-";

pub const CONFIGMAP_TABSDATA_MANIFEST: &str = "td-sys-tabsdata-manifest-manifest.yaml";
pub const CONFIGMAP_TABSDATA_SETTINGS: &str = "td-sys-tabsdata-settings-settings.json";
pub const CONTAINER_ROOT_ENV: &str = "CONTAINER_ROOT";
pub const CONTAINER_TABSDATA_ROOT: &str = "/tabsdata";
pub const CONTAINER_SETTINGS_RELATIVE: &str = "tabsdata/settings/.settings.json";

pub const SECRET_CATEGORY_ANTHROPIC_KEY: &str = "anthropic-key";
pub const SECRET_CATEGORY_OPENAI_KEY: &str = "openai-key";

#[cfg(feature = "enterprise")]
pub const AIAGENT_CONFIG_FILE: &str = "aiagent.yaml";
#[cfg(feature = "enterprise")]
pub const AIAGENT_SECRET_KEY_ANTHROPIC: &str = "AI_PROVIDERS__ANTHROPIC__API_KEY";
#[cfg(feature = "enterprise")]
pub const AIAGENT_SECRET_KEY_OPENAI: &str = "AI_PROVIDERS__OPENAI__API_KEY";
pub const SECRET_TABSDATA_KUBECONFIG: &str = "td-sys-tabsdata-secret-kubeconfig";
pub const SECRET_KEY_KUBECONFIG: &str = "kubeconfig";

pub const KUBE_NAME_SEPARATOR: &str = "-";
pub const KUBE_METADATA_FIELD_NAME: &str = "name";
pub const KUBE_METADATA_FIELD_UID: &str = "uid";
pub const KUBE_SPEC_FIELD: &str = "spec";
pub const KUBE_SPEC_FIELD_SUSPEND: &str = "suspend";
pub const KUBE_SPEC_FIELD_PAUSED: &str = "paused";

pub const HTTP_STATUS_FORBIDDEN: u16 = 403;
pub const HTTP_STATUS_NOT_FOUND: u16 = 404;
pub const HTTP_STATUS_CONFLICT: u16 = 409;

pub const SCHEDULER_FARGATE: &str = "fargate-scheduler";

pub const PROBE_POD: &str = "td-scheduler-probe";
pub const PROBE_IMAGE: &str = "busybox:1.37";
pub const PROBE_CPU: &str = "250m";
pub const PROBE_MEMORY: &str = "512Mi";

pub const POD_YAML_FIELD_KIND: &str = "kind";
pub const POD_YAML_FIELD_METADATA: &str = "metadata";
pub const POD_YAML_FIELD_LABELS: &str = "labels";
pub const POD_YAML_FIELD_API_VERSION: &str = "apiVersion";
pub const POD_YAML_FIELD_NAMESPACE: &str = "namespace";
pub const POD_YAML_FIELD_SPEC: &str = "spec";
pub const POD_YAML_FIELD_RESTART_POLICY: &str = "restartPolicy";
pub const POD_YAML_FIELD_CONTAINERS: &str = "containers";
pub const POD_YAML_FIELD_IMAGE: &str = "image";
pub const POD_YAML_FIELD_COMMAND: &str = "command";
pub const POD_YAML_FIELD_RESOURCES: &str = "resources";
pub const POD_YAML_FIELD_REQUESTS: &str = "requests";
pub const POD_YAML_FIELD_CPU: &str = "cpu";
pub const POD_YAML_FIELD_MEMORY: &str = "memory";

pub const POD_API_VERSION_V1: &str = "v1";
pub const POD_KIND: &str = "Pod";
pub const POD_RESTART_POLICY_NEVER: &str = "Never";
pub const PROBE_CONTAINER: &str = "probe";
pub const PROBE_COMMAND: &str = "true";

pub const VALUE_Y: &str = "y";
pub const VALUE_YES: &str = "yes";
pub const VALUE_N: &str = "n";
pub const VALUE_NO: &str = "no";
pub const VALUE_CONTINUE: &str = "continue";
pub const VALUE_CANCEL: &str = "cancel";
pub const VALUE_CHANGE: &str = "change";

pub const DELETE_WORKLOAD_WAIT_TIMEOUT: Duration = Duration::from_secs(300);
pub const DELETE_WORKLOAD_POLL_INTERVAL: Duration = Duration::from_millis(250);

pub const POD_PHASE_PENDING: &str = "Pending";
pub const POD_PHASE_RUNNING: &str = "Running";
pub const POD_PHASE_SUCCEEDED: &str = "Succeeded";
pub const POD_PHASE_FAILED: &str = "Failed";

pub const JOB_CONDITION_COMPLETE: &str = "Complete";
pub const JOB_CONDITION_FAILED: &str = "Failed";
pub const JOB_CONDITION_STATUS_TRUE: &str = "True";
pub const POD_CONDITION_READY: &str = "Ready";
pub const POD_CONDITION_STATUS_TRUE: &str = "True";

pub const KUBE_KIND_JOB: &str = "Job";
pub const KUBE_KIND_CRONJOB: &str = "CronJob";

pub const LOG_POD_POLL_INTERVAL: Duration = Duration::from_millis(500);

pub const KMINUS_MIN_NOFILE: u64 = 65535;

pub const KMINUS_TZDATA_PROBE: &str = "/usr/share/zoneinfo/UTC";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[value(rename_all = "lower")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

pub const DEFAULT_LOG_LEVEL: LogLevel = LogLevel::Debug;

#[cfg(test)]
mod tests;
