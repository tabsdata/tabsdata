//
// Copyright 2026 Tabs Data Inc.
//

use const_format::concatcp;

pub const ENV_KUBECONFIG: &str = "KUBECONFIG";

pub const FOLDER_CLUSTER: &str = "cluster";
pub const FOLDER_REPOSITORY: &str = "repository";
pub const FOLDER_DATABASE: &str = "database";
pub const FOLDER_STORAGE: &str = "storage";
pub const FOLDER_RUNTIMES: &str = "runtimes";
pub const FOLDER_PYTHON: &str = "python";
pub const FOLDER_PYENV_TD: &str = "td";
pub const FOLDER_PYENV_AG: &str = "ag";
pub const FOLDER_STAGING: &str = "staging";
pub const FOLDER_PROFILE: &str = "profile";
pub const FOLDER_KUBE: &str = ".kube";
pub const FILE_KUBECONFIG: &str = "config";

pub const KMINUS_MIN_NOFILE: u64 = 65536;

pub const RESOURCE_KPROFILE: &str = "resources/kprofile";

pub const PROGRAM_TD_RESOURCE: &str = "td_resource";

pub const KPROFILE_CONFIGMAPS_FOLDER: &str = "configmaps";
pub const KPROFILE_GLOBALS_FOLDER: &str = "globals";
pub const KPROFILE_PODS_FOLDER: &str = "pods";
pub const KPROFILE_CONFIGS_FOLDER: &str = "configs";

pub const DEFAULT_INSTANCE_NAME: &str = "tabsdata";

pub const TABSDATA_CONFIGMAP_PREFIX: &str = "tabsdata-";

pub const CONFIGMAP_TABSDATA_GLOBALS: &str =
    concatcp!(TABSDATA_CONFIGMAP_PREFIX, KPROFILE_GLOBALS_FOLDER);
pub const CONFIGMAP_TABSDATA_PODS: &str =
    concatcp!(TABSDATA_CONFIGMAP_PREFIX, KPROFILE_PODS_FOLDER);
pub const CONFIGMAP_TABSDATA_CONFIGS: &str =
    concatcp!(TABSDATA_CONFIGMAP_PREFIX, KPROFILE_CONFIGS_FOLDER);

pub const TABSDATA_SECRET_PREFIX: &str = "tabsdata-";

pub const SECRET_TABSDATA_KUBECONFIG: &str = concatcp!(TABSDATA_SECRET_PREFIX, "kubeconfig");

pub const SECRET_KEY_KUBECONFIG: &str = "kubeconfig";

pub const TIER_LABEL: &str = "tabsdata.com/worker-tier";

pub const PART_OF_LABEL: &str = "app.kubernetes.io/part-of";
pub const PART_OF_TABSDATA: &str = "tabsdata";

pub const KUBE_IDENTIFIER_PREFIX: &str = "td-";

pub const POD_YAML_COMPONENT_LABEL: &str = "app.kubernetes.io/component";

pub const POD_YAML_KIND_POD: &str = "Pod";
pub const POD_YAML_KIND_JOB: &str = "Job";

pub const POD_YAML_FIELD_KIND: &str = "kind";
pub const POD_YAML_FIELD_METADATA: &str = "metadata";
pub const POD_YAML_FIELD_LABELS: &str = "labels";

pub const VALUE_Y: &str = "y";
pub const VALUE_YES: &str = "yes";
pub const VALUE_N: &str = "n";
pub const VALUE_NO: &str = "no";
