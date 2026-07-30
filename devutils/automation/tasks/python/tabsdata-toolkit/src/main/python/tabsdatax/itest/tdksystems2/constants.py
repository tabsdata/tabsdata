#
# Copyright 2026 Tabsdata Inc.
#

"""Single source of truth for CLI vocabulary and environment variables.

Command names, option flags, roles and sentinels are constants (never string
literals at call sites) so the dispatcher's argv peeking and each command's
declared options can never drift apart. ``ENV_VARS`` drives the ``environment:``
section printed by top-level help.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# --- tools -----------------------------------------------------------------
SYS_TOOL = "tdx-system"
DOM_TOOL = "tdx-domain"

# --- descriptor roles ------------------------------------------------------
ROLE_SYSTEM: Literal["system"] = "system"
ROLE_DOMAIN: Literal["domain"] = "domain"

# --- verbs -----------------------------------------------------------------
V_TYPES = "types"
V_LIST = "list"
V_INFO = "info"
V_CREATE = "create"
V_DESTROY = "destroy"
V_STATUS = "status"
V_PURGE = "purge"
V_FETCH_BIN = "fetch-bin"
V_RESTART = "restart"
V_MODELS = "models"
V_CREATE_MODEL = "create-model"
V_CREATE_DATA = "create-data"
V_MODIFY_DATA = "modify-data"
V_MANIFEST = "manifest"
V_SECRETS = "secrets"
V_CLEAR_VENVS = "clear-venvs"
V_CONNECT = "connect"

# --- option flags ----------------------------------------------------------
OPT_TYPE = "--type"
OPT_SYS = "--sys"
OPT_DOMAIN = "--domain"
OPT_SYS_DIR = "--sys-dir"
OPT_DOMAIN_DIR = "--domain-dir"
OPT_FORCE = "--force"
OPT_FORCE_SHORT = "-f"
OPT_LATCH = "--latch"
OPT_MODEL = "--model"
OPT_SIZE = "--size"
OPT_ENTITY = "--entity"
OPT_SETS = "--sets"
OPT_OPS = "--ops"
OPT_SEED = "--seed"
OPT_PREFIX = "--prefix"
OPT_NAME_IN_SYSTEM = "--name-in-system"
OPT_PORT = "--port"
OPT_PORTS = "--ports"
OPT_FORMAT = "--format"
OPT_OUT = "--out"
OPT_SET = "--set"
OPT_MANIFEST = "--manifest"
OPT_SYNC = "--sync"
OPT_EXTERNAL_PREFIX = "--external-prefix"
OPT_REPORT = "--report"
OPT_FAIL_FAST = "--fail-fast"
OPT_SYS_PREFIX = "--sys-prefix"
OPT_SYS_HOSTNAME = "--sys-hostname"
OPT_DOMAIN_PREFIX = "--domain-prefix"
OPT_DRY_RUN = "--dry-run"
OPT_ENV_PREFIX = "--env-prefix"
OPT_REMOTE_SYS = "--remote-sys"
OPT_REMOTE_SECRET_ENV = "--remote-secret-env"
OPT_SSH_PORT = "--ssh-port"
OPT_IDENTITY = "--identity"
HELP_FLAGS = ("-h", "--help")

# --- batch execution -------------------------------------------------------
TARGET_ALL = "ALL"  # --sys/--domain ALL selects every type (create) / stored one
ENV_PARALLELISM = "TD_BACKENDS_PARALLELISM"
DEFAULT_PARALLELISM = 10
# Candidate free ports the dispatcher pre-allocates per system when starting many
# in parallel, so no two racing creates pick the same free port (a system uses as
# many as it needs — most one, a couple for multi-port stacks — and ignores the
# rest).
PORTS_PER_SYSTEM = 10

# --- sentinels / value sets ------------------------------------------------
SEED_AUTO = "AUTO"
ENTITY_ALL = "ALL"
INFO_FORMATS = ("json", "export", "unset")
OUT_JSON = "json"
OUT_FORMATS = (OUT_JSON,)

# --- environment variables -------------------------------------------------
ENV_CRYPTO_KEY = "TD_BACKENDS_CRYPTO_KEY"
ENV_BACKENDS_DIR = "TD_BACKENDS_DIR"
ENV_BUILD_USER = "TD_BUILD_USER"
ENV_BUILD_NUMBER = "TD_BUILD_NUMBER"
ENV_MYSQL_IMAGE_VERSION = "MYSQL_IMAGE_VERSION"
ENV_DEBUG = "TDX_DEBUG"

DEFAULT_BACKENDS_DIR = "~/.tabsdatax"
DEFAULT_MYSQL_IMAGE_VERSION = "8.4"

# Store subdirectories under the backends dir (the "2" isolates v2 from v1).
SYSTEMS_SUBDIR = "systems2"
DOMAINS_SUBDIR = "domains2"

# Where the mutating verbs write their detailed logs (a dedicated dir so old-log
# pruning only ever touches our own files). Logs are named <name>-<timestamp>.log.
LOG_DIR = "/tmp/tdx-logs"
LOG_MAX_AGE_DAYS = 1  # each run deletes logs in LOG_DIR older than this


@dataclass(frozen=True)
class EnvVar:
    name: str
    default: str  # human-readable default
    help: str


ENV_VARS: tuple[EnvVar, ...] = (
    EnvVar(
        ENV_CRYPTO_KEY, "(required, >=12 chars)", "secret that encrypts descriptors"
    ),
    EnvVar(
        ENV_BACKENDS_DIR,
        DEFAULT_BACKENDS_DIR,
        "base store dir (systems2/ and domains2/ live here)",
    ),
    EnvVar(
        ENV_BUILD_USER,
        "<OS login>",
        "build-identity user in the d_<user>_<build>_ prefix",
    ),
    EnvVar(
        ENV_BUILD_NUMBER, "0", "build-identity number in the d_<user>_<build>_ prefix"
    ),
    EnvVar(
        ENV_MYSQL_IMAGE_VERSION, DEFAULT_MYSQL_IMAGE_VERSION, "MySQL docker image tag"
    ),
    EnvVar(ENV_DEBUG, "(unset)", "set to 1 to print full tracebacks on error"),
    EnvVar(
        ENV_PARALLELISM,
        str(DEFAULT_PARALLELISM),
        "max concurrent targets in a batch (--sys/--domain list/ALL) run",
    ),
)
