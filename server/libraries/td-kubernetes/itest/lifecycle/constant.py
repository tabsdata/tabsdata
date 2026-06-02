#
# Copyright 2026 Tabs Data Inc.
#

from pathlib import Path

INSTANCE_NAME = "tdlifecycle"
INSTANCE_FOLDER = Path.home() / ".tabsdata" / "instances" / INSTANCE_NAME

KUBECONFIG_PATH = INSTANCE_FOLDER / "cluster" / "config" / "kubeconfig.yaml"

PROVIDER = "kminus"

SETTLE_SECONDS = 2
START_TIMEOUT_SECONDS = 180
STOP_TIMEOUT_SECONDS = 60
CREATE_TIMEOUT_SECONDS = 600
KUBECTL_TIMEOUT_SECONDS = 10
PGREP_TIMEOUT_SECONDS = 5
POLL_INTERVAL_SECONDS = 2

INSTANCE_HOST_PROCESS_PATTERNS = ("td_apiserver", "td_aiagent")

PART_OF_SELECTOR = "app.kubernetes.io/part-of=tabsdata"
NAMESPACE = "default"

TERMINAL_PHASES = ("Succeeded", "Failed")

KEEP_ON_FAILURE_ENV = "TABSDATA_LIFECYCLE_KEEP_ON_FAILURE"
