#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import time
from typing import Iterable

from case import (
    STATE_BOTH_STARTED,
    STATE_BOTH_STOPPED,
    STATE_CLUSTER_ONLY,
    State,
)
from constant import (
    CREATE_TIMEOUT_SECONDS,
    INSTANCE_FOLDER,
    INSTANCE_HOST_PROCESS_PATTERNS,
    INSTANCE_NAME,
    KUBECONFIG_PATH,
    KUBECTL_TIMEOUT_SECONDS,
    NAMESPACE,
    PART_OF_SELECTOR,
    PGREP_TIMEOUT_SECONDS,
    POLL_INTERVAL_SECONDS,
    PROVIDER,
    SETTLE_SECONDS,
    START_TIMEOUT_SECONDS,
    STOP_TIMEOUT_SECONDS,
    TERMINAL_PHASES,
)


def tdkserver_binary(config) -> str:
    return config.getoption("--tdkserver")


def run_tdkserver(
    config,
    *args: str,
    expect_exit_zero: bool = True,
    timeout: int = START_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    binary = tdkserver_binary(config)
    cmd = [binary, "--accept-terms", "y", *args]
    print(f"\n$ {' '.join(cmd)}")
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)
    if expect_exit_zero:
        assert proc.returncode == 0, (
            f"Expected exit 0, got {proc.returncode} from {' '.join(cmd)}"
        )
    return proc


def kubectl(
    *args: str, timeout: int = KUBECTL_TIMEOUT_SECONDS
) -> subprocess.CompletedProcess[str]:
    cmd = ["kubectl", f"--kubeconfig={KUBECONFIG_PATH}", *args]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def cluster_alive() -> bool:
    if not KUBECONFIG_PATH.exists():
        return False
    result = kubectl("cluster-info", "--request-timeout=3s")
    return result.returncode == 0


def instance_host_processes_alive() -> bool:
    result = subprocess.run(
        ["pgrep", "-f", "|".join(INSTANCE_HOST_PROCESS_PATTERNS)],
        capture_output=True,
        text=True,
        timeout=PGREP_TIMEOUT_SECONDS,
    )
    return result.returncode == 0 and bool(result.stdout.strip())


def instance_alive() -> bool:
    if not cluster_alive():
        return False
    result = kubectl(
        "get",
        "pods",
        "-n",
        NAMESPACE,
        f"--selector={PART_OF_SELECTOR}",
        "-o",
        "jsonpath={.items[*].status.phase}",
    )
    if result.returncode != 0:
        return False
    phases = [phase for phase in result.stdout.split() if phase]
    kube_alive = any(phase not in TERMINAL_PHASES for phase in phases)
    if kube_alive:
        return True
    return instance_host_processes_alive()


def observed_state() -> State:
    return State(cluster=cluster_alive(), instance=instance_alive())


def wait_for_state(target: State, timeout: int) -> State:
    deadline = time.monotonic() + timeout
    last = observed_state()
    while last != target and time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL_SECONDS)
        last = observed_state()
    return last


def assert_substrings(output: str, substrings: Iterable[str]) -> None:
    for needle in substrings:
        assert needle in output, (
            f"Expected substring not found in output: {needle!r}\n"
            f"--- output ---\n{output}\n"
        )


def ensure_instance_exists(config) -> None:
    if INSTANCE_FOLDER.exists():
        return
    run_tdkserver(
        config,
        "server",
        "create",
        "--provider",
        PROVIDER,
        "--name",
        INSTANCE_NAME,
        "--preview",
        "--yes",
        timeout=CREATE_TIMEOUT_SECONDS,
    )


def ensure_secrets_populated(config) -> None:
    folder = tempfile.mkdtemp(prefix="td-lifecycle-secrets-")
    try:
        run_tdkserver(
            config,
            "secret",
            "get",
            "--source",
            "default",
            "--provider",
            PROVIDER,
            "--name",
            INSTANCE_NAME,
            "--folder",
            folder,
            timeout=START_TIMEOUT_SECONDS,
        )
        run_tdkserver(
            config,
            "secret",
            "put",
            "--provider",
            PROVIDER,
            "--name",
            INSTANCE_NAME,
            "--folder",
            folder,
            timeout=START_TIMEOUT_SECONDS,
        )
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def stop_instance(config) -> None:
    run_tdkserver(
        config,
        "instance",
        "stop",
        "--provider",
        PROVIDER,
        "--name",
        INSTANCE_NAME,
        "--preview",
        "--yes",
        timeout=STOP_TIMEOUT_SECONDS,
    )


def stop_cluster(config) -> None:
    run_tdkserver(
        config,
        "cluster",
        "stop",
        "--provider",
        PROVIDER,
        "--name",
        INSTANCE_NAME,
        "--preview",
        "--yes",
        timeout=STOP_TIMEOUT_SECONDS,
    )


def start_cluster(config) -> None:
    run_tdkserver(
        config,
        "cluster",
        "start",
        "--provider",
        PROVIDER,
        "--name",
        INSTANCE_NAME,
        "--preview",
        "--yes",
        timeout=START_TIMEOUT_SECONDS,
    )


def start_instance(config) -> None:
    run_tdkserver(
        config,
        "instance",
        "start",
        "--provider",
        PROVIDER,
        "--name",
        INSTANCE_NAME,
        "--preview",
        "--yes",
        timeout=START_TIMEOUT_SECONDS,
    )


def drive_to_state(config, target: State) -> None:
    current = observed_state()
    if current == target:
        return
    if target == STATE_BOTH_STOPPED:
        if current.instance:
            stop_instance(config)
        if current.cluster:
            stop_cluster(config)
    elif target == STATE_CLUSTER_ONLY:
        if current.instance:
            stop_instance(config)
        if not current.cluster:
            start_cluster(config)
    elif target == STATE_BOTH_STARTED:
        if not current.cluster:
            start_cluster(config)
        if not current.instance:
            start_instance(config)
    time.sleep(SETTLE_SECONDS)
    actual = wait_for_state(target, timeout=START_TIMEOUT_SECONDS)
    assert actual == target, (
        f"Failed to drive to {target.label()}, stuck at {actual.label()}"
    )
