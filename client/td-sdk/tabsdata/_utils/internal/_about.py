#
# Copyright 2025 Tabs Data Inc.
#

import os
import shutil
import subprocess
from importlib.metadata import metadata as get_package_metadata

import yaml

from tabsdata._utils.constants import TABSDATA_MODULE_NAME
from tabsdata._utils.internal._resources import td_resource


def load_repositories():
    # noinspection PyBroadException
    try:
        repositories_path = td_resource("resources/about/repositories.yaml")
        with open(repositories_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        repositories = [
            (
                repository["name"],
                repository["description"],
                repository["prefix"],
            )
            for repository in config.get("repositories", [])
        ]
        return repositories
    except Exception:
        return []


TABSDATA_REPOSITORIES = load_repositories()


# noinspection DuplicatedCode
def tdabout_from_metadata(package_name=TABSDATA_MODULE_NAME):
    try:
        metadata = get_package_metadata(package_name)
    except Exception as exception:
        print(f"Error: Could not load tabsdata package metadata: {exception}.")
        return

    tdabout_binary = shutil.which("tdabout")
    if not tdabout_binary:
        print("Error: tdabout binary not found in PATH.")
        return

    env = os.environ.copy()

    env["TD_VERGEN_BUILD_TYPE"] = "Python"

    env["TD_VERSION"] = metadata.get("Version", "-")

    # Build Information
    env["VERGEN_BUILD_DATE"] = metadata.get("X-Build-Date-UTC", "-")
    env["VERGEN_BUILD_TIMESTAMP"] = metadata.get("X-Build-Timestamp-UTC", "-")
    env["VERGEN_BUILD_TIMEZONE_NAME"] = metadata.get("X-Build-Timezone-Name", "-")
    env["VERGEN_BUILD_TIMEZONE_OFFSET"] = metadata.get("X-Build-Timezone-Offset", "-")

    repositories = TABSDATA_REPOSITORIES

    # Git Information
    for (
        repository_name,
        repository_description,
        repository_prefix,
    ) in repositories:
        exists = metadata.get(f"X-Git-{repository_prefix}-Exists", "false")
        env[f"VERGEN_GIT_{repository_prefix}_EXISTS"] = exists

        if exists == "true":
            env[f"VERGEN_GIT_{repository_prefix}_NAME"] = metadata.get(
                f"X-Git-{repository_prefix}-Name", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_DESCRIPTION"] = metadata.get(
                f"X-Git-{repository_prefix}-Description", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_BRANCH"] = metadata.get(
                f"X-Git-{repository_prefix}-Branch", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_TAG"] = metadata.get(
                f"X-Git-{repository_prefix}-Tag", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_SHA"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Short-Hash", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_LONG_HASH"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Long-Hash", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_COMMIT_DATE"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Date", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_COMMIT_TIMESTAMP"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Timestamp", "-"
            )
            author = metadata.get(f"X-Git-{repository_prefix}-Commit-Author", "-")
            if "<" in author and ">" in author:
                name, email = author.rsplit("<", 1)
                env[f"VERGEN_GIT_{repository_prefix}_COMMIT_AUTHOR_NAME"] = name.strip()
                env[f"VERGEN_GIT_{repository_prefix}_COMMIT_AUTHOR_EMAIL"] = (
                    email.rstrip(">")
                )
            else:
                env[f"VERGEN_GIT_{repository_prefix}_COMMIT_AUTHOR_NAME"] = author
                env[f"VERGEN_GIT_{repository_prefix}_COMMIT_AUTHOR_EMAIL"] = "-"
            env[f"VERGEN_GIT_{repository_prefix}_COMMIT_MESSAGE"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Message", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_COMMIT_COUNT"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Count", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_DESCRIBE"] = metadata.get(
                f"X-Git-{repository_prefix}-Describe", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_DIRTY"] = metadata.get(
                f"X-Git-{repository_prefix}-Dirty", "-"
            )

    # Rust Information
    env["VERGEN_RUSTC_SEMVER"] = metadata.get("X-Rust-Semver", "-")
    env["VERGEN_RUSTC_CHANNEL"] = metadata.get("X-Rust-Channel", "-")
    env["VERGEN_RUSTC_HOST_TRIPLE"] = metadata.get("X-Rust-Host-Triple", "-")
    env["VERGEN_RUSTC_COMMIT_HASH"] = metadata.get("X-Rust-Commit-Hash", "-")
    env["VERGEN_RUSTC_COMMIT_DATE"] = metadata.get("X-Rust-Commit-Date", "-")
    env["VERGEN_RUSTC_LLVM_VERSION"] = metadata.get("X-Rust-LLVM-Version", "-")

    # Cargo Information
    env["VERGEN_CARGO_TARGET_TRIPLE"] = metadata.get("X-Cargo-Target-Triple", "-")
    env["VERGEN_CARGO_FEATURES"] = metadata.get("X-Cargo-Features", "-")
    env["VERGEN_CARGO_DEBUG"] = metadata.get("X-Cargo-Debug", "-")
    env["VERGEN_CARGO_OPT_LEVEL"] = metadata.get("X-Cargo-Opt-Level", "-")

    # Python Information
    env["VERGEN_PYTHON_VERSION"] = metadata.get("X-Python-Version", "-")
    env["VERGEN_PYTHON_IMPLEMENTATION"] = metadata.get("X-Python-Implementation", "-")

    # Node Information
    env["VERGEN_NODE_VERSION"] = metadata.get("X-Node-Version", "-")

    # System information
    env["VERGEN_SYSINFO_HOST"] = metadata.get("X-System-Hostname", "-")
    env["VERGEN_SYSINFO_USER"] = metadata.get("X-System-User", "-")
    env["VERGEN_SYSINFO_NAME"] = metadata.get("X-System-OS-Name", "-")
    env["VERGEN_SYSINFO_OS_VERSION"] = metadata.get("X-System-OS-Version", "-")
    env["VERGEN_SYSINFO_CPU_BRAND"] = metadata.get("X-System-CPU-Brand", "-")
    env["VERGEN_SYSINFO_CPU_NAME"] = metadata.get("X-System-CPU-Name", "-")
    env["VERGEN_SYSINFO_CPU_VENDOR"] = metadata.get("X-System-CPU-Vendor", "-")
    env["VERGEN_SYSINFO_CPU_CORE_COUNT"] = metadata.get("X-System-CPU-Core-Count", "-")
    env["VERGEN_SYSINFO_CPU_FREQUENCY"] = metadata.get("X-System-CPU-Frequency", "-")
    env["VERGEN_SYSINFO_TOTAL_MEMORY"] = metadata.get("X-System-Total-Memory", "-")

    try:
        subprocess.run([tdabout_binary], check=True, env=env)
    except subprocess.CalledProcessError as error:
        print(f"Error: tdabout failed with exit code {error.returncode}")
    except Exception as exception:
        print(f"Error: Failed to execute tdabout: {exception}")


# noinspection DuplicatedCode
def tdabout_from_build(package_name=TABSDATA_MODULE_NAME):  # noqa: C901
    try:
        build = td_resource("assets/manifest/BUILD")
        metadata = {}
        try:
            with open(build, "r", encoding="utf-8") as f:
                metadata = yaml.safe_load(f) or {}
        except Exception as exception:
            print(f"Warning: Could not parse BUILD manifest: {exception}")
    except Exception as exception:
        print(f"Error: Could not locate BUILD manifest file: {exception}.")
        return

    # noinspection PyBroadException
    try:
        package_metadata = get_package_metadata(package_name)
        version = package_metadata.get("Version", "-")
    except Exception:
        version = "-"

    tdabout_binary = shutil.which("tdabout")
    if not tdabout_binary:
        print("Error: tdabout binary not found in PATH.")
        return

    env = os.environ.copy()

    env["TD_VERGEN_BUILD_TYPE"] = "Python"

    env["TD_VERSION"] = version

    # Build Information
    env["VERGEN_BUILD_DATE"] = metadata.get("X-Build-Date-UTC", "-")
    env["VERGEN_BUILD_TIMESTAMP"] = metadata.get("X-Build-Timestamp-UTC", "-")
    env["VERGEN_BUILD_TIMEZONE_NAME"] = metadata.get("X-Build-Timezone-Name", "-")
    env["VERGEN_BUILD_TIMEZONE_OFFSET"] = metadata.get("X-Build-Timezone-Offset", "-")

    repositories = TABSDATA_REPOSITORIES

    # Git Information
    for (
        repository_name,
        repository_description,
        repository_prefix,
    ) in repositories:
        exists = metadata.get(f"X-Git-{repository_prefix}-Exists", "false")
        env[f"VERGEN_GIT_{repository_prefix}_EXISTS"] = exists

        if exists == "true":
            env[f"VERGEN_GIT_{repository_prefix}_NAME"] = metadata.get(
                f"X-Git-{repository_prefix}-Name", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_DESCRIPTION"] = metadata.get(
                f"X-Git-{repository_prefix}-Description", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_BRANCH"] = metadata.get(
                f"X-Git-{repository_prefix}-Branch", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_TAG"] = metadata.get(
                f"X-Git-{repository_prefix}-Tag", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_SHA"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Short-Hash", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_LONG_HASH"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Long-Hash", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_COMMIT_DATE"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Date", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_COMMIT_TIMESTAMP"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Timestamp", "-"
            )
            author = metadata.get(f"X-Git-{repository_prefix}-Commit-Author", "-")
            if "<" in author and ">" in author:
                name, email = author.rsplit("<", 1)
                env[f"VERGEN_GIT_{repository_prefix}_COMMIT_AUTHOR_NAME"] = name.strip()
                env[f"VERGEN_GIT_{repository_prefix}_COMMIT_AUTHOR_EMAIL"] = (
                    email.rstrip(">")
                )
            else:
                env[f"VERGEN_GIT_{repository_prefix}_COMMIT_AUTHOR_NAME"] = author
                env[f"VERGEN_GIT_{repository_prefix}_COMMIT_AUTHOR_EMAIL"] = "-"
            env[f"VERGEN_GIT_{repository_prefix}_COMMIT_MESSAGE"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Message", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_COMMIT_COUNT"] = metadata.get(
                f"X-Git-{repository_prefix}-Commit-Count", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_DESCRIBE"] = metadata.get(
                f"X-Git-{repository_prefix}-Describe", "-"
            )
            env[f"VERGEN_GIT_{repository_prefix}_DIRTY"] = metadata.get(
                f"X-Git-{repository_prefix}-Dirty", "-"
            )

    # Rust Information
    env["VERGEN_RUSTC_SEMVER"] = metadata.get("X-Rust-Semver", "-")
    env["VERGEN_RUSTC_CHANNEL"] = metadata.get("X-Rust-Channel", "-")
    env["VERGEN_RUSTC_HOST_TRIPLE"] = metadata.get("X-Rust-Host-Triple", "-")
    env["VERGEN_RUSTC_COMMIT_HASH"] = metadata.get("X-Rust-Commit-Hash", "-")
    env["VERGEN_RUSTC_COMMIT_DATE"] = metadata.get("X-Rust-Commit-Date", "-")
    env["VERGEN_RUSTC_LLVM_VERSION"] = metadata.get("X-Rust-LLVM-Version", "-")

    # Cargo Information
    env["VERGEN_CARGO_TARGET_TRIPLE"] = metadata.get("X-Cargo-Target-Triple", "-")
    env["VERGEN_CARGO_FEATURES"] = metadata.get("X-Cargo-Features", "-")
    env["VERGEN_CARGO_DEBUG"] = metadata.get("X-Cargo-Debug", "-")
    env["VERGEN_CARGO_OPT_LEVEL"] = metadata.get("X-Cargo-Opt-Level", "-")

    # Python Information
    env["VERGEN_PYTHON_VERSION"] = metadata.get("X-Python-Version", "-")
    env["VERGEN_PYTHON_IMPLEMENTATION"] = metadata.get("X-Python-Implementation", "-")

    # Node Information
    env["VERGEN_NODE_VERSION"] = metadata.get("X-Node-Version", "-")

    # System information
    env["VERGEN_SYSINFO_HOST"] = metadata.get("X-System-Hostname", "-")
    env["VERGEN_SYSINFO_USER"] = metadata.get("X-System-User", "-")
    env["VERGEN_SYSINFO_NAME"] = metadata.get("X-System-OS-Name", "-")
    env["VERGEN_SYSINFO_OS_VERSION"] = metadata.get("X-System-OS-Version", "-")
    env["VERGEN_SYSINFO_CPU_BRAND"] = metadata.get("X-System-CPU-Brand", "-")
    env["VERGEN_SYSINFO_CPU_NAME"] = metadata.get("X-System-CPU-Name", "-")
    env["VERGEN_SYSINFO_CPU_VENDOR"] = metadata.get("X-System-CPU-Vendor", "-")
    env["VERGEN_SYSINFO_CPU_CORE_COUNT"] = metadata.get("X-System-CPU-Core-Count", "-")
    env["VERGEN_SYSINFO_CPU_FREQUENCY"] = metadata.get("X-System-CPU-Frequency", "-")
    env["VERGEN_SYSINFO_TOTAL_MEMORY"] = metadata.get("X-System-Total-Memory", "-")

    try:
        subprocess.run([tdabout_binary], check=True, env=env)
    except subprocess.CalledProcessError as error:
        print(f"Error: tdabout failed with exit code {error.returncode}")
    except Exception as exception:
        print(f"Error: Failed to execute tdabout: {exception}")


def tdabout(package_name="tabsdata"):
    tdabout_from_build(package_name)
