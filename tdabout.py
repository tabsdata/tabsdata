#
# Copyright 2025 Tabs Data Inc.
#

import getpass
import os
import platform
import socket
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import cpuinfo
import psutil
import tzlocal
from archspec import cpu
from setuptools import build_meta as _backend

TABSDATA_REPOSITORIES = [
    ("tabsdata-ee", "Tabsdata Enterprise", "TABSDATA_EE"),
    ("tabsdata-os", "Tabsdata Open Source", "TABSDATA_OS"),
    ("tabsdata-ui", "Tabsdata User Interface", "TABSDATA_UI"),
    ("tabsdata-ag", "Tabsdata Agent", "TABSDATA_AG"),
    ("tabsdata-ci", "Tabsdata Automation", "TABSDATA_CI"),
]


# noinspection PyBroadException
def _run_git(arguments, timeout=5, cwd=None):
    try:
        git_output = subprocess.run(
            ["git"] + arguments,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )
        if git_output.returncode == 0:
            return git_output.stdout.strip()
    except Exception:
        pass
    return None


def _capture_build_information():
    build_time_utc = datetime.now(timezone.utc)
    build_time_local = datetime.now()
    utc_offset_seconds = (
        time.localtime().tm_gmtoff
        if hasattr(time.localtime(), "tm_gmtoff")
        else -time.timezone
    )
    utc_offset_hours = utc_offset_seconds // 3600
    utc_offset_minutes = (abs(utc_offset_seconds) % 3600) // 60
    timezone_offset = f"{utc_offset_hours:+03d}:{utc_offset_minutes:02d}"
    timezone_name = tzlocal.get_localzone_name()

    return {
        "build_date_utc": build_time_utc.strftime("%Y-%m-%d"),
        "build_timestamp_utc": build_time_utc.isoformat(),
        "build_date_local": build_time_local.strftime("%Y-%m-%d"),
        "build_timestamp_local": build_time_local.isoformat(),
        "build_timezone_name": timezone_name,
        "build_timezone_offset": timezone_offset,
    }


def _obtain_rust_version():
    version = subprocess.run(
        ["rustc", "--version", "--verbose"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if version.returncode != 0:
        return None
    return version.stdout.strip()


def _parse_rust_version(lines):
    info_map = {
        "release": "rust_version",
        "host": "rust_host_triple",
        "commit-hash": "rust_commit_hash",
        "commit-date": "rust_commit_date",
        "LLVM version": "llvm_version",
    }
    rust_info = {}
    for line in lines:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        info_key = info_map.get(key.strip())
        if info_key:
            rust_info[info_key] = value.strip()
    return rust_info


def _extract_rust_version(line):
    if not line:
        return "stable"
    if "nightly" in line:
        return "nightly"
    if "beta" in line:
        return "beta"
    return "stable"


def _capture_rust_information():
    rustc_output = _obtain_rust_version()
    if not rustc_output:
        return {}
    lines = rustc_output.splitlines()
    rust_info = _parse_rust_version(lines)
    version_line = lines[0] if lines else ""
    rust_info["rust_channel"] = _extract_rust_version(version_line)
    return rust_info


def _capture_python_information():
    return {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
    }


def _capture_node_information():
    node_output = subprocess.run(
        ["node", "--version"],
        capture_output=True,
        text=True,
        timeout=5,
    )

    node_info = {}
    if node_output.returncode == 0:
        version = node_output.stdout.strip().replace("v", "")
        node_info["node_version"] = version

    return node_info


def _capture_system_information():
    hostname = socket.gethostname()
    user = getpass.getuser()
    os_name = platform.system()
    if os_name == "Darwin":
        os_version = f"macOS {platform.mac_ver()[0]}"
    else:
        os_version = platform.release()
    cpu_info = cpuinfo.get_cpu_info()
    cpu_vendor = cpu.host().vendor
    cpu_brand = cpu_info.get("brand_raw", "?")
    cpu_name = cpu_info.get("brand_raw", "?")
    cpu_count = psutil.cpu_count(logical=False)
    cpu_freq = psutil.cpu_freq()
    cpu_frequency = str(int(cpu_freq.current))
    total_memory_bytes = psutil.virtual_memory().total
    total_memory_gib = total_memory_bytes / (1024**3)
    total_memory = f"{total_memory_gib:.0f} GiB"

    return {
        "hostname": hostname,
        "user": user,
        "os_name": os_name,
        "os_version": os_version,
        "cpu_vendor": cpu_vendor,
        "cpu_brand": cpu_brand,
        "cpu_name": cpu_name,
        "cpu_core_count": str(cpu_count),
        "cpu_frequency": cpu_frequency,
        "total_memory": total_memory,
    }


def _get_solution_path():
    solution_home = os.environ.get("TABSDATA_SOLUTION_HOME")
    if solution_home:
        return Path(solution_home)
    current = Path.cwd()
    if current.parent.exists():
        return current.parent
    return current


def _capture_git_information(repository_path):
    if not repository_path or not Path(repository_path).exists():
        return None
    repository_path_str = str(repository_path)
    git_dir = Path(repository_path) / ".git"
    if not git_dir.exists():
        return None
    return {
        "tag": (
            _run_git(
                ["describe", "--tags", "--exact-match", "HEAD"],
                cwd=repository_path_str,
            )
            or "-"
        ),
        "branch": _run_git(
            ["rev-parse", "--abbrev-ref", "HEAD"],
            cwd=repository_path_str,
        ),
        "short_hash": _run_git(
            ["rev-parse", "--short", "HEAD"],
            cwd=repository_path_str,
        ),
        "long_hash": _run_git(
            ["rev-parse", "HEAD"],
            cwd=repository_path_str,
        ),
        "commit_date": _run_git(
            ["log", "-1", "--format=%cI"],
            cwd=repository_path_str,
        ),
        "commit_timestamp": _run_git(
            ["log", "-1", "--format=%cI"],
            cwd=repository_path_str,
        ),
        "commit_author_name": _run_git(
            ["log", "-1", "--format=%an"],
            cwd=repository_path_str,
        ),
        "commit_author_email": _run_git(
            ["log", "-1", "--format=%ae"],
            cwd=repository_path_str,
        ),
        "commit_message": _run_git(
            ["log", "-1", "--format=%s"],
            cwd=repository_path_str,
        ),
        "commit_count": _run_git(
            ["rev-list", "--count", "HEAD"],
            cwd=repository_path_str,
        ),
        "describe": _run_git(
            ["describe", "--always", "--dirty", "--tags"],
            cwd=repository_path_str,
        ),
        "dirty": (
            "true"
            if _run_git(
                ["status", "--porcelain"],
                cwd=repository_path_str,
            )
            else "false"
        ),
    }


def _render_metadata_block():
    try:
        build_metadata = _capture_build_information()
        rust_metadata = _capture_rust_information()
        python_metadata = _capture_python_information()
        node_metadata = _capture_node_information()
        system_metadata = _capture_system_information()
        lines = [
            f"X-Build-Date-UTC: {build_metadata.get('build_date_utc', '?')}",
            f"X-Build-Date-Local: {build_metadata.get('build_date_local', '?')}",
            f"X-Build-Timestamp-UTC: {build_metadata.get('build_timestamp_utc', '?')}",
            (
                "X-Build-Timestamp-Local:"
                f" {build_metadata.get('build_timestamp_local', '?')}"
            ),
            f"X-Build-Timezone-Name: {build_metadata.get('build_timezone_name', '?')}",
            (
                "X-Build-Timezone-Offset:"
                f" {build_metadata.get('build_timezone_offset', '?')}"
            ),
        ]
        solution_path = _get_solution_path()
        for (
            repository_name,
            repository_description,
            repository_prefix,
        ) in TABSDATA_REPOSITORIES:
            repo_path = solution_path / repository_name
            git_metadata = _capture_git_information(repo_path)
            if git_metadata is None:
                lines.append(f"X-Git-{repository_prefix}-Exists: false")
                continue
            lines.append(f"X-Git-{repository_prefix}-Exists: true")
            lines.append(f"X-Git-{repository_prefix}-Name: {repository_name}")
            lines.append(
                f"X-Git-{repository_prefix}-Description: {repository_description}"
            )
            lines.append(
                f"X-Git-{repository_prefix}-Branch: {git_metadata.get('branch', '?')}"
            )
            lines.append(
                f"X-Git-{repository_prefix}-Tag: {git_metadata.get('tag', '?')}"
            )
            lines.append(
                f"X-Git-{repository_prefix}-Commit-Short-Hash:"
                f" {git_metadata.get('short_hash', '?')}"
            )
            lines.append(
                f"X-Git-{repository_prefix}-Commit-Long-Hash:"
                f" {git_metadata.get('long_hash', '?')}"
            )
            lines.append(
                f"X-Git-{repository_prefix}-Commit-Date:"
                f" {git_metadata.get('commit_date', '?')}"
            )
            lines.append(
                f"X-Git-{repository_prefix}-Commit-Timestamp:"
                f" {git_metadata.get('commit_timestamp', '?')}"
            )
            author_name = git_metadata.get("commit_author_name", "")
            author_email = git_metadata.get("commit_author_email", "")
            if author_name and author_email:
                lines.append(
                    f"X-Git-{repository_prefix}-Commit-Author: {author_name}"
                    f" <{author_email}>"
                )
            elif author_name:
                lines.append(f"X-Git-{repository_prefix}-Commit-Author: {author_name}")
            else:
                lines.append(f"X-Git-{repository_prefix}-Commit-Author: ?")
            commit_message = git_metadata.get("commit_message", "?")
            if commit_message and commit_message != "?":
                commit_message = commit_message.replace("\n", " ").replace("\r", "")[
                    :200
                ]
            else:
                commit_message = "?"
            lines.append(f"X-Git-{repository_prefix}-Commit-Message: {commit_message}")
            lines.append(
                f"X-Git-{repository_prefix}-Commit-Count:"
                f" {git_metadata.get('commit_count', '?')}"
            )
            lines.append(
                f"X-Git-{repository_prefix}-Describe:"
                f" {git_metadata.get('describe', '?')}"
            )
            lines.append(
                f"X-Git-{repository_prefix}-Dirty: {git_metadata.get('dirty', '?')}"
            )
        lines.extend(
            [
                f"X-Rust-Semver: {rust_metadata.get('rust_version', '?')}",
                f"X-Rust-Channel: {rust_metadata.get('rust_channel', '?')}",
                f"X-Rust-Host-Triple: {rust_metadata.get('rust_host_triple', '?')}",
                f"X-Rust-Commit-Hash: {rust_metadata.get('rust_commit_hash', '?')}",
                f"X-Rust-Commit-Date: {rust_metadata.get('rust_commit_date', '?')}",
                f"X-Rust-LLVM-Version: {rust_metadata.get('llvm_version', '?')}",
                f"X-Python-Version: {python_metadata.get('python_version', '?')}",
                (
                    "X-Python-Implementation:"
                    f" {python_metadata.get('python_implementation', '?')}"
                ),
                f"X-Node-Version: {node_metadata.get('node_version', '?')}",
                f"X-System-Hostname: {system_metadata.get('hostname', '?')}",
                f"X-System-User: {system_metadata.get('user', '?')}",
                f"X-System-OS-Name: {system_metadata.get('os_name', '?')}",
                f"X-System-OS-Version: {system_metadata.get('os_version', '?')}",
                f"X-System-CPU-Vendor: {system_metadata.get('cpu_vendor', '?')}",
                f"X-System-CPU-Brand: {system_metadata.get('cpu_brand', '?')}",
                f"X-System-CPU-Name: {system_metadata.get('cpu_name', '?')}",
                (
                    "X-System-CPU-Core-Count:"
                    f" {system_metadata.get('cpu_core_count', '?')}"
                ),
                f"X-System-CPU-Frequency: {system_metadata.get('cpu_frequency', '?')}",
                f"X-System-Total-Memory: {system_metadata.get('total_memory', '?')}",
            ]
        )
        return "\n".join(lines)
    except Exception as exception:
        raise RuntimeError(
            f"Failed to render metadata block: {exception}"
        ) from exception


def _merge_metadata_contents(original_text):
    marker = "X-Build-Date-UTC:"
    if marker in original_text:
        return original_text
    block = _render_metadata_block()
    stripped = original_text.rstrip("\n")
    return f"{block}\n{stripped}\n"


# noinspection PyBroadException
def _inject_metadata(metadata_file):
    path = Path(metadata_file)
    if not path.exists():
        return
    try:
        existing = path.read_text(encoding="utf-8")
    except Exception:
        return
    merged = _merge_metadata_contents(existing)
    if merged == existing:
        return
    path.write_text(merged, encoding="utf-8")


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    metadata = _backend.prepare_metadata_for_build_wheel(
        metadata_directory, config_settings
    )
    metadata_path = Path(metadata_directory) / metadata / "METADATA"
    if metadata_path.exists():
        _inject_metadata(metadata_path)
    return metadata


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    wheel_name = _backend.build_wheel(
        wheel_directory, config_settings, metadata_directory
    )
    wheel_path = Path(wheel_directory) / wheel_name
    _inject_wheel_metadata(wheel_path)
    return wheel_name


def _inject_wheel_metadata(wheel_path):
    wheel_path = Path(wheel_path)
    if not wheel_path.exists():
        return
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            subprocess.run(
                [
                    "wheel",
                    "unpack",
                    "-d",
                    str(temp_path),
                    str(
                        wheel_path,
                    ),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            unpacked_wheels = list(temp_path.glob("*"))
            if not unpacked_wheels:
                return
            unpacked_wheel = unpacked_wheels[0]
            metadata_files = list(unpacked_wheel.glob("*.dist-info/METADATA"))
            if not metadata_files:
                return
            metadata_file = metadata_files[0]
            existing_text = metadata_file.read_text(encoding="utf-8")
            merged_text = _merge_metadata_contents(existing_text)
            if merged_text != existing_text:
                metadata_file.write_text(merged_text, encoding="utf-8")
                repacked_wheel_folder = temp_path / "output"
                repacked_wheel_folder.mkdir()
                subprocess.run(
                    [
                        "wheel",
                        "pack",
                        "-d",
                        str(repacked_wheel_folder),
                        str(
                            unpacked_wheel,
                        ),
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                repacked_wheels = list(repacked_wheel_folder.glob("*.whl"))
                if repacked_wheels:
                    repacked_wheel = repacked_wheels[0]
                    os.replace(repacked_wheel, wheel_path)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Error injecting metadata into wheel {wheel_path}: {e.stderr}"
        ) from e
    except Exception as exception:
        raise RuntimeError(
            f"Exception injecting metadata into wheel {wheel_path}: {exception}"
        ) from exception


def inject_wheel_metadata(wheel_path):
    _inject_wheel_metadata(wheel_path)


build_sdist = _backend.build_sdist
