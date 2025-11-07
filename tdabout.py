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
import yaml
from archspec import cpu
from setuptools import build_meta as _backend

TABSDATA_REPOSITORIES_ENTERPRISE = [
    ("tabsdata-ee", None, "Tabsdata Enterprise", "TABSDATA_EE"),
    ("tabsdata-os", None, "Tabsdata Open Source", "TABSDATA_OS"),
    ("tabsdata-ui", None, "Tabsdata User Interface", "TABSDATA_UI"),
    ("tabsdata-ag", None, "Tabsdata Agent", "TABSDATA_AG"),
    ("tabsdata-ci", None, "Tabsdata Automation", "TABSDATA_CI"),
]

TABSDATA_REPOSITORIES_OPENSOURCE = [
    ("tabsdata-os", "tabsdata", "Tabsdata Open Source", "TABSDATA_OS"),
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


# noinspection PyBroadException
def _capture_system_information():  # noqa: C901
    try:
        hostname = socket.gethostname()
    except Exception:
        hostname = "-"

    try:
        user = getpass.getuser()
    except Exception:
        user = "-"

    try:
        os_name = platform.system()
    except Exception:
        os_name = "-"

    try:
        if os_name == "Darwin":
            os_version = f"macOS {platform.mac_ver()[0]}"
        else:
            os_version = platform.release()
    except Exception:
        os_version = "-"

    try:
        cpu_info = cpuinfo.get_cpu_info()
    except Exception:
        cpu_info = {}

    try:
        cpu_vendor = cpu.host().vendor
    except Exception:
        cpu_vendor = "-"

    cpu_brand = cpu_info.get("brand_raw", "-")
    cpu_name = cpu_info.get("brand_raw", "-")

    try:
        cpu_count = psutil.cpu_count(logical=False)
    except Exception:
        cpu_count = "-"

    try:
        cpu_freq = psutil.cpu_freq()
        cpu_frequency = str(int(cpu_freq.current))
    except Exception:
        cpu_frequency = "-"

    try:
        total_memory_bytes = psutil.virtual_memory().total
        total_memory_gib = total_memory_bytes / (1024**3)
        total_memory = f"{total_memory_gib:.0f} GiB"
    except Exception:
        total_memory = "-"

    return {
        "hostname": hostname,
        "user": user,
        "os_name": os_name,
        "os_version": os_version,
        "cpu_vendor": cpu_vendor,
        "cpu_brand": cpu_brand,
        "cpu_name": cpu_name,
        "cpu_core_count": str(cpu_count) if cpu_count != "-" else "-",
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


def _render_build(enterprise: bool = False):
    try:
        build_metadata = _capture_build_information()
        rust_metadata = _capture_rust_information()
        python_metadata = _capture_python_information()
        node_metadata = _capture_node_information()
        system_metadata = _capture_system_information()
        metadata = {
            "X-Build-Date-UTC": build_metadata.get("build_date_utc", "?"),
            "X-Build-Date-Local": build_metadata.get("build_date_local", "?"),
            "X-Build-Timestamp-UTC": build_metadata.get("build_timestamp_utc", "?"),
            "X-Build-Timestamp-Local": build_metadata.get("build_timestamp_local", "?"),
            "X-Build-Timezone-Name": build_metadata.get("build_timezone_name", "?"),
            "X-Build-Timezone-Offset": build_metadata.get("build_timezone_offset", "?"),
        }
        solution_path = _get_solution_path()
        repositories = (
            TABSDATA_REPOSITORIES_ENTERPRISE
            if enterprise
            else TABSDATA_REPOSITORIES_OPENSOURCE
        )
        for (
            repository_name,
            alternate_repository_name,
            repository_description,
            repository_prefix,
        ) in repositories:
            repository_path = solution_path / repository_name
            git_metadata = _capture_git_information(repository_path)
            if git_metadata is None:
                if alternate_repository_name is not None:
                    # noinspection PyTypeChecker
                    alternate_repository_path = (
                        solution_path / alternate_repository_name
                    )
                    git_metadata = _capture_git_information(alternate_repository_path)
            if git_metadata is None:
                metadata[f"X-Git-{repository_prefix}-Exists"] = "false"
                continue
            metadata[f"X-Git-{repository_prefix}-Exists"] = "true"
            metadata[f"X-Git-{repository_prefix}-Name"] = repository_name
            metadata[f"X-Git-{repository_prefix}-Description"] = repository_description
            metadata[f"X-Git-{repository_prefix}-Branch"] = git_metadata.get(
                "branch", "?"
            )
            metadata[f"X-Git-{repository_prefix}-Tag"] = git_metadata.get("tag", "?")
            metadata[f"X-Git-{repository_prefix}-Commit-Short-Hash"] = git_metadata.get(
                "short_hash", "?"
            )
            metadata[f"X-Git-{repository_prefix}-Commit-Long-Hash"] = git_metadata.get(
                "long_hash", "?"
            )
            metadata[f"X-Git-{repository_prefix}-Commit-Date"] = git_metadata.get(
                "commit_date", "?"
            )
            metadata[f"X-Git-{repository_prefix}-Commit-Timestamp"] = git_metadata.get(
                "commit_timestamp", "?"
            )
            author_name = git_metadata.get("commit_author_name", "")
            author_email = git_metadata.get("commit_author_email", "")
            if author_name and author_email:
                metadata[f"X-Git-{repository_prefix}-Commit-Author"] = (
                    f"{author_name} <{author_email}>"
                )
            elif author_name:
                metadata[f"X-Git-{repository_prefix}-Commit-Author"] = author_name
            else:
                metadata[f"X-Git-{repository_prefix}-Commit-Author"] = "-"
            commit_message = git_metadata.get("commit_message", "-")
            if commit_message and commit_message != "-":
                commit_message = commit_message.replace("\n", " ").replace("\r", "")[
                    :200
                ]
            else:
                commit_message = "-"
            metadata[f"X-Git-{repository_prefix}-Commit-Message"] = commit_message
            metadata[f"X-Git-{repository_prefix}-Commit-Count"] = git_metadata.get(
                "commit_count", "?"
            )
            metadata[f"X-Git-{repository_prefix}-Describe"] = git_metadata.get(
                "describe", "?"
            )
            metadata[f"X-Git-{repository_prefix}-Dirty"] = git_metadata.get(
                "dirty", "?"
            )
        metadata.update(
            {
                "X-Rust-Semver": rust_metadata.get("rust_version", "?"),
                "X-Rust-Channel": rust_metadata.get("rust_channel", "?"),
                "X-Rust-Host-Triple": rust_metadata.get("rust_host_triple", "?"),
                "X-Rust-Commit-Hash": rust_metadata.get("rust_commit_hash", "?"),
                "X-Rust-Commit-Date": rust_metadata.get("rust_commit_date", "?"),
                "X-Rust-LLVM-Version": rust_metadata.get("llvm_version", "?"),
            }
        )
        metadata.update(
            {
                "X-Python-Version": python_metadata.get("python_version", "?"),
                "X-Python-Implementation": python_metadata.get(
                    "python_implementation", "?"
                ),
            }
        )
        metadata["X-Node-Version"] = node_metadata.get("node_version", "?")
        metadata.update(
            {
                "X-System-Hostname": system_metadata.get("hostname", "?"),
                "X-System-User": system_metadata.get("user", "?"),
                "X-System-OS-Name": system_metadata.get("os_name", "?"),
                "X-System-OS-Version": system_metadata.get("os_version", "?"),
                "X-System-CPU-Vendor": system_metadata.get("cpu_vendor", "?"),
                "X-System-CPU-Brand": system_metadata.get("cpu_brand", "?"),
                "X-System-CPU-Name": system_metadata.get("cpu_name", "?"),
                "X-System-CPU-Core-Count": system_metadata.get("cpu_core_count", "?"),
                "X-System-CPU-Frequency": system_metadata.get("cpu_frequency", "?"),
                "X-System-Total-Memory": system_metadata.get("total_memory", "?"),
            }
        )
        return metadata
    except Exception as exception:
        raise RuntimeError(
            f"Failed to render metadata block: {exception}"
        ) from exception


def _merge_metadata_contents(original_text):
    marker = "X-Build-Date-UTC:"
    if marker in original_text:
        return original_text
    block = _render_build()
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


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    wheel_name = _backend.build_editable(
        wheel_directory, config_settings, metadata_directory
    )
    wheel_path = Path(wheel_directory) / wheel_name
    _inject_wheel_metadata(wheel_path)
    return wheel_name


def inject_wheel_metadata(wheel_path):
    _inject_wheel_metadata(wheel_path)


def write_build_manifest(root_path, output_path):
    try:
        feature_yaml_path = Path(root_path) / ".manifest" / "feature.yaml"
        enterprise = False
        if feature_yaml_path.exists():
            # noinspection PyBroadException
            try:
                with open(feature_yaml_path, "r", encoding="utf-8") as f:
                    features = yaml.safe_load(f)
                    if features and isinstance(features, list):
                        enterprise = "enterprise" in features
            except Exception:
                pass

        build = _render_build(enterprise=enterprise)
        build["X-Enterprise"] = "true" if enterprise else "false"

        build_file = Path(output_path)
        build_file.parent.mkdir(parents=True, exist_ok=True)

        current_year = datetime.now().year
        build_content = yaml.dump(
            build,
            default_flow_style=False,
            default_style=None,
            sort_keys=False,
            allow_unicode=True,
        )
        header = f"#\n# Copyright {current_year} Tabs Data Inc.\n#\n\n"
        yaml_content = header + build_content
        build_file.write_text(yaml_content, encoding="utf-8")
    except Exception as exception:
        raise RuntimeError(
            f"Failed to write build manifest to {output_path}: {exception}"
        ) from exception


build_sdist = _backend.build_sdist
