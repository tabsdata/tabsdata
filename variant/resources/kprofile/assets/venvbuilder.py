#
# Copyright 2026 Tabsdata Inc.
#

import argparse
import os
import platform
import re
import shutil
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
import time
import traceback
import urllib.request
from pathlib import Path
from typing import Any, Callable, NoReturn

PYTHON_VERSION: str = "3.12.13"

PYTHON_MAJOR: int = 3
PYTHON_MINOR: int = 12
PYTHON_MINIMUM_PATCH: int = 13

IMAGE_NAME: str = "tdvenvs"
IMAGE_SUFFIX: str = ".tar.zst"

UV_FOLDER: str = ".uv"
UV_PYTHON_FOLDER: str = "python"
UV_KENVS_FOLDER: str = "kenvs"

TOOLS_FOLDER: str = ".tools"
TOOLS_BIN_FOLDER: str = "bin"
TOOLS_KENVS_FOLDER: str = "kenvs"

VENV_TD: str = "td"
VENV_Z: str = "z"

ZSTANDARD_PACKAGE: str = "zstandard"

ARCHIVE_MODULE: str = "tabsdatak.common._interpreter.python.archive"

ARCHIVE_COMPRESSION_LEVEL: str = "3"
ARCHIVE_COMPRESSION_THREADS: str = "-1"
ARCHIVE_LONG_DISTANCE_WINDOW_LOG: str = "27"

UV_INSTALLER_POSIX: str = "https://astral.sh/uv/install.sh"
UV_INSTALLER_WINDOWS: str = "https://astral.sh/uv/install.ps1"

DOWNLOAD_USER_AGENT: str = "curl/8"

SYSCONFIG_SCHEME_VENV: str = "venv"
SYSCONFIG_PATH_SCRIPTS: str = "scripts"
PYTHON_EXECUTABLE: str = "python"
UV_EXECUTABLE: str = "uv"

ENV_UV_EXTRA_INDEX_URL: str = "UV_EXTRA_INDEX_URL"
ENV_UV_LINK_MODE: str = "UV_LINK_MODE"
UV_LINK_MODE_COPY: str = "copy"

PACKAGE_PREFIX: str = "tabsdata"

CACHE_CLEAN_TIMEOUT: int = 120
INDEX_LINK_PATTERN: str = r">\s*(tabsdata[A-Za-z0-9._-]*)\s*<"

PACKAGES_TD: list[str] = ["tabsdata[all]"]
APT_GET_EXECUTABLE: str = "apt-get"

STARTED: float = time.monotonic()


def die(description: str, cause: BaseException | None = None) -> NoReturn:
    if cause is None:
        print(f"{description} failed", file=sys.stderr)
        sys.exit(1)
    print(f"{description} failed: {type(cause).__name__}: {cause}", file=sys.stderr)
    streams = (
        (getattr(cause, "stdout", None), "stdout"),
        (getattr(cause, "stderr", None), "stderr"),
    )
    for stream, name in streams:
        if stream:
            text = (
                stream if isinstance(stream, str) else stream.decode(errors="replace")
            )
            print(f"--- {name} of the failed command ---", file=sys.stderr)
            print(text.rstrip(), file=sys.stderr)
    traceback.print_exception(cause, file=sys.stderr)
    sys.exit(1)


def trace(message: str) -> None:
    elapsed = time.monotonic() - STARTED
    print(f">> [{elapsed:8.2f}s] {message}", flush=True)
    sys.stderr.flush()


def step(description: str, function: Callable[..., Any], *arguments: Any) -> Any:
    started = time.monotonic()
    trace(f"BEGIN {description}")
    result = None
    try:
        result = function(*arguments)
    except Exception as cause:
        trace(f"FAILED {description} after {time.monotonic() - started:.2f}s")
        die(description, cause)
    trace(f"END   {description} in {time.monotonic() - started:.2f}s")
    return result


def windows() -> bool:
    return platform.system() == "Windows"


def run(command: list[str], **kwargs: Any) -> None:
    started = time.monotonic()
    trace(f"RUN {' '.join(command)}")
    subprocess.run(command, check=True, **kwargs)
    trace(f"RAN in {time.monotonic() - started:.2f}s: {os.path.basename(command[0])}")


def download(url: str, target: str) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": DOWNLOAD_USER_AGENT})
    with urllib.request.urlopen(request) as response:
        payload: bytes = response.read()
    with open(target, "wb") as file:
        file.write(payload)


def on_path(name: str) -> str | None:
    # noinspection deprecation
    return shutil.which(name)


def uv_binary(workspace: dict[str, str]) -> str:
    name = executable_name(UV_EXECUTABLE)
    candidate = os.path.join(workspace["tools_bin_home"], name)
    if os.path.isfile(candidate):
        return candidate
    return on_path(name) or name


def scripts_folder(root: str) -> str:
    paths = sysconfig.get_paths(
        scheme=SYSCONFIG_SCHEME_VENV,
        vars={"base": str(root), "platbase": str(root)},
    )
    return paths[SYSCONFIG_PATH_SCRIPTS]


def executable_suffix() -> str:
    return Path(sys.executable).suffix


def executable_name(name: str) -> str:
    return name + executable_suffix()


def venv_python(virtual_environment: str) -> str:
    return os.path.join(
        scripts_folder(virtual_environment), executable_name(PYTHON_EXECUTABLE)
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="venvbuilder",
        description="Build the relocatable tabsdata python virtual environments.",
    )
    parser.add_argument(
        "-p",
        "--python",
        dest="python_version",
        default=PYTHON_VERSION,
        help=f"Python version (default: {PYTHON_VERSION})",
    )
    parser.add_argument(
        "--requirements-td",
        dest="requirements_td",
        default="",
        help="Complementary packages for td (default: none)",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        default="",
        help="Output folder (mandatory)",
    )
    parser.add_argument(
        "--version",
        dest="version",
        required=True,
        help="Tabsdata version to install",
    )
    parser.add_argument(
        "--storage-config",
        dest="storage_config",
        default="",
        help="Storage configuration used to copy the image (default: none)",
    )
    parser.add_argument(
        "--transporter-request",
        dest="transporter_request",
        default="",
        help="Transporter request file written when the image is sent",
    )
    parser.add_argument(
        "--transporter-response",
        dest="transporter_response",
        default="",
        help="Transporter response file written when the image is sent",
    )
    return parser.parse_args()


def validate_arguments(arguments: argparse.Namespace) -> None:
    if not arguments.python_version:
        print("Python version must not be empty", file=sys.stderr)
        sys.exit(2)
    matched = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", arguments.python_version)
    if not matched:
        print(
            "Python version must have the form major.minor.patch, for example 3.12.13",
            file=sys.stderr,
        )
        sys.exit(2)
    major, minor, patch = (int(group) for group in matched.groups())
    if major != PYTHON_MAJOR or minor != PYTHON_MINOR or patch < PYTHON_MINIMUM_PATCH:
        print(
            f"Python version must be {PYTHON_MAJOR}.{PYTHON_MINOR}."
            f"{PYTHON_MINIMUM_PATCH} or higher within the "
            f"{PYTHON_MAJOR}.{PYTHON_MINOR} series",
            file=sys.stderr,
        )
        sys.exit(2)

    if arguments.requirements_td and not os.path.isfile(arguments.requirements_td):
        print(
            f"Requirements file for td not found: {arguments.requirements_td}",
            file=sys.stderr,
        )
        sys.exit(1)

    if not arguments.output:
        print("Output folder is mandatory", file=sys.stderr)
        sys.exit(2)

    if arguments.storage_config and not os.path.isfile(arguments.storage_config):
        print(
            f"Storage configuration file not found: {arguments.storage_config}",
            file=sys.stderr,
        )
        sys.exit(1)

    if arguments.storage_config and not arguments.transporter_request:
        print(
            "Transporter request file is mandatory when a storage "
            "configuration is given",
            file=sys.stderr,
        )
        sys.exit(2)

    if arguments.storage_config and not arguments.transporter_response:
        print(
            "Transporter response file is mandatory when a storage "
            "configuration is given",
            file=sys.stderr,
        )
        sys.exit(2)


def setup_workspace(arguments: argparse.Namespace) -> dict[str, str]:
    work = tempfile.mkdtemp(prefix="tdvenvs.")
    output: str = str(arguments.output)
    root = os.path.abspath(output)
    uv_home = os.path.join(root, UV_FOLDER)
    uv_python_home = os.path.join(uv_home, UV_PYTHON_FOLDER)
    uv_kenvs_home = os.path.join(uv_home, UV_KENVS_FOLDER)
    tools_home = os.path.join(root, TOOLS_FOLDER)
    tools_bin_home = os.path.join(tools_home, TOOLS_BIN_FOLDER)
    tools_kenvs_home = os.path.join(tools_home, TOOLS_KENVS_FOLDER)
    workspace = {
        "work": work,
        "uv_home": uv_home,
        "uv_python_home": uv_python_home,
        "uv_kenvs_home": uv_kenvs_home,
        "tools_home": tools_home,
        "tools_bin_home": tools_bin_home,
        "tools_kenvs_home": tools_kenvs_home,
        "output_image": os.path.join(output, f"{IMAGE_NAME}{IMAGE_SUFFIX}"),
        "virtual_environment_td": os.path.join(uv_kenvs_home, VENV_TD),
        "virtual_environment_z": os.path.join(tools_kenvs_home, VENV_Z),
    }
    os.makedirs(tools_bin_home, exist_ok=True)
    os.environ["UV_PYTHON_INSTALL_DIR"] = uv_python_home
    os.environ[ENV_UV_LINK_MODE] = (
        os.environ.get(ENV_UV_LINK_MODE, "").strip() or UV_LINK_MODE_COPY
    )
    print(f">> Using Python {arguments.python_version} in work folder {work}")
    print(f">> Using uv home {uv_home}")
    print(f">> Using uv link mode {os.environ[ENV_UV_LINK_MODE]}")
    return workspace


def install_prerequisites() -> None:
    if windows() or on_path(APT_GET_EXECUTABLE) is None:
        print(">> Skipping prerequisites; apt-get is not available")
        return
    os.environ["DEBIAN_FRONTEND"] = "noninteractive"
    retries = ["-o", "Acquire::Retries=5", "-o", "Acquire::http::Timeout=30"]
    run(["apt-get", *retries, "update"])
    run(["apt-get", *retries, "install", "--yes", "curl", "ca-certificates"])


def uv_install_environment(workspace: dict[str, str]) -> dict[str, str]:
    environment: dict[str, str] = {key: value for key, value in os.environ.items()}
    environment["UV_INSTALL_DIR"] = workspace["tools_bin_home"]
    environment["UV_UNMANAGED_INSTALL"] = workspace["tools_bin_home"]
    return environment


def install_uv(workspace: dict[str, str]) -> None:
    if windows():
        installer = os.path.join(workspace["work"], "uv-install.ps1")
        download(UV_INSTALLER_WINDOWS, installer)
        run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                installer,
            ],
            env=uv_install_environment(workspace),
        )
    else:
        installer = os.path.join(workspace["work"], "uv-install.sh")
        download(UV_INSTALLER_POSIX, installer)
        run(["sh", installer], env=uv_install_environment(workspace))
    os.environ["PATH"] = (
        workspace["tools_bin_home"] + os.pathsep + os.environ.get("PATH", "")
    )
    run([uv_binary(workspace), "--version"])


def indexed_packages(index_url: str) -> list[str]:
    try:
        request = urllib.request.Request(
            index_url, headers={"User-Agent": DOWNLOAD_USER_AGENT}
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            listing = response.read().decode(errors="replace")
    except OSError as cause:
        print(f">> Could not list {index_url}: {cause}", file=sys.stderr)
        return []
    return sorted(set(re.findall(INDEX_LINK_PATTERN, listing)))


def package_names(index_url: str) -> list[str]:
    names: set[str] = set()
    names.update(PACKAGES_TD)
    declared = {name.split("[", 1)[0] for name in names}
    return sorted(declared.union(indexed_packages(index_url)))


def clean_cache(workspace: dict[str, str]) -> None:
    index_url = os.environ.get(ENV_UV_EXTRA_INDEX_URL, "").strip()
    if not index_url:
        print(">> Skipping the cache clean; no extra index in use")
        return
    packages = package_names(index_url)
    print(f">> Cleaning {len(packages)} {PACKAGE_PREFIX} package(s) from the uv cache")
    command = [uv_binary(workspace), "cache", "clean", "--force", *packages]
    try:
        run(command, timeout=CACHE_CLEAN_TIMEOUT)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        print(f">> Skipping the cache clean; {error}")


def install_python(arguments: argparse.Namespace, workspace: dict[str, str]) -> None:
    run([uv_binary(workspace), "python", "install", arguments.python_version])
    run([uv_binary(workspace), "python", "find", arguments.python_version])


def remove_virtual_environment(virtual_environment: str) -> None:
    if os.path.isdir(virtual_environment):
        print(f">> Removing the existing virtual environment {virtual_environment}")
        shutil.rmtree(virtual_environment)


def create_compressor_environment(
    arguments: argparse.Namespace, workspace: dict[str, str]
) -> None:
    virtual_environment_z = workspace["virtual_environment_z"]
    remove_virtual_environment(virtual_environment_z)
    run([
        uv_binary(workspace),
        "venv",
        "--python-preference",
        "only-managed",
        "--python",
        arguments.python_version,
        virtual_environment_z,
    ])
    run([
        uv_binary(workspace),
        "pip",
        "install",
        "--python",
        venv_python(virtual_environment_z),
        ZSTANDARD_PACKAGE,
    ])


def create_one_virtual_environment(
    arguments: argparse.Namespace,
    workspace: dict[str, str],
    virtual_environment: str,
) -> None:
    remove_virtual_environment(virtual_environment)
    run([
        uv_binary(workspace),
        "venv",
        "--seed",
        "--relocatable",
        "--python-preference",
        "only-managed",
        "--python",
        arguments.python_version,
        virtual_environment,
    ])
    run([
        uv_binary(workspace),
        "pip",
        "install",
        "--python",
        venv_python(virtual_environment),
        "--upgrade",
        "pip",
    ])


def create_virtual_environment(
    arguments: argparse.Namespace, workspace: dict[str, str]
) -> None:
    create_one_virtual_environment(
        arguments, workspace, workspace["virtual_environment_td"]
    )


def has_requirements(file: str) -> bool:
    if not file or not os.path.isfile(file):
        return False
    with open(file, encoding="utf-8") as reader:
        return any(
            line.strip() and not line.lstrip().startswith("#") for line in reader
        )


def pinned(packages: list[str], version: str) -> list[str]:
    return [f"{package}=={version}" for package in packages]


def install_one_set_of_packages(
    workspace: dict[str, str],
    virtual_environment: str,
    packages: list[str],
    requirements: str,
) -> None:
    command = [
        uv_binary(workspace),
        "pip",
        "install",
        "--python",
        venv_python(virtual_environment),
        *packages,
    ]
    if has_requirements(requirements):
        command += ["-r", requirements]
    run(command)


def install_packages(arguments: argparse.Namespace, workspace: dict[str, str]) -> None:
    install_one_set_of_packages(
        workspace,
        workspace["virtual_environment_td"],
        pinned(PACKAGES_TD, arguments.version),
        arguments.requirements_td,
    )


def verify_image(workspace: dict[str, str]) -> None:
    for folder in (
        workspace["uv_python_home"],
        workspace["virtual_environment_td"],
    ):
        if not os.path.isdir(folder):
            print(f"Expected folder is missing: {folder}", file=sys.stderr)
            sys.exit(1)


COMPRESS_SCRIPT = """
import sys
import zstandard

source, target = sys.argv[1], sys.argv[2]
level, threads, window_log = int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
parameters = zstandard.ZstdCompressionParameters.from_level(
    level,
    threads=threads,
    window_log=window_log,
    write_content_size=True,
)
compressor = zstandard.ZstdCompressor(compression_params=parameters)
with open(source, "rb") as reader, open(target, "wb") as writer:
    compressor.copy_stream(reader, writer)
"""


def write_image(arguments: argparse.Namespace, workspace: dict[str, str]) -> None:
    output_image = workspace["output_image"]
    os.makedirs(arguments.output, exist_ok=True)
    archive = os.path.join(workspace["work"], f"{IMAGE_NAME}.tar")
    with tarfile.open(archive, "w") as image:
        for name in (UV_PYTHON_FOLDER, UV_KENVS_FOLDER):
            image.add(os.path.join(workspace["uv_home"], name), arcname=name)
    script = os.path.join(workspace["work"], "compress.py")
    with open(script, "w") as writer:
        writer.write(COMPRESS_SCRIPT)
    run([
        venv_python(workspace["virtual_environment_z"]),
        script,
        archive,
        output_image,
        ARCHIVE_COMPRESSION_LEVEL,
        ARCHIVE_COMPRESSION_THREADS,
        ARCHIVE_LONG_DISTANCE_WINDOW_LOG,
    ])
    shutil.rmtree(workspace["work"], ignore_errors=True)


def send_image(arguments: argparse.Namespace, workspace: dict[str, str]) -> None:
    if not arguments.storage_config:
        print(">> Skipping the image upload; no storage configuration given")
        return
    run([
        venv_python(workspace["virtual_environment_td"]),
        "-m",
        ARCHIVE_MODULE,
        "--image",
        workspace["output_image"],
        "--storage-config",
        arguments.storage_config,
        "--transporter-request",
        arguments.transporter_request,
        "--transporter-response",
        arguments.transporter_response,
    ])


def main() -> None:
    start = STARTED

    arguments = parse_arguments()
    validate_arguments(arguments)
    workspace = setup_workspace(arguments)
    step("Installing prerequisites", install_prerequisites)
    step("Installing uv", install_uv, workspace)
    step("Cleaning the cache", clean_cache, workspace)
    step("Installing Python", install_python, arguments, workspace)
    step(
        "Creating the compressor environment",
        create_compressor_environment,
        arguments,
        workspace,
    )
    step(
        "Creating the virtual environments",
        create_virtual_environment,
        arguments,
        workspace,
    )
    step("Installing packages", install_packages, arguments, workspace)
    step("Verifying the image", verify_image, workspace)
    step("Writing the image", write_image, arguments, workspace)
    step("Sending the image", send_image, arguments, workspace)

    hours = (time.monotonic() - start) / 3600
    print(f">> Done! Image written to {workspace['output_image']}")
    print(f">> Elapsed time: {hours:.4f} hours")


if __name__ == "__main__":
    main()
