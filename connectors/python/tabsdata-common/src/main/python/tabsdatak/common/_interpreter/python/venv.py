#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os
import platform
import shutil
import site
import stat
import subprocess
import sys
import sysconfig
import tarfile
import time
from dataclasses import dataclass, field
from pathlib import Path

import zstandard
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

from tabsdatak.common._interpreter.python.binary import SYSCONFIG_VAR_EXE
from tabsdatak.common._interpreter.python.error import (
    BinaryNotFoundError,
    FeatureDetectionError,
    IncompleteEnvironmentError,
    UnusableEnvironmentError,
    VenvCreationError,
    WheelFileMissingError,
)
from tabsdatak.common._interpreter.python.package import Package
from tabsdatak.common._interpreter.python.recipe import (
    InstallRecipe,
    PackageRecipe,
    TabsdataEdition,
    VenvRecipe,
)
from tabsdatak.common._resources.reader import td_resource

logger = logging.getLogger(__name__)


CLEAR_ATTEMPTS = 8
CLEAR_DELAY = 1

TABSDATA_PACKAGE = "tabsdata"
TABSDATA_TOOLKIT_PACKAGE = "tabsdata-toolkit"
TABSDATA_AGENT_PACKAGE = "tabsdata-agent"

TABSDATA_CORE_PACKAGES: tuple[Package, ...] = (
    Package(TABSDATA_PACKAGE, mandatory=True),
    Package(TABSDATA_TOOLKIT_PACKAGE),
    Package(TABSDATA_AGENT_PACKAGE),
)

TABSDATA_CONNECTORS_NAMES: tuple[str, ...] = (
    "tabsdata-api",
    "tabsdata-backend",
    "tabsdata-cli",
    "tabsdata-common",
    "tabsdata-conn-azureblob",
    "tabsdata-conn-bigquery",
    "tabsdata-conn-clickhouse",
    "tabsdata-conn-common",
    "tabsdata-conn-common-file",
    "tabsdata-conn-common-sql",
    "tabsdata-conn-databricks",
    "tabsdata-conn-db2",
    "tabsdata-conn-dev",
    "tabsdata-conn-druid",
    "tabsdata-conn-gcs",
    "tabsdata-conn-kafka",
    "tabsdata-conn-localfile",
    "tabsdata-conn-mongodb",
    "tabsdata-conn-mssql",
    "tabsdata-conn-mysql",
    "tabsdata-conn-oracle",
    "tabsdata-conn-postgres",
    "tabsdata-conn-s3",
    "tabsdata-conn-salesforce",
    "tabsdata-conn-snowflake",
    "tabsdata-conn-starrocks",
    "tabsdata-conn-stream-stage",
    "tabsdata-control-plane",
    "tabsdata-execution-plane",
    "tabsdata-medallion",
    "tabsdata-sdk",
    "tabsdata-server",
)

CONNECTORS_RELATIVE_PATH = Path("connectors") / "python"

TABSDATA_ENTERPRISE_PROJECT = "tabsdata-ee"

BUILD_MANIFEST_KEY_ENTERPRISE = "X-Enterprise"
BUILD_MANIFEST_VALUE_TRUE = "true"

REQUIREMENTS_DEV_FILE = "requirements-dev.txt"
CONSTRAINTS_RESOURCE = "resources/lock/tabsdata-requirements.txt"
CONSTRAINTS_DEV_RESOURCE = "resources/lock/tabsdata-requirements-dev.txt"

# Archive compression knobs. Level 3 keeps build-time CPU low; long=27
# (128 MB window) catches cross-wheel repetition the default window
# misses.
ARCHIVE_COMPRESSION_LEVEL = 3
ARCHIVE_COMPRESSION_THREADS = -1
ARCHIVE_LONG_DISTANCE_WINDOW_LOG = 27

LIBEXEC_BUILD_RELATIVE_PATH = Path("target") / "python" / "libexec" / "tabsdata"
LIBEXEC_INSTALL_RELATIVE_PATH = Path("libexec") / "tabsdata"

PYTHON_BINARIES: tuple[str, ...] = ("tdk", "td_pyvenv", "td_resource")

RUST_BINARIES: tuple[str, ...] = ("tdkserver", "tdk_kcheck", "td_apiserver")

SHEBANG_PREFIX = "#!"
SHEBANG_ENV = "/usr/bin/env"
UV_LAUNCHER_EXEC_PREFIX = "'''exec' "


@dataclass(frozen=True)
class EnvironmentProfile:
    root: Path
    python_version: str
    edition: TabsdataEdition
    packages: tuple[PackageRecipe, ...]


@dataclass(frozen=True)
class PythonEnvironment:
    root: Path = field(default_factory=lambda: Path(sys.prefix))

    def resolve(self) -> EnvironmentProfile:
        packages = self.tdpackages()
        return EnvironmentProfile(
            root=self.root,
            python_version=self.version(),
            edition=self.edition(packages),
            packages=packages,
        )

    def verify(self) -> EnvironmentProfile:
        profile = self.resolve()
        expected = self.expected_packages(profile.edition)
        installed = self.installed_packages()
        missing = [
            name for name in expected if canonicalize_name(name) not in installed
        ]
        if missing:
            raise IncompleteEnvironmentError(
                root=self.root,
                edition=profile.edition.value,
                expected=len(expected),
                missing_count=len(missing),
                missing=", ".join(missing),
            )
        self.verify_launchers()
        return profile

    @staticmethod
    def expected_packages(edition: TabsdataEdition) -> tuple[str, ...]:
        names = [TABSDATA_PACKAGE, TABSDATA_TOOLKIT_PACKAGE, *TABSDATA_CONNECTORS_NAMES]
        if edition == TabsdataEdition.ENTERPRISE:
            names.append(TABSDATA_AGENT_PACKAGE)
        return tuple(sorted(set(names), key=str.casefold))

    def verify_launchers(self) -> None:
        python = self.python()
        if not os.access(python, os.X_OK):
            raise UnusableEnvironmentError(
                root=self.root,
                detail=(
                    f"its interpreter '{python}' is missing or not executable. The"
                    " environment was most likely moved or removed after it was"
                    " created."
                ),
            )
        for name, shebang in self.stale_launchers():
            raise UnusableEnvironmentError(
                root=self.root,
                detail=(
                    f"the console script '{name}' points at the interpreter"
                    f" '{shebang}', which no longer exists. The environment was most"
                    " likely created at a different path and then moved; recreate it"
                    " at its current location."
                ),
            )
        for name in (*PYTHON_BINARIES, *RUST_BINARIES):
            try:
                self.binary(name)
            except BinaryNotFoundError as error:
                raise UnusableEnvironmentError(
                    root=self.root,
                    detail=f"binary '{name}' is not installed in it.",
                ) from error

    def stale_launchers(self) -> list[tuple[str, str]]:
        stale: list[tuple[str, str]] = []
        scripts = self.scripts()
        for name in PYTHON_BINARIES:
            launcher = scripts / (name + sysconfig.get_config_var(SYSCONFIG_VAR_EXE))
            if not launcher.is_file():
                continue
            interpreter = launcher_interpreter(launcher)
            if interpreter is not None and not Path(interpreter).exists():
                stale.append((name, interpreter))
        return stale

    @staticmethod
    def version() -> str:
        return platform.python_version()

    @staticmethod
    def edition(packages: tuple[PackageRecipe, ...]) -> TabsdataEdition:
        recipe = next(
            (entry for entry in packages if entry.name == TABSDATA_PACKAGE),
            None,
        )
        if recipe is None:
            raise FeatureDetectionError(package=TABSDATA_PACKAGE)
        manifest = Package(TABSDATA_PACKAGE).manifest()
        if manifest is not None:
            return PythonEnvironment.edition_from_manifest(manifest)
        return PythonEnvironment.edition_from_location(recipe)

    @staticmethod
    def edition_from_manifest(manifest: dict) -> TabsdataEdition:
        flag = str(manifest.get(BUILD_MANIFEST_KEY_ENTERPRISE, "")).lower()
        if flag == BUILD_MANIFEST_VALUE_TRUE:
            return TabsdataEdition.ENTERPRISE
        return TabsdataEdition.OPEN_SOURCE

    @staticmethod
    def edition_from_location(recipe: PackageRecipe) -> TabsdataEdition:
        if recipe.location is None:
            raise FeatureDetectionError(package=TABSDATA_PACKAGE)
        if recipe.location.name == TABSDATA_ENTERPRISE_PROJECT:
            return TabsdataEdition.ENTERPRISE
        return TabsdataEdition.OPEN_SOURCE

    @staticmethod
    def tdpackages() -> tuple[PackageRecipe, ...]:
        return tuple(
            recipe
            for recipe in (package.recipe() for package in TABSDATA_CORE_PACKAGES)
            if recipe is not None
        )

    @staticmethod
    def synthesise() -> tuple[list[str], str]:
        python_version = (
            f"{sys.version_info.major}."
            f"{sys.version_info.minor}."
            f"{sys.version_info.micro}"
        )
        tabsdata_dependencies = PythonEnvironment.locked_packages()
        extra_dependencies: set[str] = set()
        for canonical in PythonEnvironment.installed_packages():
            if (
                canonical.startswith(TABSDATA_PACKAGE)
                or canonical in tabsdata_dependencies
            ):
                continue
            recipe = Package(canonical).recipe()
            if recipe is None or recipe.recipe != InstallRecipe.REGISTRY:
                continue
            if recipe.version is None:
                continue
            extra_dependencies.add(f"{canonical}=={recipe.version}")
        if not extra_dependencies:
            return [], python_version
        sorted_extra_dependencies = sorted(extra_dependencies, key=str.casefold)
        constraints = "\n".join(sorted_extra_dependencies) + "\n"
        arguments = [
            "pip",
            "compile",
            "--universal",
            "--quiet",
            "--no-progress",
            "--no-header",
            "--no-annotate",
            "--python-version",
            f"{sys.version_info.major}.{sys.version_info.minor}",
            "-",
        ]
        try:
            stdout = PythonEnvironment.uv(
                [
                    *arguments,
                    "--offline",
                ],
                stdin=constraints,
            )
        except VenvCreationError:
            stdout = PythonEnvironment.uv(arguments, stdin=constraints)
        resolved_requirements = [
            line.strip()
            for line in stdout.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        return resolved_requirements, python_version

    @staticmethod
    def installed_packages() -> frozenset[str]:
        roots: list[str] = list(site.getsitepackages())
        user_site = site.getusersitepackages()
        if user_site:
            roots.append(user_site)
        names: set[str] = set()
        for entry in roots:
            root = Path(entry)
            if not root.is_dir():
                continue
            try:
                children = root.iterdir()
            except OSError:
                continue
            for child in children:
                name = child.name
                if name.endswith(".dist-info") or name.endswith(".egg-info"):
                    stem = name.rpartition(".")[0]
                    package_name = stem.partition("-")[0]
                    if package_name:
                        names.add(canonicalize_name(package_name))
        return frozenset(names)

    @staticmethod
    def locked_packages() -> frozenset[str]:
        resources = [CONSTRAINTS_RESOURCE]
        recipe = Package(TABSDATA_PACKAGE).recipe()
        if recipe is not None and recipe.recipe == InstallRecipe.EDITABLE:
            resources.append(CONSTRAINTS_DEV_RESOURCE)
        names: set[str] = set()
        for resource in resources:
            lock_path = td_resource(resource)
            for raw in lock_path.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    names.add(canonicalize_name(Requirement(line).name))
                except InvalidRequirement:
                    continue
        return frozenset(names)

    def clear(self) -> None:
        if not self.root.exists():
            return
        logger.debug(f"Clearing {self.root}")

        def unlock(function, path, cause) -> None:
            if not isinstance(cause, PermissionError):
                raise cause
            os.chmod(path, os.stat(path).st_mode | stat.S_IWRITE)
            function(path)

        for attempt in range(CLEAR_ATTEMPTS):
            try:
                shutil.rmtree(self.root, onexc=unlock)
                return
            except OSError as error:
                if attempt == CLEAR_ATTEMPTS - 1:
                    locked = getattr(error, "filename", None) or self.root
                    raise VenvCreationError(
                        detail=(
                            f"Could not clear '{self.root}': access to '{locked}' is"
                            " denied. An executable cannot be deleted while a process"
                            " is running it, so a process using this virtual"
                            " environment is most likely still up. Stop all the"
                            " instance services before rebuilding the virtual"
                            f" environments. Cause: {error}"
                        ),
                        cause=error,
                    ) from error
                logger.warning(f"Clearing {self.root} failed ({error}); retrying...")
                time.sleep(CLEAR_DELAY)

    def create(
        self,
        recipe: VenvRecipe,
        *,
        python_version: str,
        verbose: bool = False,
    ) -> None:
        self.clear()
        self.install_venv(python_version, verbose=verbose)
        self.upgrade_pip(verbose=verbose)
        self.install_requirements_dev(recipe.packages, verbose=verbose)
        toolkit, others = self.split_packages(recipe.packages)
        if toolkit:
            self.install_tabsdata_packages(toolkit, verbose=verbose)
        if others:
            self.install_tabsdata_packages(others, verbose=verbose)
        source_root = Path(sys.prefix)
        for package in recipe.packages:
            for binary in package.binaries:
                binary.link(source_root, self.root)
        self.link_libexec_launchers(recipe.packages)
        self.compile(verbose=verbose)

    def link_libexec_launchers(self, packages: tuple[PackageRecipe, ...]) -> None:
        target = self.root / LIBEXEC_INSTALL_RELATIVE_PATH
        seen: set[Path] = set()
        for package in packages:
            if package.recipe != InstallRecipe.EDITABLE or package.location is None:
                continue
            source = package.location / LIBEXEC_BUILD_RELATIVE_PATH
            if source in seen or not source.is_dir():
                continue
            seen.add(source)
            target.mkdir(parents=True, exist_ok=True)
            for entry in source.iterdir():
                if not entry.is_file():
                    continue
                link = target / entry.name
                if link.is_symlink() or link.exists():
                    try:
                        link.unlink()
                    except OSError:
                        continue
                try:
                    link.symlink_to(entry.resolve())
                except OSError as error:
                    logger.warning(
                        f"Failed to link libexec launcher '{entry.name}': {error}"
                    )

    def install_requirements_dev(
        self,
        packages: tuple[PackageRecipe, ...],
        *,
        verbose: bool = False,
    ) -> None:
        tabsdata = next(
            (package for package in packages if package.name == TABSDATA_PACKAGE),
            None,
        )
        if tabsdata is None or tabsdata.recipe != InstallRecipe.EDITABLE:
            return
        if tabsdata.location is None:
            raise VenvCreationError(
                detail=f"Editable package '{tabsdata.name}' is missing a location",
            )
        requirements = tabsdata.location / REQUIREMENTS_DEV_FILE
        if not requirements.is_file():
            raise VenvCreationError(
                detail=f"Requirements file '{requirements}' not found",
            )
        try:
            constraints = td_resource(CONSTRAINTS_DEV_RESOURCE)
        except FileNotFoundError as error:
            raise VenvCreationError(detail=str(error), cause=error)
        command = ["pip", "install"]
        if not verbose:
            command.append("--quiet")
        command.extend([
            "--python",
            str(self.python()),
            "-r",
            str(requirements),
            "-c",
            str(constraints),
        ])
        self.uv(command, verbose=verbose)

    @staticmethod
    def split_packages(
        packages: tuple[PackageRecipe, ...],
    ) -> tuple[tuple[PackageRecipe, ...], tuple[PackageRecipe, ...]]:
        toolkit = tuple(
            package for package in packages if package.name == TABSDATA_TOOLKIT_PACKAGE
        )
        product = tuple(
            package for package in packages if package.name != TABSDATA_TOOLKIT_PACKAGE
        )
        return toolkit, product

    def install_venv(self, python_version: str, *, verbose: bool = False) -> None:
        command = ["venv"]
        if not verbose:
            command.append("--quiet")
        command.extend([
            "--clear",
            "--seed",
            "--relocatable",
            "--python-preference",
            "only-managed",
            "--python",
            python_version,
            str(self.root),
        ])
        self.uv(command, verbose=verbose)

    def upgrade_pip(self, *, verbose: bool = False) -> None:
        command = ["pip", "install"]
        if not verbose:
            command.append("--quiet")
        command.extend([
            "--python",
            str(self.python()),
            "--upgrade",
            "pip",
        ])
        self.uv(command, verbose=verbose)

    def install_tabsdata_packages(
        self,
        packages: tuple[PackageRecipe, ...],
        *,
        verbose: bool = False,
    ) -> None:
        command = ["pip", "install"]
        if not verbose:
            command.append("--quiet")
        command.extend([
            "--python",
            str(self.python()),
        ])
        if any(package.recipe == InstallRecipe.EDITABLE for package in packages):
            command.extend([
                "--config-settings",
                "editable_mode=compat",
                "--no-build-isolation",
            ])
        for package in packages:
            command.extend(self.install_tabsdata_packages_arguments(package))
        self.uv(command, verbose=verbose)

    @staticmethod
    def install_tabsdata_packages_arguments(package: PackageRecipe) -> list[str]:
        if package.recipe == InstallRecipe.EDITABLE:
            if package.location is None:
                raise VenvCreationError(
                    detail=f"Editable package '{package.name}' is missing a location",
                )
            return ["-e", str(package.location)]
        if package.recipe == InstallRecipe.REGISTRY:
            return [
                f"{package.name}=={package.version}"
                if package.version
                else package.name
            ]
        if package.recipe == InstallRecipe.WHEEL:
            if package.location is None:
                raise VenvCreationError(
                    detail=f"Wheel package '{package.name}' is missing a location",
                )
            if not package.location.exists():
                raise WheelFileMissingError(
                    package=package.name,
                    location=package.location,
                )
            return [str(package.location)]
        raise VenvCreationError(
            detail=(
                f"Install method '{package.recipe.name}' for package"
                f" '{package.name}' is not supported in target environments"
            ),
        )

    def scripts(self) -> Path:
        paths = sysconfig.get_paths(
            scheme="venv",
            vars={"base": str(self.root), "platbase": str(self.root)},
        )
        return Path(paths["scripts"])

    def python(self) -> Path:
        # noinspection PyDeprecation
        return self.scripts() / ("python" + sysconfig.get_config_var(SYSCONFIG_VAR_EXE))

    def lib(self) -> Path:
        paths = sysconfig.get_paths(
            scheme="venv",
            vars={"base": str(self.root), "platbase": str(self.root)},
        )
        return Path(paths["purelib"]).parent

    def compile(self, *, verbose: bool = False) -> None:
        command = [
            str(self.python()),
            "-m",
            "compileall",
            "-j",
            "0",
        ]
        if not verbose:
            command.append("-q")
        command.append(str(self.lib()))
        try:
            subprocess.run(command, capture_output=not verbose)
        except Exception as error:
            logger.warning(f"Precompiling {self.root} failed ({error}); skipping")

    def binary(self, name: str) -> Path:
        scripts = self.scripts()
        # noinspection PyDeprecation
        executable = name + sysconfig.get_config_var(SYSCONFIG_VAR_EXE)
        candidate = scripts / executable
        if candidate.is_file():
            return candidate
        candidate = self.root / LIBEXEC_INSTALL_RELATIVE_PATH / executable
        if candidate.is_file():
            return candidate
        raise BinaryNotFoundError(name=name, scripts=scripts)

    def archive(self, archive_path: Path) -> None:
        if archive_path.exists():
            archive_path.unlink()
        parameters = zstandard.ZstdCompressionParameters.from_level(
            ARCHIVE_COMPRESSION_LEVEL,
            threads=ARCHIVE_COMPRESSION_THREADS,
            window_log=ARCHIVE_LONG_DISTANCE_WINDOW_LOG,
            write_content_size=True,
        )
        compressor = zstandard.ZstdCompressor(compression_params=parameters)
        with open(archive_path, "wb") as raw:
            with compressor.stream_writer(raw, closefd=False) as stream:
                with tarfile.open(fileobj=stream, mode="w|") as tar:
                    tar.add(self.root, arcname=self.root.name)

    @staticmethod
    def install(python_version: str, *, verbose: bool = False) -> None:
        PythonEnvironment.uv(
            ["python", "install", python_version],
            verbose=verbose,
        )
        PythonEnvironment.uv(
            ["python", "find", python_version],
            verbose=verbose,
        )

    @staticmethod
    def uv(
        arguments: list[str],
        *,
        stdin: str | None = None,
        verbose: bool = False,
    ) -> str:
        try:
            result = subprocess.run(
                [
                    "uv",
                    *arguments,
                ],
                check=False,
                capture_output=not verbose,
                text=True,
                input=stdin,
            )
        except FileNotFoundError as error:
            raise VenvCreationError(
                detail=f"Binary 'uv' not found: {error}",
                cause=error,
            )
        if result.returncode != 0:
            captured = (
                f": {result.stderr.strip() or result.stdout.strip()}"
                if not verbose
                else ""
            )
            raise VenvCreationError(
                detail=(
                    f"Command 'uv {arguments[0]}' failed"
                    f" (exit {result.returncode}){captured}"
                ),
            )
        return result.stdout


def launcher_interpreter(launcher: Path) -> str | None:
    try:
        with open(launcher, encoding="utf-8", errors="replace") as reader:
            first = reader.readline().rstrip("\n")
            second = reader.readline().rstrip("\n")
    except OSError:
        return None
    if not first.startswith(SHEBANG_PREFIX):
        return None
    shebang = first[len(SHEBANG_PREFIX) :].strip()
    if shebang.startswith(SHEBANG_ENV):
        return None
    if second.startswith(UV_LAUNCHER_EXEC_PREFIX):
        return uv_launcher_interpreter(second)
    return shebang or None


def uv_launcher_interpreter(line: str) -> str | None:
    remainder = line[len(UV_LAUNCHER_EXEC_PREFIX) :].lstrip()
    if not remainder.startswith("'"):
        return None
    closing = remainder.find("'", 1)
    if closing < 0:
        return None
    return remainder[1:closing] or None
