#
# Copyright 2026 Tabs Data Inc.
#

"""tabsdatak function-bundle producer.

Pairs with the consumer side at
`tabsdatak.server._tdrunner.function._lib.bundle.extract_bundle`: this module
*builds* the `.tar.zst` (zstandard-compressed tar) that the runner
later unpacks.

Bundle layout (all paths relative to the bundle root):

    function_def.json        # info.to_json() — the manifest
    requirements.yaml        # env-builder manifest: python + packages + flags
    local_packages/<n>/...   # optional local-package staging area
    user_code/...            # the user's project tree; the runner adds
                             # this subdir (not the bundle root) to
                             # sys.path, so `info.func`'s module path
                             # is resolved against this directory

The runner only needs `function_def.json` + `user_code/` to execute.
The rest is for `prepare-py-env`, the server-side bundle inspector,
and future tooling.

`requirements.yaml` matches the legacy schema produced by
`tabsdata._utils.bundle_utils.create_requirements` so the env-builder
keeps reading a format it already understands.

`capture_source_snippet` is exposed alongside `build_bundle` for the
caller to capture the function's source separately (e.g. to pass to
the server's register API as display metadata, mirroring the legacy
pattern in `tabsdata.api.tabsdata_server._create_archive`).
"""

from __future__ import annotations

import inspect
import logging
import shutil
import tarfile
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Union

import yaml
import zstandard

from tabsdatak._info import (
    PublisherInfo,
    SubscriberInfo,
    TransformerInfo,
    _unwrap_info,
)
from tabsdatak.common._error import CommonErrorCode
from tabsdatak.common._interpreter.python.venv import PythonEnvironment

log = logging.getLogger(__name__)

# --- bundle layout constants ----------------------------------------------

BUNDLE_LOCAL_PACKAGES_DIRNAME = "local_packages"
BUNDLE_MANIFEST_FILENAME = "function_def.json"
BUNDLE_REQUIREMENTS_FILENAME = "requirements.yaml"
BUNDLE_USER_CODE_DIRNAME = "user_code"

# Archive compression knobs. Level 10 is a sweet spot for the
# text-heavy, small-payload function bundle (~ms of extra CPU vs L3
# for a meaningfully smaller artifact to upload).
_ZSTD_COMPRESSION_LEVEL = 10
_ZSTD_COMPRESSION_THREADS = -1

# requirements.yaml schema keys (legacy-compatible).
_IGNORE_UNAVAILABLE_KEY = "ignoreUnavailablePackages"
_INSTALL_DEPENDENCIES_KEY = "installPackagesDependencies"
_LOCAL_PACKAGES_KEY = "localPackages"
_PUBLIC_PACKAGES_KEY = "publicPackages"
_PYTHON_VERSION_KEY = "pythonVersion"

# Directory / glob patterns excluded when copying the user's project
# tree or local-package directories into the bundle. Mirrors the
# IGNORED_FOLDERS list from the legacy `bundle_utils.py`, trimmed to
# the universally-safe entries.
_IGNORE_PATTERNS = (
    "__pycache__",
    "*.pyc",
    "*.pyo",
    "*.pyd",
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    "build",
    "dist",
    ".eggs",
    "*.egg-info",
)

FunctionInfo = Union[PublisherInfo, TransformerInfo, SubscriberInfo]


# --- function_def.json ----------------------------------------------------


def write_function_def(info: FunctionInfo, dest_dir: Path) -> Path:
    """Write `info.to_json()` to `<dest_dir>/function_def.json` and
    return the path."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / BUNDLE_MANIFEST_FILENAME
    path.write_text(info.to_json(indent=2), encoding="utf-8")
    return path


# --- source-snippet capture -----------------------------------------------


def capture_source_snippet(info: FunctionInfo) -> str:
    """Return `inspect.getsource()` of the decorated callable.

    Exposed as a standalone helper so the caller can pass the snippet
    to the server's register API as display metadata, mirroring the
    legacy pattern in `tabsdata.api.tabsdata_server._create_archive`.
    Not written into the bundle.

    If the source can't be located (e.g. compiled from a string,
    defined in a REPL, or otherwise not file-backed) a one-line
    placeholder is returned. Capture is best-effort: failure is
    logged, not raised.
    """
    func = _unwrap_info(info.func)
    try:
        return inspect.getsource(func)
    except (OSError, TypeError) as exc:
        log.warning("could not capture source for %r: %s", func, exc)
        return f"# source unavailable: {exc}\n"


# --- requirements snapshot + python version ------------------------------


def write_requirements(
    dest_dir: Path,
    requirements: Iterable[str],
    python_version: str,
    *,
    local_packages: Iterable[Path] | None = None,
) -> Path:
    """Write the env-builder manifest to `<dest_dir>/requirements.yaml`.

    Schema (legacy-compatible with the format produced by
    `tabsdata._utils.bundle_utils.create_requirements`):

        pythonVersion:               "X.Y.Z"
        publicPackages:              [name==version, ...]
        installPackagesDependencies: false
        ignoreUnavailablePackages:   true
        localPackages:               [path, ...]   # only when provided
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    data: dict = {
        _PYTHON_VERSION_KEY: python_version,
        _PUBLIC_PACKAGES_KEY: list(requirements),
        _INSTALL_DEPENDENCIES_KEY: False,
        _IGNORE_UNAVAILABLE_KEY: True,
    }
    if local_packages:
        data[_LOCAL_PACKAGES_KEY] = [str(p) for p in local_packages]
    path = dest_dir / BUNDLE_REQUIREMENTS_FILENAME
    path.write_text(yaml.safe_dump(data, sort_keys=True), encoding="utf-8")
    return path


def validate_python_version(python_version: str, valid_versions: Iterable[str]) -> None:
    """Exact-match check: raise if `python_version` isn't in `valid_versions`."""
    allowed = list(valid_versions)
    if python_version not in allowed:
        raise CommonErrorCode.COMMON_11.exception(
            version=python_version, allowed=allowed
        )


# --- local packages ------------------------------------------------------


def collect_local_packages(packages: Iterable[Path], dest_dir: Path) -> list[Path]:
    """Copy each local-package directory into
    `<dest_dir>/local_packages/<n>/<package_basename>/`.

    Returns the list of staged target directories, in order.

    Raises:
        CommonRuntimeException (COMMON_10): an entry isn't a directory.
    """
    base = dest_dir / BUNDLE_LOCAL_PACKAGES_DIRNAME
    staged: list[Path] = []
    for index, pkg in enumerate(packages):
        if not pkg.is_dir():
            raise CommonErrorCode.COMMON_10.exception(path=pkg)
        slot = base / str(index)
        slot.mkdir(parents=True, exist_ok=True)
        target = slot / pkg.name
        shutil.copytree(
            pkg,
            target,
            ignore=shutil.ignore_patterns(*_IGNORE_PATTERNS),
            dirs_exist_ok=True,
        )
        staged.append(target)
    return staged


# --- top-level bundle build ----------------------------------------------


def build_bundle(
    info: FunctionInfo,
    project_root: Path,
    output_path: Path,
    *,
    local_packages: Iterable[Path] | None = None,
    valid_python_versions: Iterable[str] | None = None,
    include: Iterable[str] | None = None,
    requirements_yaml: Path | None = None,
) -> Path:
    """Build a function bundle at `output_path`.

    The user's project tree is staged under the dedicated
    `user_code/` subdir; bundle-owned metadata (manifest, requirements,
    local packages) lives at the bundle root. The runner only adds
    `<bundle>/user_code/` to `sys.path`, so `info.func`'s `<module>:
    <qualname>` must be importable from there. Concretely: if
    `project_root` contains a `my_pkg/` package, `info.func` should
    look like `"my_pkg.module:my_func"`.

    The function's source snippet is *not* bundled — call
    `capture_source_snippet(info)` separately if you need it (e.g. to
    pass to the register API as display metadata).

    Args:
        info: the decorated function.
        project_root: directory containing the user's package tree.
            Every non-ignored top-level entry is bundled under
            `user_code/`, unless `include` restricts the set.
        output_path: where to write the `.tar.zst`. Parent directories
            are created if missing.
        local_packages: optional list of local-package directories to
            stage under `local_packages/<n>/`.
        valid_python_versions: optional whitelist; if given and the
            producer's interpreter isn't listed, `CommonErrorCode.COMMON_11`
            is raised.
        include: optional iterable of top-level names in `project_root`
            to bundle; if `None`, every non-ignored entry is included.
        requirements_yaml: optional path to a user-supplied
            ``requirements.yaml`` to embed in the bundle verbatim
            (overriding the auto-synthesised env snapshot). Use this
            when the build env differs from the runtime env (e.g.
            cross-platform deployment) and the legacy env-builder
            schema must be controlled by hand.

    Returns:
        `output_path`.

    Raises:
        CommonRuntimeException (COMMON_9): `project_root` is not a directory.
        CommonRuntimeException (COMMON_10): a local-package entry is not a directory.
        CommonRuntimeException (COMMON_11): see `valid_python_versions`.
        OSError: I/O failure during staging or tar creation.
    """
    if not project_root.is_dir():
        raise CommonErrorCode.COMMON_9.exception(path=project_root)
    include_list = list(include) if include is not None else None

    if requirements_yaml is None:
        requirements, python_version = PythonEnvironment.synthesise()
        if valid_python_versions is not None:
            validate_python_version(python_version, valid_python_versions)
    else:
        # User-supplied requirements.yaml: trust the caller's file
        # verbatim. The interpreter-version validation is skipped
        # because the file may target a python different from the
        # producer's.
        requirements = None
        python_version = None

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        staging = Path(tmp)

        _copy_project_tree(
            project_root,
            staging / BUNDLE_USER_CODE_DIRNAME,
            include=include_list,
        )

        write_function_def(info, staging)
        if local_packages:
            collect_local_packages(local_packages, staging)
        if requirements_yaml is not None:
            shutil.copyfile(requirements_yaml, staging / BUNDLE_REQUIREMENTS_FILENAME)
        else:
            write_requirements(
                staging,
                requirements,
                python_version,
                local_packages=local_packages,
            )

        # zstd-compressed tar stream. Same shape as
        # ``interpreter.python.venv.archive``: write the raw zstd stream
        # through a ``stream_writer`` and feed it to a streaming-mode
        # tarfile. ``closefd=False`` keeps the outer file handle
        # owned by the outer ``with``, so the stream is flushed on
        # close but the file is closed exactly once.
        params = zstandard.ZstdCompressionParameters.from_level(
            _ZSTD_COMPRESSION_LEVEL,
            threads=_ZSTD_COMPRESSION_THREADS,
            write_content_size=True,
        )
        compressor = zstandard.ZstdCompressor(compression_params=params)
        with open(output_path, "wb") as raw:
            with compressor.stream_writer(raw, closefd=False) as stream:
                with tarfile.open(fileobj=stream, mode="w|") as tar:
                    tar.add(staging, arcname=".")

    return output_path


def _copy_project_tree(
    project_root: Path,
    dest: Path,
    *,
    include: list[str] | None,
) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    for entry in _iter_project_entries(project_root, include):
        target = dest / entry.name
        if entry.is_dir():
            shutil.copytree(
                entry,
                target,
                ignore=shutil.ignore_patterns(*_IGNORE_PATTERNS),
                dirs_exist_ok=True,
            )
        elif entry.is_file():
            shutil.copy2(entry, target)


def _iter_project_entries(project_root: Path, include: list[str] | None) -> list[Path]:
    entries = sorted(project_root.iterdir())
    if include is not None:
        wanted = set(include)
        entries = [e for e in entries if e.name in wanted]
    ignored = set(
        shutil.ignore_patterns(*_IGNORE_PATTERNS)(
            str(project_root), [e.name for e in entries]
        )
    )
    return [e for e in entries if e.name not in ignored]
