#
# Copyright 2026 Tabsdata Inc.
#

"""tabsdatak function-bundle producer.

Pairs with the consumer side at
`tabsdatak.server._tdrunner.function._handlers.bundle.BundleArchive`: this module
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

A downloaded bundle is read back with `read_function_source` /
`read_bundle_sources`, unpacked with `extract_bundle`, and re-based for a
code-only patch with `splice_function_source`.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import re
import shutil
import tarfile
import tempfile
from collections.abc import Container, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
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


@contextmanager
def _open_tar(archive: Path) -> Iterator[tarfile.TarFile]:
    with (
        open(archive, "rb") as raw,
        zstandard.ZstdDecompressor().stream_reader(raw) as stream,
        tarfile.open(fileobj=stream, mode="r|") as tar,
    ):
        yield tar


def _walk(archive: Path):
    """Yield `(tar, member, bundle-relative name)` for every file in a
    `.tar.zst` bundle. Streaming: read a member before advancing."""
    with _open_tar(archive) as tar:
        for member in tar:
            if member.isfile():
                yield tar, member, member.name.removeprefix("./")


def _member_text(tar: tarfile.TarFile, member: tarfile.TarInfo) -> str:
    content = tar.extractfile(member)
    return "" if content is None else content.read().decode("utf-8", errors="replace")


def function_ref(manifest: str) -> tuple[str, str] | None:
    """`(module, qualname)` of the bundled function, from the manifest
    envelope's `spec.func` (`"<module>:<qualname>"`)."""
    try:
        spec = json.loads(manifest).get("spec") or {}
    except (json.JSONDecodeError, AttributeError):
        return None
    module, _, qualname = (spec.get("func") or "").partition(":")
    return (module, qualname) if module else None


def defining_module(module: str, names: Container[str]) -> str | None:
    """Bundle path of `module` among the bundle's file `names`: the module
    resolves against `user_code/` as the runner resolves it, to
    `<module>.py` or to a package's `<module>/__init__.py`."""
    base = f"{BUNDLE_USER_CODE_DIRNAME}/{module.replace('.', '/')}"
    for candidate in (f"{base}.py", f"{base}/__init__.py"):
        if candidate in names:
            return candidate
    return None


def read_function_source(archive: Path) -> tuple[str, str] | None:
    """Return `(qualname, source)` of the module defining the bundled
    function, read back out of a `.tar.zst` bundle.

    This is the source that actually runs — unlike the registered snippet,
    which is display metadata and may have been trimmed. Returns `None` when
    the bundle carries no manifest, no `func`, or no matching module.
    """
    manifest: str | None = None
    modules: dict[str, str] = {}
    for tar, member, name in _walk(archive):
        if name == BUNDLE_MANIFEST_FILENAME:
            manifest = _member_text(tar, member)
        elif name.startswith(f"{BUNDLE_USER_CODE_DIRNAME}/") and name.endswith(".py"):
            modules[name] = _member_text(tar, member)
    ref = function_ref(manifest) if manifest is not None else None
    if ref is None:
        return None
    module, qualname = ref
    path = defining_module(module, modules)
    return None if path is None else (qualname, modules[path])


_TEXT_SUFFIXES = (
    ".cfg",
    ".ini",
    ".json",
    ".md",
    ".py",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
)
_MAX_FILE_BYTES = 128 * 1024
_MAX_TOTAL_BYTES = 512 * 1024
# The defining module is trimmed rather than dropped, but capped so it
# cannot starve its siblings.
_MAX_DEFINING_BYTES = _MAX_TOTAL_BYTES // 2
_TRIM_MARKER_RESERVE = 64


@dataclass(frozen=True)
class BundleSources:
    """The text members of a bundle, within a byte budget."""

    defining_module: str | None
    files: dict[str, str]
    skipped: list[str]
    truncated: list[str]


def _within_budget(sizes: dict[str, int], defining: str | None) -> dict[str, int]:
    """Bytes each member may spend, by relevance: the defining module, then
    the rest of the user's Python, then everything else."""

    def rank(name: str) -> int:
        if name == defining:
            return 0
        if name.startswith(f"{BUNDLE_USER_CODE_DIRNAME}/") and name.endswith(".py"):
            return 1
        return 2

    allowance: dict[str, int] = {}
    total = 0
    for name in sorted(sizes, key=lambda member: (rank(member), member)):
        size = sizes[name]
        if not name.endswith(_TEXT_SUFFIXES):
            continue
        if name == defining:
            spend = min(size, _MAX_DEFINING_BYTES)
        elif size > _MAX_FILE_BYTES or total + size > _MAX_TOTAL_BYTES:
            continue
        else:
            spend = size
        allowance[name] = spend
        total += spend
    return allowance


def _trimmed(data: bytes, budget: int) -> str:
    """Head and tail of `data` around an elision marker, cut on line
    boundaries and no longer than `budget` bytes."""
    room = max(budget - _TRIM_MARKER_RESERVE, 0)
    head_end = data.rfind(b"\n", 0, room * 2 // 3) + 1
    tail_index = data.find(b"\n", len(data) - (room - head_end))
    tail_start = len(data) if tail_index < 0 else tail_index + 1
    elided = tail_start - head_end
    return (
        data[:head_end].decode("utf-8")
        + f"# ... {elided} bytes elided ...\n"
        + data[tail_start:].decode("utf-8")
    )


def read_bundle_sources(archive: Path) -> BundleSources:
    """Read the text members of a `.tar.zst` bundle, keyed by bundle path.

    Binary members, and anything that would push the total past the byte
    caps, are `skipped`; the defining module is never skipped for its size
    but `truncated` to its own cap instead. The archive is walked twice: to
    size every member, then to read the ones the budget kept.
    """
    manifest = ""
    sizes: dict[str, int] = {}
    for tar, member, name in _walk(archive):
        if name == BUNDLE_MANIFEST_FILENAME:
            manifest = _member_text(tar, member)
        sizes[name] = member.size
    ref = function_ref(manifest)
    defining = defining_module(ref[0], sizes) if ref else None
    allowance = _within_budget(sizes, defining)

    files: dict[str, str] = {}
    truncated: list[str] = []
    skipped: list[str] = [name for name in sizes if name not in allowance]
    for tar, member, name in _walk(archive):
        if name not in allowance:
            continue
        content = tar.extractfile(member)
        data = content.read() if content is not None else None
        try:
            text = data.decode("utf-8") if data is not None else None
        except UnicodeDecodeError:
            text = None
        if text is None:
            skipped.append(name)
            continue
        if len(data) > allowance[name]:
            text = _trimmed(data, allowance[name])
            truncated.append(name)
        files[name] = text
    return BundleSources(defining, files, sorted(skipped), sorted(truncated))


def extract_bundle(archive: Path, dest: Path) -> None:
    """Unpack a `.tar.zst` bundle into `dest`, refusing members that would
    land outside it (tarfile's `data` filter)."""
    dest.mkdir(parents=True, exist_ok=True)
    with _open_tar(archive) as tar:
        tar.extractall(dest, filter="data")


@dataclass(frozen=True)
class SplicedBundle:
    """A bundle's project tree with its defining module replaced: `path` is
    the `file.py::qualname` to import, `project_root` the tree to bundle,
    `requirements` the parent's `requirements.yaml`, if it has one, and
    `local_packages` the parent's staged local packages, in bundle order."""

    path: str
    project_root: Path
    requirements: Path | None
    local_packages: tuple[Path, ...] = ()


def _package_name(requirement: str) -> str:
    name = re.split(r"[\s\[<>=!~;@(]", requirement.strip(), maxsplit=1)[0]
    return re.sub(r"[-_.]+", "-", name).lower()


def _merged_pins(pinned: Iterable[str], packages: Iterable[str]) -> list[str]:
    """`pinned` with each of `packages` added, replacing any requirement on
    the same package (names compared as PEP 503 normalises them)."""
    added = {_package_name(package): package for package in packages}
    kept = [p for p in pinned if _package_name(p) not in added]
    return sorted([*kept, *added.values()], key=str.casefold)


def _merge_packages(requirements: Path, packages: Iterable[str]) -> None:
    manifest = yaml.safe_load(requirements.read_text(encoding="utf-8")) or {}
    manifest[_PUBLIC_PACKAGES_KEY] = _merged_pins(
        manifest.get(_PUBLIC_PACKAGES_KEY) or [], packages
    )
    requirements.write_text(yaml.safe_dump(manifest, sort_keys=True), encoding="utf-8")


def _staged_local_packages(dest_dir: Path) -> tuple[Path, ...]:
    base = dest_dir / BUNDLE_LOCAL_PACKAGES_DIRNAME
    if not base.is_dir():
        return ()
    slots = sorted(
        (slot for slot in base.iterdir() if slot.is_dir() and slot.name.isdigit()),
        key=lambda slot: int(slot.name),
    )
    return tuple(package for slot in slots for package in sorted(slot.iterdir()))


def splice_function_source(
    archive: Path,
    new_source: str,
    dest_dir: Path,
    *,
    add_packages: Iterable[str] = (),
) -> SplicedBundle:
    """Extract `archive` into `dest_dir` and replace the module defining its
    function with `new_source`, keeping every sibling module and the
    parent's `requirements.yaml`, with `add_packages` merged into its public
    packages (replacing the pins of the same packages), and returning the
    parent's staged local packages to bundle again.

    Raises:
        CommonRuntimeException (COMMON_15): the bundle carries no manifest,
            no function reference, or no defining module.
    """
    extract_bundle(archive, dest_dir)
    manifest = dest_dir / BUNDLE_MANIFEST_FILENAME
    if not manifest.is_file():
        raise CommonErrorCode.COMMON_15.exception(missing=BUNDLE_MANIFEST_FILENAME)
    ref = function_ref(manifest.read_text(encoding="utf-8"))
    if ref is None:
        raise CommonErrorCode.COMMON_15.exception(missing="function reference")
    module, qualname = ref
    names = {
        path.relative_to(dest_dir).as_posix()
        for path in dest_dir.rglob("*")
        if path.is_file()
    }
    found = defining_module(module, names)
    if found is None:
        raise CommonErrorCode.COMMON_15.exception(missing=f"module '{module}'")
    module_file = dest_dir / found
    module_file.write_text(new_source, encoding="utf-8")
    requirements = dest_dir / BUNDLE_REQUIREMENTS_FILENAME
    packages = list(add_packages)
    if requirements.is_file():
        if packages:
            _merge_packages(requirements, packages)
    elif packages:
        synthesised, python_version = PythonEnvironment.synthesise()
        write_requirements(
            dest_dir, _merged_pins(synthesised, packages), python_version
        )
    else:
        requirements = None
    return SplicedBundle(
        path=f"{module_file}::{qualname}",
        project_root=dest_dir / BUNDLE_USER_CODE_DIRNAME,
        requirements=requirements,
        local_packages=_staged_local_packages(dest_dir),
    )


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


def _require_inside(path: Path | str, root: Path | None) -> None:
    if root is None:
        return
    real = os.path.realpath(path)
    try:
        inside = os.path.commonpath([root, real]) == str(root)
    except ValueError:
        inside = False
    if not inside:
        raise CommonErrorCode.COMMON_14.exception(path=path, root=root)


def _copy(root: Path | None):
    def copy(src, dst, *, follow_symlinks=True):
        _require_inside(src, root)
        return shutil.copy2(src, dst, follow_symlinks=follow_symlinks)

    return copy


def _real_root(confine_to: Path | None) -> Path | None:
    return None if confine_to is None else Path(os.path.realpath(confine_to))


def collect_local_packages(
    packages: Iterable[Path], dest_dir: Path, *, confine_to: Path | None = None
) -> list[Path]:
    """Copy each local-package directory into
    `<dest_dir>/local_packages/<n>/<package_basename>/`.

    Returns the list of staged target directories, in order.

    Raises:
        CommonRuntimeException (COMMON_10): an entry isn't a directory.
        CommonRuntimeException (COMMON_14): a file resolves outside
            `confine_to`.
    """
    root = _real_root(confine_to)
    base = dest_dir / BUNDLE_LOCAL_PACKAGES_DIRNAME
    staged: list[Path] = []
    for index, pkg in enumerate(packages):
        if not pkg.is_dir():
            raise CommonErrorCode.COMMON_10.exception(path=pkg)
        slot = base / str(index)
        slot.mkdir(parents=True, exist_ok=True)
        target = slot / pkg.name
        _require_inside(pkg, root)
        shutil.copytree(
            pkg,
            target,
            ignore=shutil.ignore_patterns(*_IGNORE_PATTERNS),
            copy_function=_copy(root),
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
    confine_to: Path | None = None,
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
        confine_to: optional directory every bundled file must resolve
            inside of, symlinks followed.

    Returns:
        `output_path`.

    Raises:
        CommonRuntimeException (COMMON_9): `project_root` is not a directory.
        CommonRuntimeException (COMMON_10): a local-package entry is not a directory.
        CommonRuntimeException (COMMON_11): see `valid_python_versions`.
        CommonRuntimeException (COMMON_14): a file resolves outside
            `confine_to`.
        OSError: I/O failure during staging or tar creation.
    """
    if not project_root.is_dir():
        raise CommonErrorCode.COMMON_9.exception(path=project_root)
    include_list = list(include) if include is not None else None
    root = _real_root(confine_to)
    if requirements_yaml is not None:
        _require_inside(requirements_yaml, root)

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
            root=root,
        )

        write_function_def(info, staging)
        if local_packages:
            collect_local_packages(local_packages, staging, confine_to=root)
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
    root: Path | None,
) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    copy = _copy(root)
    for entry in _iter_project_entries(project_root, include):
        target = dest / entry.name
        if entry.is_dir():
            _require_inside(entry, root)
            shutil.copytree(
                entry,
                target,
                ignore=shutil.ignore_patterns(*_IGNORE_PATTERNS),
                copy_function=copy,
                dirs_exist_ok=True,
            )
        elif entry.is_file():
            copy(entry, target)


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
