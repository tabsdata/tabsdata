#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import sys
import tarfile
from pathlib import Path

import pytest
import yaml
import zstandard

from tabsdatak._info import KIND_PUBLISHER_DEF, PublisherInfo
from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.common._archive.bundle import (
    BUNDLE_LOCAL_PACKAGES_DIRNAME,
    BUNDLE_MANIFEST_FILENAME,
    BUNDLE_REQUIREMENTS_FILENAME,
    BUNDLE_USER_CODE_DIRNAME,
    build_bundle,
    capture_source_snippet,
    collect_local_packages,
    extract_bundle,
    read_bundle_sources,
    read_function_source,
    splice_function_source,
    validate_python_version,
    write_function_def,
    write_requirements,
)
from tabsdatak.common._error import CommonErrorCode, CommonRuntimeException
from tabsdatak.conn.localfile import LocalFileSrc

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# Real Info fixture used across the suite.
#
# `_fixture_publisher` is a `@publisher`-decorated module-level
# function: this is the only shape real users ever produce, so the
# round-trip test below exercises the realistic path. After
# decoration, the module attribute `_fixture_publisher` is a
# `PublisherInfo` wrapper; the bare function lives on `.func`.
#
# `_check_func_signature` must `_unwrap_info(self.func)` before
# introspecting, otherwise the round-trip resolution
# (`callable_from_str("...:_fixture_publisher")` -> the wrapper)
# would feed the wrapper into `get_type_hints`, which can't resolve
# the stringified `Callable` annotation on `PublisherInfo.__init__`
# in the `_api.dataclass` wrapper's globals. If a future change
# regresses that unwrap, the round-trip test in this file will fail.


_SRC = LocalFileSrc(
    paths=["data/*.csv"],
)


@publisher(source=_SRC, output_tables=["t"])
def _fixture_publisher(in_1: TableFramesSpec) -> TableFrameSpec:
    """Docstring for the test fixture publisher."""
    return None


def _info() -> PublisherInfo:
    assert isinstance(_fixture_publisher, PublisherInfo)
    return _fixture_publisher


# --- write_function_def --------------------------------------------------


def test_write_function_def_creates_file_with_manifest_name(tmp_path):
    path = write_function_def(_info(), tmp_path)
    assert path == tmp_path / BUNDLE_MANIFEST_FILENAME
    assert path.is_file()


def test_write_function_def_content_is_publisherdef_envelope(tmp_path):
    path = write_function_def(_info(), tmp_path)
    envelope = json.loads(path.read_text())
    assert envelope["kind"] == KIND_PUBLISHER_DEF
    assert "spec" in envelope
    assert envelope["spec"]["func"].endswith(":_fixture_publisher")


def test_write_function_def_creates_missing_dest_dir(tmp_path):
    nested = tmp_path / "a" / "b"
    write_function_def(_info(), nested)
    assert (nested / BUNDLE_MANIFEST_FILENAME).is_file()


# --- source-snippet capture ----------------------------------------------


def test_capture_source_snippet_contains_def_line():
    snippet = capture_source_snippet(_info())
    assert "def _fixture_publisher" in snippet


def test_capture_source_snippet_returns_placeholder_on_failure():
    """A callable whose source can't be located returns a `# source
    unavailable: ...` placeholder rather than raising."""
    # PublisherInfo's signature validator rejects `len` at construction
    # (its 'obj' parameter has no slot-type annotation), so build a
    # valid Info first and then swap `func` in post-construction with
    # `object.__setattr__` to bypass the frozen guard. Same pattern as
    # `test__api.py:test_publisher_rejects_closure_func`.
    fake = PublisherInfo(
        # unwrap to the bare function for valid construction
        func=_fixture_publisher.func,
        source=_SRC,
        output_tables=["t"],
        trigger_by=None,
        on_tables=None,
    )
    object.__setattr__(fake, "func", len)  # builtin → no Python source
    snippet = capture_source_snippet(fake)
    assert snippet.startswith("# source unavailable")


def test_write_requirements_yaml_minimal_schema(tmp_path):
    path = write_requirements(tmp_path, ["foo==1.0", "bar==2.0"], "3.12.10")
    assert path == tmp_path / BUNDLE_REQUIREMENTS_FILENAME
    data = yaml.safe_load(path.read_text())
    assert data == {
        "pythonVersion": "3.12.10",
        "publicPackages": ["foo==1.0", "bar==2.0"],
        "installPackagesDependencies": False,
        "ignoreUnavailablePackages": True,
    }


def test_write_requirements_yaml_includes_local_packages(tmp_path):
    path = write_requirements(
        tmp_path,
        ["foo==1.0"],
        "3.12.10",
        local_packages=[Path("/host/a"), Path("/host/b")],
    )
    data = yaml.safe_load(path.read_text())
    assert [Path(p) for p in data["localPackages"]] == [Path("/host/a"), Path("/host/b")]


def test_write_requirements_yaml_omits_local_packages_when_empty(tmp_path):
    path = write_requirements(tmp_path, ["foo==1.0"], "3.12.10")
    data = yaml.safe_load(path.read_text())
    assert "localPackages" not in data


def test_validate_python_version_accepts_listed():
    validate_python_version("3.12.10", ["3.12.10", "3.13.0"])


def test_validate_python_version_rejects_unlisted():
    with pytest.raises(CommonRuntimeException, match="3.12.10") as exc_info:
        validate_python_version("3.12.10", ["2.0.0", "1.0.0"])
    assert exc_info.value.api_error is CommonErrorCode.COMMON_11


def test_validate_python_version_empty_allowed_rejects_anything():
    with pytest.raises(CommonRuntimeException) as exc_info:
        validate_python_version("3.12.10", [])
    assert exc_info.value.api_error is CommonErrorCode.COMMON_11


# --- local packages ------------------------------------------------------


def test_collect_local_packages_copies_each_under_indexed_slot(tmp_path):
    pkg_a = tmp_path / "src_a" / "my_pkg_a"
    (pkg_a / "__init__.py").parent.mkdir(parents=True)
    (pkg_a / "__init__.py").write_text("")
    (pkg_a / "module.py").write_text("X = 1\n")

    pkg_b = tmp_path / "src_b" / "my_pkg_b"
    (pkg_b / "__init__.py").parent.mkdir(parents=True)
    (pkg_b / "__init__.py").write_text("")

    staging = tmp_path / "stage"
    staged = collect_local_packages([pkg_a, pkg_b], staging)

    assert staged == [
        staging / BUNDLE_LOCAL_PACKAGES_DIRNAME / "0" / "my_pkg_a",
        staging / BUNDLE_LOCAL_PACKAGES_DIRNAME / "1" / "my_pkg_b",
    ]
    assert (staged[0] / "module.py").read_text() == "X = 1\n"


def test_collect_local_packages_ignores_pycache(tmp_path):
    pkg = tmp_path / "src" / "my_pkg"
    (pkg / "__pycache__").mkdir(parents=True)
    (pkg / "__pycache__" / "junk.pyc").write_bytes(b"\x00")
    (pkg / "__init__.py").write_text("")

    staging = tmp_path / "stage"
    collect_local_packages([pkg], staging)
    staged_root = staging / BUNDLE_LOCAL_PACKAGES_DIRNAME / "0" / "my_pkg"
    assert (staged_root / "__init__.py").is_file()
    assert not (staged_root / "__pycache__").exists()


def test_collect_local_packages_rejects_non_directory(tmp_path):
    missing = tmp_path / "nope"
    with pytest.raises(CommonRuntimeException, match="not a directory") as exc_info:
        collect_local_packages([missing], tmp_path / "stage")
    assert exc_info.value.api_error is CommonErrorCode.COMMON_10


def test_collect_local_packages_empty_iterable_is_noop(tmp_path):
    staging = tmp_path / "stage"
    assert collect_local_packages([], staging) == []


# --- build_bundle integration -------------------------------------------


def _make_project(tmp_path: Path) -> Path:
    """A minimal user project with one package + one stray file +
    a __pycache__ that must be filtered."""
    root = tmp_path / "project"
    pkg = root / "my_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "module.py").write_text("VALUE = 42\n")
    (pkg / "__pycache__").mkdir()
    (pkg / "__pycache__" / "junk.pyc").write_bytes(b"\x00")
    (root / "README.md").write_text("hi\n")
    return root


def _write_bundle(path: Path, members: dict[str, str]) -> None:
    """Write a `.tar.zst` holding exactly `members` (name -> text)."""
    staging = path.parent / f"{path.stem}_staging"
    for name, text in members.items():
        member = staging / name
        member.parent.mkdir(parents=True, exist_ok=True)
        member.write_text(text, encoding="utf-8", newline="")
    compressor = zstandard.ZstdCompressor()
    with open(path, "wb") as raw:
        with compressor.stream_writer(raw, closefd=False) as stream:
            with tarfile.open(fileobj=stream, mode="w|") as tar:
                tar.add(staging, arcname=".")


def _manifest(func: str) -> dict:
    """The `function_def.json` envelope `info.to_json()` produces."""
    return {
        "kind": "transformerDef",
        "apiVersion": "1.0",
        "type": "tabsdatak._info:TransformerInfo",
        "spec": {"func": func},
    }


_MODULE_SOURCE = (
    "import tabsdata as td\n\n\n"
    "@td.publisher(source=td.LocalFileSource(paths=['a.csv']), tables=['t'])\n"
    "def my_fn():\n    return None\n"
)


def test_read_function_source_returns_the_defining_module(tmp_path):
    out = tmp_path / "bundle.tar.zst"
    _write_bundle(
        out,
        {
            BUNDLE_MANIFEST_FILENAME: json.dumps(_manifest("my_pkg.module:my_fn")),
            f"{BUNDLE_USER_CODE_DIRNAME}/my_pkg/module.py": _MODULE_SOURCE,
            f"{BUNDLE_USER_CODE_DIRNAME}/my_pkg/other.py": "# unrelated\n",
        },
    )

    assert read_function_source(out) == ("my_fn", _MODULE_SOURCE)


def test_read_function_source_without_manifest_returns_none(tmp_path):
    out = tmp_path / "bundle.tar.zst"
    _write_bundle(out, {f"{BUNDLE_USER_CODE_DIRNAME}/my_pkg/module.py": _MODULE_SOURCE})

    assert read_function_source(out) is None


def test_read_function_source_with_module_missing_returns_none(tmp_path):
    out = tmp_path / "bundle.tar.zst"
    _write_bundle(
        out,
        {BUNDLE_MANIFEST_FILENAME: json.dumps(_manifest("my_pkg.module:my_fn"))},
    )

    assert read_function_source(out) is None


def test_read_function_source_reads_a_package_init_module(tmp_path):
    out = tmp_path / "bundle.tar.zst"
    _write_bundle(
        out,
        {
            BUNDLE_MANIFEST_FILENAME: json.dumps(_manifest("my_pkg:my_fn")),
            f"{BUNDLE_USER_CODE_DIRNAME}/my_pkg/__init__.py": _MODULE_SOURCE,
        },
    )

    assert read_function_source(out) == ("my_fn", _MODULE_SOURCE)


def _tar_members(path: Path) -> set[str]:
    """Return the set of member names in the bundle (a `.tar.zst`).

    Reads via streaming `r|` mode -- zstandard's stream_reader is a
    non-seekable file-like, so we iterate the tar sequentially and
    collect names rather than calling ``getmembers()`` (which walks
    the central index and would need seek support).
    """
    names: set[str] = set()
    with open(path, "rb") as raw:
        with zstandard.ZstdDecompressor().stream_reader(raw) as stream:
            with tarfile.open(fileobj=stream, mode="r|") as tar:
                for m in tar:
                    if m.name != ".":
                        names.add(m.name.lstrip("./"))
    return names


def _read_tar_member(path: Path, name: str) -> bytes:
    """Stream the bundle and return the bytes of the first member
    matching ``name`` (where ``name`` is the tarball's path)."""
    with open(path, "rb") as raw:
        with zstandard.ZstdDecompressor().stream_reader(raw) as stream:
            with tarfile.open(fileobj=stream, mode="r|") as tar:
                for m in tar:
                    if m.name == name:
                        return tar.extractfile(m).read()
    raise KeyError(f"member {name!r} not found in {path}")


def test_build_bundle_creates_tarball_with_expected_top_level(tmp_path):
    project = _make_project(tmp_path)
    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out)
    assert out.is_file()

    members = _tar_members(out)
    assert BUNDLE_MANIFEST_FILENAME in members
    assert BUNDLE_REQUIREMENTS_FILENAME in members
    assert f"{BUNDLE_USER_CODE_DIRNAME}/my_pkg/__init__.py" in members
    assert f"{BUNDLE_USER_CODE_DIRNAME}/my_pkg/module.py" in members
    assert f"{BUNDLE_USER_CODE_DIRNAME}/README.md" in members


def test_build_bundle_user_code_lives_under_dedicated_subdir(tmp_path):
    """User content must live exclusively under `user_code/` — never at
    the bundle root. This is the contract that lets the runner
    deterministically pick the sys.path entry."""
    project = _make_project(tmp_path)
    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out)
    members = _tar_members(out)
    # No user file lands at bundle root.
    assert "my_pkg/__init__.py" not in members
    assert "README.md" not in members
    # Every user-content member sits under user_code/.
    user_content = {m for m in members if "my_pkg" in m or m.endswith("README.md")}
    assert user_content
    assert all(m.startswith(f"{BUNDLE_USER_CODE_DIRNAME}/") for m in user_content)


def test_build_bundle_does_not_include_source_snippet(tmp_path):
    """Snippet capture is a standalone helper; the bundle never carries it."""
    project = _make_project(tmp_path)
    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out)
    members = _tar_members(out)
    assert not any(
        m.endswith(".snippet.py") or m == "function_snippet.py" for m in members
    )


def test_build_bundle_ignores_pycache_in_project_tree(tmp_path):
    project = _make_project(tmp_path)
    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out)
    members = _tar_members(out)
    assert not any("__pycache__" in m for m in members)


def test_build_bundle_excludes_loose_compiled_files_in_subdir(tmp_path):
    """Compiled artifacts (``*.pyc`` / ``*.pyo`` / ``*.pyd``) sitting loose
    beside source inside a subfolder -- not only inside ``__pycache__`` --
    are excluded from the bundle."""
    project = _make_project(tmp_path)
    pkg = project / "my_pkg"
    (pkg / "module.pyc").write_bytes(b"\x00")
    (pkg / "module.pyo").write_bytes(b"\x00")
    (pkg / "native.pyd").write_bytes(b"\x00")

    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out)
    members = _tar_members(out)
    assert not any(m.endswith((".pyc", ".pyo", ".pyd")) for m in members)
    # Real source is still bundled.
    assert f"{BUNDLE_USER_CODE_DIRNAME}/my_pkg/module.py" in members


def test_build_bundle_excludes_compiled_files_at_project_root(tmp_path):
    """Compiled artifacts at the *top level* of the project tree are
    excluded too. These are copied as individual entries (not via a
    recursive ``copytree`` of a subfolder), so they exercise the
    top-level entry filter specifically."""
    project = _make_project(tmp_path)
    (project / "stray.pyc").write_bytes(b"\x00")
    (project / "legacy.pyo").write_bytes(b"\x00")
    (project / "root__pycache__placeholder.pyd").write_bytes(b"\x00")

    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out)
    members = _tar_members(out)
    assert not any(m.endswith((".pyc", ".pyo", ".pyd")) for m in members)
    assert f"{BUNDLE_USER_CODE_DIRNAME}/README.md" in members


def test_build_bundle_excludes_pycache_dir_at_project_root(tmp_path):
    """A ``__pycache__`` directory at the top level of the project tree is
    excluded (the top-level entry filter drops the directory itself, not
    just its contents)."""
    project = _make_project(tmp_path)
    (project / "__pycache__").mkdir()
    (project / "__pycache__" / "top.cpython-312.pyc").write_bytes(b"\x00")

    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out)
    members = _tar_members(out)
    assert not any("__pycache__" in m for m in members)


def test_build_bundle_include_filter_restricts_top_level(tmp_path):
    project = _make_project(tmp_path)
    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out, include=["my_pkg"])
    members = _tar_members(out)
    assert f"{BUNDLE_USER_CODE_DIRNAME}/my_pkg/__init__.py" in members
    assert f"{BUNDLE_USER_CODE_DIRNAME}/README.md" not in members


def test_build_bundle_with_local_packages_stages_them(tmp_path):
    project = _make_project(tmp_path)
    pkg = tmp_path / "lp" / "vendored"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("VENDORED = True\n")

    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out, local_packages=[pkg])
    members = _tar_members(out)
    assert f"{BUNDLE_LOCAL_PACKAGES_DIRNAME}/0/vendored/__init__.py" in members


def test_build_bundle_creates_missing_parent_for_output(tmp_path):
    project = _make_project(tmp_path)
    out = tmp_path / "deep" / "nested" / "bundle.tar.zst"
    build_bundle(_info(), project, out)
    assert out.is_file()


def test_build_bundle_rejects_missing_project_root(tmp_path):
    with pytest.raises(CommonRuntimeException, match="not a directory") as exc_info:
        build_bundle(_info(), tmp_path / "nope", tmp_path / "out.tar.zst")
    assert exc_info.value.api_error is CommonErrorCode.COMMON_9


def test_build_bundle_tolerates_user_file_named_like_a_reserved_path(tmp_path):
    """User content lives under user_code/, so a file the user happens to
    name `function_def.json` doesn't collide with the bundle manifest at
    the root — it just lands at `user_code/function_def.json`."""
    project = _make_project(tmp_path)
    (project / BUNDLE_MANIFEST_FILENAME).write_text("{}")
    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out)
    members = _tar_members(out)
    # The bundle manifest is at root; the user's shadow file is under user_code/.
    assert BUNDLE_MANIFEST_FILENAME in members
    assert f"{BUNDLE_USER_CODE_DIRNAME}/{BUNDLE_MANIFEST_FILENAME}" in members


def test_build_bundle_rejects_unsupported_python_version(tmp_path):
    project = _make_project(tmp_path)
    with pytest.raises(CommonRuntimeException) as exc_info:
        build_bundle(
            _info(),
            project,
            tmp_path / "out.tar.zst",
            valid_python_versions=["2.0.0"],
        )
    assert exc_info.value.api_error is CommonErrorCode.COMMON_11


def test_build_bundle_accepts_current_python_version(tmp_path):
    project = _make_project(tmp_path)
    pyv = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out, valid_python_versions=[pyv])
    assert out.is_file()


def test_build_bundle_manifest_round_trips(tmp_path):
    """Sanity: the manifest we write can be deserialised back into the
    same PublisherInfo."""
    project = _make_project(tmp_path)
    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out)
    text = _read_tar_member(out, f"./{BUNDLE_MANIFEST_FILENAME}").decode("utf-8")
    roundtripped = PublisherInfo.from_json(text)
    assert roundtripped.output_tables == _info().output_tables


def _outside(tmp_path: Path) -> Path:
    outside = tmp_path / "outside"
    (outside / "secrets").mkdir(parents=True)
    (outside / "secrets" / "key.txt").write_text("k\n")
    return outside


@pytest.mark.parametrize(
    "link, target",
    [
        ("secrets", "secrets"),
        ("my_pkg/secrets", "secrets"),
        ("key.txt", "secrets/key.txt"),
        ("my_pkg/key.txt", "secrets/key.txt"),
    ],
)
def test_build_bundle_confined_rejects_symlinks_leaving_the_root(
    link, target, tmp_path
):
    project = _make_project(tmp_path)
    (project / link).symlink_to(_outside(tmp_path) / target)
    with pytest.raises(CommonRuntimeException, match="outside") as exc_info:
        build_bundle(_info(), project, tmp_path / "bundle.tar.zst", confine_to=project)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_14


def test_build_bundle_confined_allows_symlinks_inside_the_root(tmp_path):
    project = _make_project(tmp_path)
    (project / "alias.md").symlink_to(project / "README.md")
    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out, confine_to=tmp_path)
    assert f"{BUNDLE_USER_CODE_DIRNAME}/alias.md" in _tar_members(out)


def test_build_bundle_unconfined_follows_symlinks(tmp_path):
    project = _make_project(tmp_path)
    (project / "secrets").symlink_to(_outside(tmp_path) / "secrets")
    out = tmp_path / "bundle.tar.zst"
    build_bundle(_info(), project, out)
    assert f"{BUNDLE_USER_CODE_DIRNAME}/secrets/key.txt" in _tar_members(out)


def test_build_bundle_confined_rejects_local_packages_outside(tmp_path):
    project = _make_project(tmp_path)
    with pytest.raises(CommonRuntimeException) as exc_info:
        build_bundle(
            _info(),
            project,
            tmp_path / "bundle.tar.zst",
            local_packages=[_outside(tmp_path) / "secrets"],
            confine_to=project,
        )
    assert exc_info.value.api_error is CommonErrorCode.COMMON_14


_UC = BUNDLE_USER_CODE_DIRNAME


def _parent_bundle(tmp_path, module="pkg.mod", files=None, requirements=True):
    members = {
        BUNDLE_MANIFEST_FILENAME: json.dumps(_manifest(f"{module}:my_fn")),
        f"{_UC}/pkg/mod.py": "OLD = 1\n",
        f"{_UC}/pkg/helpers.py": "HELP = 1\n",
        **(files or {}),
    }
    if requirements:
        members[BUNDLE_REQUIREMENTS_FILENAME] = yaml.safe_dump({
            "pythonVersion": "3.12.1",
            "publicPackages": ["polars==1.0"],
        })
    out = tmp_path / "parent.tar.zst"
    _write_bundle(out, members)
    return out


def test_splice_replaces_the_defining_module_and_keeps_its_siblings(tmp_path):
    spliced = splice_function_source(
        _parent_bundle(tmp_path), "NEW = 1\n", tmp_path / "tree"
    )
    root = tmp_path / "tree" / _UC
    assert spliced.project_root == root
    assert spliced.path == f"{root / 'pkg' / 'mod.py'}::my_fn"
    assert (root / "pkg" / "mod.py").read_text() == "NEW = 1\n"
    assert (root / "pkg" / "helpers.py").read_text() == "HELP = 1\n"
    assert spliced.requirements == tmp_path / "tree" / BUNDLE_REQUIREMENTS_FILENAME


def test_splice_merges_added_packages_into_the_parent_requirements(tmp_path):
    spliced = splice_function_source(
        _parent_bundle(tmp_path),
        "NEW = 1\n",
        tmp_path / "tree",
        add_packages=["Requests==2.0", "polars==1.0"],
    )
    manifest = yaml.safe_load(spliced.requirements.read_text())
    assert manifest["pythonVersion"] == "3.12.1"
    assert manifest["publicPackages"] == ["polars==1.0", "Requests==2.0"]


def test_splice_without_parent_requirements(tmp_path):
    archive = _parent_bundle(tmp_path, requirements=False)
    plain = splice_function_source(archive, "x = 1\n", tmp_path / "a")
    assert plain.requirements is None
    spliced = splice_function_source(
        archive, "x = 1\n", tmp_path / "b", add_packages=["extra==1"]
    )
    packages = yaml.safe_load(spliced.requirements.read_text())["publicPackages"]
    assert "extra==1" in packages


def test_splice_replaces_a_package_init_module(tmp_path):
    archive = _parent_bundle(
        tmp_path, module="pkg", files={f"{_UC}/pkg/__init__.py": "OLD = 1\n"}
    )
    spliced = splice_function_source(archive, "NEW = 1\n", tmp_path / "tree")
    init = tmp_path / "tree" / _UC / "pkg" / "__init__.py"
    assert init.read_text() == "NEW = 1\n"
    assert spliced.path == f"{init}::my_fn"


@pytest.mark.parametrize(
    "members, missing",
    [
        ({f"{_UC}/pkg/mod.py": ""}, BUNDLE_MANIFEST_FILENAME),
        ({BUNDLE_MANIFEST_FILENAME: json.dumps({"spec": {}})}, "function reference"),
        (
            {BUNDLE_MANIFEST_FILENAME: json.dumps(_manifest("pkg.gone:my_fn"))},
            "module 'pkg.gone'",
        ),
    ],
)
def test_splice_refuses_a_bundle_it_cannot_patch(members, missing, tmp_path):
    out = tmp_path / "b.tar.zst"
    _write_bundle(out, members)
    with pytest.raises(CommonRuntimeException) as exc_info:
        splice_function_source(out, "x = 1\n", tmp_path / "tree")
    assert exc_info.value.api_error is CommonErrorCode.COMMON_15
    assert missing in str(exc_info.value)


def test_extract_refuses_members_escaping_the_destination(tmp_path):
    staging = tmp_path / "evil.txt"
    staging.write_text("x")
    out = tmp_path / "evil.tar.zst"
    with open(out, "wb") as raw:
        with zstandard.ZstdCompressor().stream_writer(raw, closefd=False) as stream:
            with tarfile.open(fileobj=stream, mode="w|") as tar:
                tar.add(staging, arcname="../escaped.txt")
    with pytest.raises(tarfile.OutsideDestinationError):
        extract_bundle(out, tmp_path / "dest")
    assert not (tmp_path / "escaped.txt").exists()


def test_bundle_sources_rank_the_defining_module_first(tmp_path):
    sources = read_bundle_sources(
        _parent_bundle(tmp_path, files={f"{_UC}/data.bin": "x"})
    )
    assert sources.defining_module == f"{_UC}/pkg/mod.py"
    assert sources.files[f"{_UC}/pkg/mod.py"] == "OLD = 1\n"
    assert f"{_UC}/pkg/helpers.py" in sources.files
    assert BUNDLE_MANIFEST_FILENAME in sources.files
    assert sources.skipped == [f"{_UC}/data.bin"]
    assert sources.truncated == []


def test_bundle_sources_trim_a_large_defining_module_by_bytes(tmp_path):
    line = "s = '" + "é" * 10 + "'\n"
    big = line * 20_000
    sources = read_bundle_sources(
        _parent_bundle(tmp_path, files={f"{_UC}/pkg/mod.py": big})
    )
    text = sources.files[f"{_UC}/pkg/mod.py"]
    assert sources.truncated == [f"{_UC}/pkg/mod.py"]
    assert "bytes elided" in text
    assert len(text.encode("utf-8")) <= 256 * 1024
    assert text.startswith(line)
    assert text.endswith(line)


def test_bundle_sources_skip_what_exceeds_the_budget(tmp_path):
    large = "x" * (129 * 1024)
    sources = read_bundle_sources(
        _parent_bundle(tmp_path, files={f"{_UC}/pkg/large.py": large})
    )
    assert f"{_UC}/pkg/large.py" in sources.skipped
    assert f"{_UC}/pkg/large.py" not in sources.files
