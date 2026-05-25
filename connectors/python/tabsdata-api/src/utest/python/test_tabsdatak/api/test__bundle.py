		#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import sys
import tarfile
from pathlib import Path

import pytest
import yaml
import zstandard

from tabsdatak._bundle import (
    BUNDLE_LOCAL_PACKAGES_DIRNAME,
    BUNDLE_MANIFEST_FILENAME,
    BUNDLE_REQUIREMENTS_FILENAME,
    BUNDLE_USER_CODE_DIRNAME,
    build_bundle,
    capture_source_snippet,
    collect_local_packages,
    synthesise_requirements,
    validate_python_version,
    write_function_def,
    write_requirements,
)
from tabsdatak._info import KIND_PUBLISHER_DEF, PublisherInfo
from tabsdatak._tdruntime.python.recipe import InstallRecipe
from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrcConn, LocalFileSrc
from tabsdatak.error import ApiErrorCode, ApiRuntimeException

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
    conn=LocalFileSrcConn(base_path="/tmp"),
    paths=["data/*.csv"],
)


@publisher(source=_SRC, output_tables=["t"])
def _fixture_publisher(in_1: TableFramesSpec) -> tuple[TableFrameSpec]:
    """Docstring for the test fixture publisher."""
    return (None,)


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


# --- requirements + python version --------------------------------------


def test_synthesise_requirements_returns_sorted_unique():
    reqs, pyv = synthesise_requirements()
    assert reqs == sorted(set(reqs), key=str.casefold)


def test_synthesise_requirements_python_version_is_dotted_triple():
    _, pyv = synthesise_requirements()
    parts = pyv.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)


# --- synthesise_requirements: install-method filter ----------------------
#
# `synthesise_requirements` only emits packages installed from a
# PyPI-like registry. The four cases below cover each `InstallRecipe`
# value plus the "name resolves but `Package.recipe()` returns None"
# case (uninstalled / unknown). Detection is delegated to
# `Package(name).recipe()`; we monkeypatch it at the bundle module's
# import site so the test doesn't depend on the producer's actual
# installed-package layout.


def _stub_dist(name: str):
    class _Dist:
        metadata = {"Name": name}
        version = "9.9.9"

    return _Dist()


def _stub_recipe(recipe_value, *, version: str | None = "1.2.3"):
    # mirrors the PackageRecipe surface synthesise_requirements touches:
    # `.recipe` (InstallRecipe enum) and `.version` (str|None). Built as
    # an instance with __dict__ assignment rather than a class body so
    # the function-scoped `version` argument actually closes over.
    stub = type("_StubRecipe", (), {})()
    stub.recipe = recipe_value
    stub.version = version
    return stub


def _patch_recipes(monkeypatch, by_name: dict[str, object]):
    """Override `Package(name).recipe()` lookup via the bundle module.

    `by_name[name]` is the value returned for that name (a stub or None
    to simulate uninstalled). Unknown names raise to surface unexpected
    lookups during the test.
    """

    class _PkgStub:
        def __init__(self, name):
            self.name = name

        def recipe(self):
            if self.name not in by_name:
                raise AssertionError(f"unexpected Package lookup: {self.name}")
            return by_name[self.name]

    monkeypatch.setattr("tabsdatak._bundle.Package", _PkgStub)

    monkeypatch.setattr(
        "tabsdatak._bundle.importlib_metadata.distributions",
        lambda: [_stub_dist(n) for n in by_name],
    )


def test_synthesise_requirements_emits_registry_installs(monkeypatch):
    _patch_recipes(monkeypatch, {"polars": _stub_recipe(InstallRecipe.REGISTRY)})
    reqs, _ = synthesise_requirements()
    assert reqs == ["polars==1.2.3"]


def test_synthesise_requirements_drops_editable_installs(monkeypatch):
    _patch_recipes(monkeypatch, {"my-local-pkg": _stub_recipe(InstallRecipe.EDITABLE)})
    reqs, _ = synthesise_requirements()
    assert reqs == []


def test_synthesise_requirements_drops_local_wheel_installs(monkeypatch):
    _patch_recipes(monkeypatch, {"wheel-pkg": _stub_recipe(InstallRecipe.WHEEL)})
    reqs, _ = synthesise_requirements()
    assert reqs == []


def test_synthesise_requirements_drops_frozen_folder_installs(monkeypatch):
    _patch_recipes(monkeypatch, {"frozen-pkg": _stub_recipe(InstallRecipe.FROZEN)})
    reqs, _ = synthesise_requirements()
    assert reqs == []


def test_synthesise_requirements_drops_unresolvable_packages(monkeypatch):
    """Package.recipe() returns None when distribution metadata can't
    be found -- e.g. the entry was uninstalled mid-snapshot."""
    _patch_recipes(monkeypatch, {"ghost-pkg": None})
    reqs, _ = synthesise_requirements()
    assert reqs == []


def test_synthesise_requirements_drops_versionless_recipes(monkeypatch):
    """A registry install with no version is dropped (pip would refuse
    the unpinned constraint and we'd rather skip than emit it bare)."""
    _patch_recipes(
        monkeypatch,
        {"versionless": _stub_recipe(InstallRecipe.REGISTRY, version=None)},
    )
    reqs, _ = synthesise_requirements()
    assert reqs == []


def test_synthesise_requirements_mixes_kinds_keeping_only_registry(monkeypatch):
    _patch_recipes(
        monkeypatch,
        {
            "polars": _stub_recipe(InstallRecipe.REGISTRY),
            "my-local-pkg": _stub_recipe(InstallRecipe.EDITABLE),
            "wheel-pkg": _stub_recipe(InstallRecipe.WHEEL),
            "boto3": _stub_recipe(InstallRecipe.REGISTRY),
        },
    )
    reqs, _ = synthesise_requirements()
    assert reqs == ["boto3==1.2.3", "polars==1.2.3"]


# --- synthesise_requirements: PEP 503 name canonicalisation --------------


def test_synthesise_requirements_canonicalises_underscores_to_dashes(monkeypatch):
    """PEP 503: `_` and `.` collapse to `-`. `tabsdata_api` and
    `tabsdata.api` both come out as `tabsdata-api`."""
    _patch_recipes(
        monkeypatch,
        {"tabsdata_api": _stub_recipe(InstallRecipe.REGISTRY, version="1.0")},
    )
    reqs, _ = synthesise_requirements()
    assert reqs == ["tabsdata-api==1.0"]


def test_synthesise_requirements_canonicalises_to_lowercase(monkeypatch):
    """PEP 503: names are lowercased."""
    _patch_recipes(
        monkeypatch,
        {"Tabsdata-API": _stub_recipe(InstallRecipe.REGISTRY, version="1.0")},
    )
    reqs, _ = synthesise_requirements()
    assert reqs == ["tabsdata-api==1.0"]


def test_synthesise_requirements_dedupes_after_canonicalisation(monkeypatch):
    """Two distributions whose names canonicalise to the same string
    are deduped to one entry (and the value is whichever pinned
    version landed last; both produce the same line here)."""
    _patch_recipes(
        monkeypatch,
        {
            "Tabsdata_API": _stub_recipe(InstallRecipe.REGISTRY, version="1.0"),
            "tabsdata-api": _stub_recipe(InstallRecipe.REGISTRY, version="1.0"),
        },
    )
    reqs, _ = synthesise_requirements()
    assert reqs == ["tabsdata-api==1.0"]


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
    assert data["localPackages"] == ["/host/a", "/host/b"]


def test_write_requirements_yaml_omits_local_packages_when_empty(tmp_path):
    path = write_requirements(tmp_path, ["foo==1.0"], "3.12.10")
    data = yaml.safe_load(path.read_text())
    assert "localPackages" not in data


def test_validate_python_version_accepts_listed():
    validate_python_version("3.12.10", ["3.12.10", "3.13.0"])


def test_validate_python_version_rejects_unlisted():
    with pytest.raises(ApiRuntimeException, match="3.12.10") as exc_info:
        validate_python_version("3.12.10", ["2.0.0", "1.0.0"])
    assert exc_info.value.api_error is ApiErrorCode.API_24


def test_validate_python_version_empty_allowed_rejects_anything():
    with pytest.raises(ApiRuntimeException) as exc_info:
        validate_python_version("3.12.10", [])
    assert exc_info.value.api_error is ApiErrorCode.API_24


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
    with pytest.raises(ApiRuntimeException, match="not a directory") as exc_info:
        collect_local_packages([missing], tmp_path / "stage")
    assert exc_info.value.api_error is ApiErrorCode.API_23


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
    with pytest.raises(ApiRuntimeException, match="not a directory") as exc_info:
        build_bundle(_info(), tmp_path / "nope", tmp_path / "out.tar.zst")
    assert exc_info.value.api_error is ApiErrorCode.API_22


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
    with pytest.raises(ApiRuntimeException) as exc_info:
        build_bundle(
            _info(),
            project,
            tmp_path / "out.tar.zst",
            valid_python_versions=["2.0.0"],
        )
    assert exc_info.value.api_error is ApiErrorCode.API_24


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
