#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatak.common._interpreter.python.error import FeatureDetectionError
from tabsdatak.common._interpreter.python.recipe import (
    InstallRecipe,
    PackageRecipe,
    TabsdataEdition,
)
from tabsdatak.common._interpreter.python.venv import (
    TABSDATA_PACKAGE,
    TABSDATA_TOOLKIT_PACKAGE,
    PythonEnvironment,
)

pytestmark = pytest.mark.unit


def make_recipe(
    name: str,
    install: InstallRecipe = InstallRecipe.EDITABLE,
    location: Path | None = None,
) -> PackageRecipe:
    return PackageRecipe(
        name=name,
        recipe=install,
        version="2.0.0",
        location=location,
    )


def test_edition_from_manifest_returns_enterprise_when_flag_true():
    edition = PythonEnvironment.edition_from_manifest({"X-Enterprise": "true"})
    assert edition == TabsdataEdition.ENTERPRISE


def test_edition_from_manifest_is_case_insensitive():
    edition = PythonEnvironment.edition_from_manifest({"X-Enterprise": "TRUE"})
    assert edition == TabsdataEdition.ENTERPRISE


def test_edition_from_manifest_defaults_to_open_source_when_flag_false():
    edition = PythonEnvironment.edition_from_manifest({"X-Enterprise": "false"})
    assert edition == TabsdataEdition.OPEN_SOURCE


def test_edition_from_manifest_defaults_to_open_source_when_flag_missing():
    edition = PythonEnvironment.edition_from_manifest({})
    assert edition == TabsdataEdition.OPEN_SOURCE


def test_edition_from_location_returns_enterprise_for_tabsdata_ee(tmp_path: Path):
    recipe = make_recipe(TABSDATA_PACKAGE, location=tmp_path / "tabsdata-ee")
    assert (
        PythonEnvironment.edition_from_location(recipe) == TabsdataEdition.ENTERPRISE
    )


def test_edition_from_location_returns_open_source_for_other_paths(tmp_path: Path):
    recipe = make_recipe(TABSDATA_PACKAGE, location=tmp_path / "tabsdata-os")
    assert (
        PythonEnvironment.edition_from_location(recipe) == TabsdataEdition.OPEN_SOURCE
    )


def test_edition_from_location_raises_when_location_is_none():
    recipe = make_recipe(TABSDATA_PACKAGE, install=InstallRecipe.REGISTRY)
    with pytest.raises(FeatureDetectionError):
        PythonEnvironment.edition_from_location(recipe)


def test_split_packages_separates_toolkit_first():
    toolkit = make_recipe(TABSDATA_TOOLKIT_PACKAGE)
    main = make_recipe(TABSDATA_PACKAGE)
    other = make_recipe("tabsdata-agent")
    toolkit_only, others = PythonEnvironment.split_packages((main, toolkit, other))
    assert toolkit_only == (toolkit,)
    assert others == (main, other)


def test_split_packages_returns_empty_toolkit_when_absent():
    main = make_recipe(TABSDATA_PACKAGE)
    toolkit, others = PythonEnvironment.split_packages((main,))
    assert toolkit == ()
    assert others == (main,)


def test_link_libexec_launchers_skips_non_editable_packages(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    environment = PythonEnvironment(env_root)
    recipe = make_recipe(TABSDATA_PACKAGE, install=InstallRecipe.REGISTRY)
    environment.link_libexec_launchers((recipe,))
    assert not (env_root / "libexec" / "tabsdata").exists()


def test_link_libexec_launchers_skips_packages_without_location(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    environment = PythonEnvironment(env_root)
    recipe = make_recipe(TABSDATA_PACKAGE, install=InstallRecipe.EDITABLE)
    environment.link_libexec_launchers((recipe,))
    assert not (env_root / "libexec" / "tabsdata").exists()


def test_link_libexec_launchers_skips_missing_source_folder(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    project_root = tmp_path / "tabsdata-ee"
    project_root.mkdir()
    environment = PythonEnvironment(env_root)
    recipe = make_recipe(TABSDATA_PACKAGE, location=project_root)
    environment.link_libexec_launchers((recipe,))
    assert not (env_root / "libexec" / "tabsdata").exists()


def test_link_libexec_launchers_symlinks_each_source_file(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    project_root = tmp_path / "tabsdata-ee"
    source = project_root / "target" / "python" / "libexec" / "tabsdata"
    source.mkdir(parents=True)
    (source / "td_run_function").write_text("#!script", encoding="utf-8")
    (source / "td_fetch_request").write_text("#!script", encoding="utf-8")
    environment = PythonEnvironment(env_root)
    recipe = make_recipe(TABSDATA_PACKAGE, location=project_root)

    environment.link_libexec_launchers((recipe,))

    target = env_root / "libexec" / "tabsdata"
    assert target.is_dir()
    assert {entry.name for entry in target.iterdir()} == {
        "td_run_function",
        "td_fetch_request",
    }
    for entry in target.iterdir():
        assert entry.is_symlink()


def test_link_libexec_launchers_replaces_existing_symlink(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    project_root = tmp_path / "tabsdata-ee"
    source = project_root / "target" / "python" / "libexec" / "tabsdata"
    source.mkdir(parents=True)
    fresh = source / "td_run_function"
    fresh.write_text("fresh", encoding="utf-8")
    environment = PythonEnvironment(env_root)

    target = env_root / "libexec" / "tabsdata"
    target.mkdir(parents=True)
    stale_source = tmp_path / "stale"
    stale_source.write_text("stale", encoding="utf-8")
    (target / "td_run_function").symlink_to(stale_source)

    recipe = make_recipe(TABSDATA_PACKAGE, location=project_root)
    environment.link_libexec_launchers((recipe,))

    link = target / "td_run_function"
    assert link.is_symlink()
    assert link.resolve() == fresh.resolve()


def test_link_libexec_launchers_dedupes_same_source(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    project_root = tmp_path / "tabsdata-ee"
    source = project_root / "target" / "python" / "libexec" / "tabsdata"
    source.mkdir(parents=True)
    (source / "td_run_function").write_text("payload", encoding="utf-8")
    environment = PythonEnvironment(env_root)
    recipe_a = make_recipe(TABSDATA_PACKAGE, location=project_root)
    recipe_b = make_recipe("tabsdata-toolkit", location=project_root)

    environment.link_libexec_launchers((recipe_a, recipe_b))

    target = env_root / "libexec" / "tabsdata"
    assert len(list(target.iterdir())) == 1


def stub_recipe(install: InstallRecipe, *, version: str | None = "1.2.3"):
    return PackageRecipe(
        name="stub",
        recipe=install,
        version=version,
        location=None,
    )


def patch_environment(
    monkeypatch,
    *,
    installed: dict[str, object],
    locked: frozenset[str] = frozenset(),
):
    """Stub the three boundary points synthesise() depends on:

    * `installed_packages()` returns the canonical names of the keys in
      `installed` (the dist-info scan).
    * `Package(name).recipe()` returns the matching value from `installed`
      (a PackageRecipe stub or None).
    * `locked_packages()` returns the supplied `locked` set.
    * `uv()` echoes its stdin back so the returned list equals the input
      constraints unchanged.
    """

    class PackageStub:
        def __init__(self, name, mandatory: bool = False):
            self.name = name

        def recipe(self):
            if self.name not in installed:
                raise AssertionError(f"unexpected Package lookup: {self.name}")
            return installed[self.name]

    monkeypatch.setattr("tabsdatak.common._interpreter.python.venv.Package", PackageStub)
    monkeypatch.setattr(
        "tabsdatak.common._interpreter.python.venv.PythonEnvironment.installed_packages",
        staticmethod(lambda: frozenset(installed.keys())),
    )
    monkeypatch.setattr(
        "tabsdatak.common._interpreter.python.venv.PythonEnvironment.locked_packages",
        staticmethod(lambda: locked),
    )
    monkeypatch.setattr(
        "tabsdatak.common._interpreter.python.venv.PythonEnvironment.uv",
        staticmethod(lambda arguments, *, stdin=None: stdin or ""),
    )


def test_synthesise_python_version_is_dotted_triple():
    _, python_version = PythonEnvironment.synthesise()
    parts = python_version.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)


def test_synthesise_emits_registry_installs(monkeypatch):
    patch_environment(
        monkeypatch, installed={"polars": stub_recipe(InstallRecipe.REGISTRY)}
    )
    requirements, _ = PythonEnvironment.synthesise()
    assert requirements == ["polars==1.2.3"]


def test_synthesise_drops_editable_installs(monkeypatch):
    patch_environment(
        monkeypatch,
        installed={"my-local-pkg": stub_recipe(InstallRecipe.EDITABLE)},
    )
    requirements, _ = PythonEnvironment.synthesise()
    assert requirements == []


def test_synthesise_drops_local_wheel_installs(monkeypatch):
    patch_environment(
        monkeypatch, installed={"wheel-pkg": stub_recipe(InstallRecipe.WHEEL)}
    )
    requirements, _ = PythonEnvironment.synthesise()
    assert requirements == []


def test_synthesise_drops_frozen_folder_installs(monkeypatch):
    patch_environment(
        monkeypatch, installed={"frozen-pkg": stub_recipe(InstallRecipe.FROZEN)}
    )
    requirements, _ = PythonEnvironment.synthesise()
    assert requirements == []


def test_synthesise_drops_unresolvable_packages(monkeypatch):
    """Package.recipe() returns None when distribution metadata can't
    be found -- e.g. the entry was uninstalled mid-snapshot."""
    patch_environment(monkeypatch, installed={"ghost-pkg": None})
    requirements, _ = PythonEnvironment.synthesise()
    assert requirements == []


def test_synthesise_drops_versionless_recipes(monkeypatch):
    """A registry install with no version is dropped (pip would refuse
    the unpinned constraint and we'd rather skip than emit it bare)."""
    patch_environment(
        monkeypatch,
        installed={"versionless": stub_recipe(InstallRecipe.REGISTRY, version=None)},
    )
    requirements, _ = PythonEnvironment.synthesise()
    assert requirements == []


def test_synthesise_mixes_kinds_keeping_only_registry(monkeypatch):
    patch_environment(
        monkeypatch,
        installed={
            "polars": stub_recipe(InstallRecipe.REGISTRY),
            "my-local-pkg": stub_recipe(InstallRecipe.EDITABLE),
            "wheel-pkg": stub_recipe(InstallRecipe.WHEEL),
            "boto3": stub_recipe(InstallRecipe.REGISTRY),
        },
    )
    requirements, _ = PythonEnvironment.synthesise()
    assert requirements == ["boto3==1.2.3", "polars==1.2.3"]


def test_synthesise_excludes_tabsdata_dependencies_packages(monkeypatch):
    """Names appearing in the tabsdata lock are excluded from extras."""
    patch_environment(
        monkeypatch,
        installed={
            "polars": stub_recipe(InstallRecipe.REGISTRY),
            "boto3": stub_recipe(InstallRecipe.REGISTRY),
        },
        locked=frozenset({"boto3"}),
    )
    requirements, _ = PythonEnvironment.synthesise()
    assert requirements == ["polars==1.2.3"]


def test_synthesise_excludes_tabsdata_prefixed_packages(monkeypatch):
    """Any installed package whose canonical name starts with `tabsdata`
    is owned by tabsdata and never emitted as an extra, regardless of
    lock contents."""
    patch_environment(
        monkeypatch,
        installed={
            "tabsdata-api": stub_recipe(InstallRecipe.REGISTRY),
            "polars": stub_recipe(InstallRecipe.REGISTRY),
        },
    )
    requirements, _ = PythonEnvironment.synthesise()
    assert requirements == ["polars==1.2.3"]


def test_synthesise_falls_back_to_online_when_offline_uv_fails(monkeypatch):
    """First uv call uses --offline; a VenvCreationError triggers a retry
    without --offline. The fallback path must still produce results."""
    from tabsdatak.common._interpreter.python.error import VenvCreationError

    calls: list[list[str]] = []

    def fake_uv(arguments, *, stdin=None):
        calls.append(arguments)
        if "--offline" in arguments:
            raise VenvCreationError(detail="offline cache miss")
        return stdin or ""

    monkeypatch.setattr(
        "tabsdatak.common._interpreter.python.venv.PythonEnvironment.uv",
        staticmethod(fake_uv),
    )
    monkeypatch.setattr(
        "tabsdatak.common._interpreter.python.venv.PythonEnvironment.installed_packages",
        staticmethod(lambda: frozenset({"polars"})),
    )
    monkeypatch.setattr(
        "tabsdatak.common._interpreter.python.venv.PythonEnvironment.locked_packages",
        staticmethod(lambda: frozenset()),
    )

    class PackageStub:
        def __init__(self, name, mandatory: bool = False):
            self.name = name

        def recipe(self):
            return stub_recipe(InstallRecipe.REGISTRY)

    monkeypatch.setattr("tabsdatak.common._interpreter.python.venv.Package", PackageStub)

    requirements, _ = PythonEnvironment.synthesise()
    assert requirements == ["polars==1.2.3"]
    assert len(calls) == 2
    assert "--offline" in calls[0]
    assert "--offline" not in calls[1]


def test_installed_packages_returns_canonical_names(
    monkeypatch, tmp_path: Path
):
    """Direct test of the dist-info scan helper. Builds a fake site-packages
    with a mix of .dist-info / .egg-info / unrelated entries and confirms
    that the canonical names are extracted from the directory names."""
    site_packages = tmp_path / "site-packages"
    site_packages.mkdir()
    (site_packages / "Tabsdata_API-1.0.0.dist-info").mkdir()
    (site_packages / "polars-1.2.3.dist-info").mkdir()
    (site_packages / "legacy_pkg-0.1.egg-info").mkdir()
    (site_packages / "not-a-dist").mkdir()

    monkeypatch.setattr(
        "tabsdatak.common._interpreter.python.venv.site.getsitepackages",
        lambda: [str(site_packages)],
    )
    monkeypatch.setattr(
        "tabsdatak.common._interpreter.python.venv.site.getusersitepackages",
        lambda: "",
    )

    names = PythonEnvironment.installed_packages()
    assert names == frozenset({"tabsdata-api", "polars", "legacy-pkg"})


def patch_locks(
    monkeypatch,
    tmp_path: Path,
    *,
    tabsdata_recipe: PackageRecipe,
    lock_files: dict[str, str],
):
    """Stub td_resource and Package(TABSDATA_PACKAGE).recipe() so the
    locked-names loader sees the supplied lock contents in the appropriate
    install mode.

    `lock_files` maps resource path (`resources/lock/<file>.txt`) to lock
    body text. Missing resources raise FileNotFoundError.
    """

    def fake_td_resource(resource: str, *, package: str = "tabsdatak") -> Path:
        if resource not in lock_files:
            raise FileNotFoundError(f"missing fake resource: {resource}")
        target = tmp_path / resource.replace("/", "_")
        target.write_text(lock_files[resource], encoding="utf-8")
        return target

    class PackageStub:
        def __init__(self, name, mandatory: bool = False):
            self.name = name

        def recipe(self):
            if self.name == TABSDATA_PACKAGE:
                return tabsdata_recipe
            raise AssertionError(f"unexpected Package lookup: {self.name}")

    monkeypatch.setattr("tabsdatak.common._interpreter.python.venv.Package", PackageStub)
    monkeypatch.setattr(
        "tabsdatak.common._interpreter.python.venv.td_resource", fake_td_resource
    )


def tabsdata_recipe(install: InstallRecipe, *, location: Path | None) -> PackageRecipe:
    return PackageRecipe(
        name=TABSDATA_PACKAGE,
        recipe=install,
        version="2.0.0",
        location=location,
    )


def test_locked_packages_loads_runtime_lock_only_for_registry(
    monkeypatch, tmp_path: Path
):
    patch_locks(
        monkeypatch,
        tmp_path,
        tabsdata_recipe=tabsdata_recipe(InstallRecipe.REGISTRY, location=None),
        lock_files={
            "resources/lock/tabsdata-requirements.txt": (
                "polars==1.2.3\nboto3==2.0.0\n"
            ),
            "resources/lock/tabsdata-requirements-dev.txt": "pytest==8.0.0\n",
        },
    )
    names = PythonEnvironment.locked_packages()
    assert names == frozenset({"polars", "boto3"})


def test_locked_packages_loads_both_locks_for_editable(
    monkeypatch, tmp_path: Path
):
    patch_locks(
        monkeypatch,
        tmp_path,
        tabsdata_recipe=tabsdata_recipe(InstallRecipe.EDITABLE, location=tmp_path),
        lock_files={
            "resources/lock/tabsdata-requirements.txt": (
                "polars==1.2.3\nboto3==2.0.0\n"
            ),
            "resources/lock/tabsdata-requirements-dev.txt": (
                "pytest==8.0.0\nruff==0.5.0\n"
            ),
        },
    )
    names = PythonEnvironment.locked_packages()
    assert names == frozenset({"polars", "boto3", "pytest", "ruff"})


def test_locked_packages_skips_comments_and_blank_lines(
    monkeypatch, tmp_path: Path
):
    patch_locks(
        monkeypatch,
        tmp_path,
        tabsdata_recipe=tabsdata_recipe(InstallRecipe.REGISTRY, location=None),
        lock_files={
            "resources/lock/tabsdata-requirements.txt": (
                "# Copyright header\n"
                "\n"
                "polars==1.2.3\n"
                "  # indented comment\n"
                "boto3==2.0.0\n"
            ),
        },
    )
    names = PythonEnvironment.locked_packages()
    assert names == frozenset({"polars", "boto3"})


def test_locked_packages_preserves_markered_lines(
    monkeypatch, tmp_path: Path
):
    """PEP 508 markers in lock lines don't affect canonical-name extraction."""
    patch_locks(
        monkeypatch,
        tmp_path,
        tabsdata_recipe=tabsdata_recipe(InstallRecipe.REGISTRY, location=None),
        lock_files={
            "resources/lock/tabsdata-requirements.txt": (
                "cffi==2.0.0 ; platform_python_implementation != 'PyPy'\n"
                "pywin32==311 ; sys_platform == 'win32'\n"
            ),
        },
    )
    names = PythonEnvironment.locked_packages()
    assert names == frozenset({"cffi", "pywin32"})


def test_locked_packages_raises_when_runtime_lock_missing(
    monkeypatch, tmp_path: Path
):
    patch_locks(
        monkeypatch,
        tmp_path,
        tabsdata_recipe=tabsdata_recipe(InstallRecipe.REGISTRY, location=None),
        lock_files={},
    )
    with pytest.raises(FileNotFoundError):
        PythonEnvironment.locked_packages()


@pytest.mark.integration
def test_synthesise_runs_real_uv_against_producer_environment():
    """End-to-end smoke: runs the real `uv pip compile --universal` against
    whatever packages the test environment has installed. Validates that
    `uv` is on PATH, accepts the arguments we pass, and emits a parsable
    requirements.txt. Skipped if no registry-installed packages are
    present (uv would have nothing to resolve)."""
    requirements, python_version = PythonEnvironment.synthesise()
    assert python_version.count(".") == 2
    if not requirements:
        pytest.skip("no extras beyond the tabsdata lock in this environment")
    for line in requirements:
        assert "==" in line
