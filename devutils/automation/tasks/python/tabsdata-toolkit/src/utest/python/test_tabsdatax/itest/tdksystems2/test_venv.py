#
# Copyright 2026 Tabsdata Inc.
#

"""Managed-venv routing: which env each invocation re-execs into, and that
``bootstrap`` creates + re-execs exactly once (never for the cases that must stay
in the launcher interpreter).

Nothing here builds a real venv — ``ensure`` and ``os.execv`` are recorded.
"""

from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems2 import _venv, constants, descriptor, store

SYS = constants.SYS_TOOL
DOM = constants.DOM_TOOL


@pytest.fixture
def seeded(stores):
    """A mysql system, a mysql domain and a salesforce domain in the store."""
    descriptor.save(
        descriptor.SystemInfo(
            system="mysql", name="m1", started=descriptor.now_stamp()
        ),
        store.system_path("m1"),
    )
    descriptor.save(
        descriptor.DomainInfo(
            system="mysql",
            name="d1",
            started=descriptor.now_stamp(),
            sys_name="m1",
            external_name="x",
            model="employees",
        ),
        store.domain_path("d1"),
    )
    descriptor.save(
        descriptor.DomainInfo(
            system="salesforce",
            name="sf1",
            started=descriptor.now_stamp(),
            sys_name="s1",
            external_name="x",
            model="salesforce",
        ),
        store.domain_path("sf1"),
    )
    return stores


@pytest.mark.parametrize(
    "tool,argv",
    [
        (SYS, []),
        (SYS, ["-h"]),
        (SYS, ["--help"]),
        (SYS, [constants.V_CLEAR_VENVS]),
        # a batch parent orchestrates children that bootstrap themselves
        (SYS, ["create", "--type", "ALL"]),
        (SYS, ["create", "--type", "mysql,postgres"]),
    ],
)
def test_stays_in_launcher(tool, argv):
    assert _venv.target_env(tool, argv) is None


@pytest.mark.parametrize(
    "verb", [constants.V_TYPES, constants.V_LIST, constants.V_INFO, constants.V_MANIFEST]
)
def test_common_verbs_need_no_drivers(verb):
    assert _venv.target_env(SYS, [verb]) == _venv.ENV_C


def test_verb_help_needs_no_drivers():
    assert _venv.target_env(SYS, ["create", "--type", "mysql", "-h"]) == _venv.ENV_C


@pytest.mark.parametrize(
    "argv,expected",
    [
        # a container's outside is docker-only; an object store's is not
        (["create", "--type", "mysql"], _venv.ENV_C),
        (["fetch-bin", "--type", "db2"], _venv.ENV_C),
        (["create", "--type", "s3"], _venv.ENV_A),
        (["create", "--type", "snowflake"], _venv.ENV_A),
    ],
)
def test_system_create_routes_on_type(argv, expected):
    assert _venv.target_env(SYS, argv) == expected


@pytest.mark.parametrize(
    "tool,argv,expected",
    [
        (SYS, ["destroy", "--sys", "m1"], _venv.ENV_C),
        (SYS, ["status", "--sys", "m1"], _venv.ENV_C),
        # purge is a system verb that reaches inside, so it takes dom_env
        (SYS, ["purge", "--sys", "m1"], _venv.ENV_A),
        (DOM, ["create", "--sys", "m1"], _venv.ENV_A),
        (DOM, ["status", "--domain", "d1"], _venv.ENV_A),
        (DOM, ["create-data", "--domain", "d1"], _venv.ENV_A),
        # the salesforce data plane cannot share an env with the SQL drivers
        (DOM, ["create-data", "--domain", "sf1"], _venv.ENV_B),
    ],
)
def test_routes_from_the_descriptor(seeded, tool, argv, expected):
    assert _venv.target_env(tool, argv) == expected


def test_model_overrides_the_domain_env(seeded, monkeypatch):
    monkeypatch.setattr(
        _venv, "env_for_model", lambda model: _venv.ENV_B if model == "employees" else None
    )
    assert _venv.target_env(DOM, ["create-data", "--domain", "d1"]) == _venv.ENV_B
    # ... but only for the data plane
    assert _venv.target_env(DOM, ["status", "--domain", "d1"]) == _venv.ENV_A


def test_model_env_read_from_model_json(tmp_path, monkeypatch):
    declared = tmp_path / "models" / "widgets"
    declared.mkdir(parents=True)
    (declared / "model.json").write_text('{"env": "tdx-sys-b"}', encoding="utf-8")
    (tmp_path / "models" / "gadgets").mkdir()  # a model that declares nothing
    monkeypatch.setattr(_venv, "__file__", str(tmp_path / "_venv.py"))
    assert _venv.env_for_model("widgets") == _venv.ENV_B
    assert _venv.env_for_model("gadgets") is None


@pytest.mark.parametrize(
    "tool,argv",
    [
        (SYS, ["destroy", "--sys", "does-not-exist"]),
        (SYS, ["create", "--type", "no-such-system"]),
        (DOM, ["status", "--domain", "does-not-exist"]),
        (SYS, ["destroy"]),  # no target at all
    ],
)
def test_unresolvable_targets_fall_back_to_the_driver_free_env(stores, tool, argv):
    assert _venv.target_env(tool, argv) == _venv.ENV_C


def test_every_system_declares_both_envs():
    from tabsdatax.itest.tdksystems2 import systems

    for slug in systems.all_slugs():
        cls = systems.get_class(slug)
        assert cls.sys_env in _venv.ENVS, slug
        assert cls.dom_env in _venv.ENVS, slug


def test_every_env_has_a_requirements_file():
    for env in _venv.ENVS:
        assert _venv.requirements_file(env).is_file(), env


def test_the_driver_env_declares_a_dialect_for_every_sql_backend():
    """A backend's ``driver`` is a SQLAlchemy dialect name, and the package that
    registers it is often not the one named after the database — clickhousedb
    comes from clickhouse-connect, snowflake from snowflake-sqlalchemy. Nothing
    else checks the requirements actually cover them, and a gap only shows as
    NoSuchModuleError against a live system."""
    from tabsdatax.itest.tdksystems2 import systems

    reqs = _venv.requirements_file(_venv.ENV_A).read_text(encoding="utf-8")
    declared = {
        line.split(";")[0].split("[")[0].strip().lower()
        for line in reqs.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    # dialect name (Backend.driver, before any "+driver") -> distribution
    providers = {
        "clickhousedb": "clickhouse-connect",
        "databricks": "databricks-sqlalchemy",
        "snowflake": "snowflake-sqlalchemy",
        "db2": "ibm_db_sa",
        "mssql": "pyodbc",
        "mysql": "mysql-connector-python",
        "oracle": "oracledb",
        "postgresql": "psycopg2-binary",
    }
    for slug in systems.all_slugs():
        driver = getattr(systems.get_class(slug), "driver", "")
        if not driver:
            continue
        dialect = driver.split("+")[0]
        assert dialect in providers, f"{slug}: no known provider for '{dialect}'"
        assert providers[dialect] in declared, (
            f"{slug} speaks '{dialect}', which {providers[dialect]} registers — "
            f"add it to {_venv.requirements_file(_venv.ENV_A).name}"
        )


def test_bootstrap_creates_then_execs(monkeypatch):
    calls: dict[str, object] = {}
    monkeypatch.setattr("sys.argv", ["tdx-system", "types"])
    monkeypatch.setattr(_venv, "ensure", lambda env: calls.setdefault("ensure", env))
    monkeypatch.setattr(
        _venv.os, "execv", lambda py, args: calls.setdefault("execv", (py, args))
    )
    _venv.bootstrap(SYS, "mod")
    assert calls["ensure"] == _venv.ENV_C
    py, args = calls["execv"]
    assert str(_venv.venv_path(_venv.ENV_C)) in py
    assert args[1:] == ["-m", "mod", "types"]


def test_bootstrap_is_a_noop_inside_the_target_venv(monkeypatch):
    monkeypatch.setattr("sys.argv", ["tdx-system", "types"])
    monkeypatch.setattr("sys.prefix", str(_venv.venv_path(_venv.ENV_C)))
    monkeypatch.setattr(
        _venv, "ensure", lambda env: pytest.fail("must not rebuild the current venv")
    )
    monkeypatch.setattr(
        _venv.os, "execv", lambda py, args: pytest.fail("must not re-exec")
    )
    _venv.bootstrap(SYS, "mod")


def test_clear_venvs_removes_every_managed_venv(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(_venv, "ENVS_DIR", tmp_path)
    present = _venv.venv_path(_venv.ENV_A)
    (present / "lib").mkdir(parents=True)
    assert _venv.clear_venvs() == 0
    assert not present.exists()
    out = capsys.readouterr().out
    assert f"removed {present}" in out
    assert f"(absent) {_venv.venv_path(_venv.ENV_B)}" in out


def test_types_reports_the_venv_of_each_plane(capsys):
    from tabsdatax.itest.tdksystems2 import tdx

    assert tdx._cmn_types(SYS, []) == 0
    lines = capsys.readouterr().out.splitlines()
    assert "SYS-ENV" in lines[0] and "DOM-ENV" in lines[0]
    # the venv's own name, so it can be copied straight to a path or an rm
    mysql = next(ln for ln in lines if ln.split()[0] == "mysql").split()
    assert mysql[3:5] == [_venv.venv_name(_venv.ENV_C), _venv.venv_name(_venv.ENV_A)]
    salesforce = next(ln for ln in lines if ln.split()[0] == "salesforce").split()
    assert salesforce[4] == _venv.venv_name(_venv.ENV_B) == "_tdx-sys-b"


def test_bootstrap_never_touches_the_launcher_only_verbs(monkeypatch):
    monkeypatch.setattr("sys.argv", ["tdx-system", constants.V_CLEAR_VENVS])
    monkeypatch.setattr(
        _venv, "ensure", lambda env: pytest.fail("clear-venvs must not build a venv")
    )
    monkeypatch.setattr(
        _venv.os, "execv", lambda py, args: pytest.fail("clear-venvs must not re-exec")
    )
    _venv.bootstrap(SYS, "mod")
