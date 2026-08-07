#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end verb dispatch with docker + SQL faked at the backend boundary.

Every method that would touch Docker or a live database is stubbed, so these
tests exercise the real parse -> resolve -> descriptor-write path (the same one
the CLI runs) without external services.
"""

from __future__ import annotations

import json

import pytest

from tabsdatax.itest.tdksystems2 import datagen, descriptor, store, tdx
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.systems.mysql.backend import Mysql


@pytest.fixture
def faked(monkeypatch):
    """Stub the Docker / SQL touching methods on the MySQL backend."""
    entities: list[str] = []
    purged: list[str] = []
    monkeypatch.setattr(Mysql, "provision_system", lambda self, meta, force=False: {})
    monkeypatch.setattr(Mysql, "system_status", lambda self, meta: (True, "up"))
    monkeypatch.setattr(Mysql, "destroy_system", lambda self, meta: None)
    monkeypatch.setattr(Mysql, "restart_system", lambda self, meta: "restarted")
    monkeypatch.setattr(Mysql, "list_domain_entities", lambda self, meta: list(entities))
    monkeypatch.setattr(
        Mysql, "purge_domain_entity", lambda self, meta, name: purged.append(name)
    )
    monkeypatch.setattr(
        Mysql, "provision_domain",
        lambda self, sys_meta, entity, dmeta, force=False: None,
    )
    monkeypatch.setattr(Mysql, "destroy_domain", lambda self, dmeta: None)
    monkeypatch.setattr(Mysql, "domain_status", lambda self, dmeta: (True, "up"))
    monkeypatch.setattr(Mysql, "install_schema", lambda self, uri, model, force=False: None)
    monkeypatch.setattr(
        Mysql, "load_tables",
        lambda self, uri, tables: {t.name: len(t.rows) for t in tables},
    )
    def _apply_changes(self, uri, txns, before):
        # stub only the SQL write; keep the real entity-count accounting
        new = dict(before)
        for table, delta in datagen.net_row_deltas(txns).items():
            new[table] = max(0, new.get(table, 0) + delta)
        return new

    monkeypatch.setattr(Mysql, "apply_changes", _apply_changes)
    return {"entities": entities, "purged": purged}


def _sys(argv):
    return tdx._dispatch("tdx-system", argv)


def _dom(argv):
    return tdx._dispatch("tdx-domain", argv)


def test_verb_discovery():
    assert set(tdx._discover("tdx-system")) == {
        "create", "destroy", "status", "purge", "fetch-bin", "restart", "connect"
    }
    assert set(tdx._discover("tdx-domain")) == {
        "create", "destroy", "status", "models", "create-model", "create-data",
        "modify-data",
    }


def test_unknown_verb_raises():
    with pytest.raises(TdxError):
        _sys(["frobnicate"])


def test_bare_create_help_shows_no_type_specific_options(capsys):
    # Without --type there is no type to speak for; borrowing the first
    # registered system's options would advertise them as generic.
    with pytest.raises(SystemExit):
        _sys(["create", "-h"])
    out = capsys.readouterr().out
    assert "--api-key" not in out and "ANTHROPIC" not in out
    assert "--type TYPE -h" in out  # points at where they come from
    assert "--force" in out  # the generic ones are still there


def test_create_help_for_a_named_type_shows_its_options(capsys):
    with pytest.raises(SystemExit):
        _sys(["create", "--type", "anthropic", "-h"])
    out = capsys.readouterr().out
    assert "--api-key" in out and "ANTHROPIC__API_KEY" in out
    with pytest.raises(SystemExit):
        _sys(["create", "--type", "mysql", "-h"])
    out = capsys.readouterr().out
    assert "--port" in out and "--api-key" not in out


@pytest.mark.parametrize(
    "tool,verb",
    [("tdx-system", v) for v in ("create", "destroy", "status", "restart", "purge")]
    + [("tdx-domain", v) for v in ("create", "destroy", "status")],
)
def test_every_verb_that_takes_a_manifest_says_so(tool, verb, capsys):
    """The dispatcher peels --manifest off raw argv before argparse, so a verb
    that accepts one can easily fail to declare it — and then -h denies the
    option exists."""
    with pytest.raises(SystemExit):
        tdx._dispatch(tool, [verb, "-h"])
    assert "--manifest" in capsys.readouterr().out


def test_the_verbs_that_declare_a_manifest_are_the_ones_that_honour_it():
    # what the dispatcher actually intercepts ...
    intercepts = set(tdx._MANIFEST_VERBS) | {"create"}
    # ... must be what the catalogs' parsers advertise
    for tool, catalog in (
        ("tdx-system", tdx._SYS_CATALOG),
        ("tdx-domain", tdx._DOM_CATALOG),
    ):
        for verb in catalog:
            module = tdx._command_module(tool, tdx._default_slug(tool, verb), verb)
            p = tdx.ArgumentParser(prog="x")
            p.tdx_system = None
            module.add_arguments(p)
            declared = any(
                "--manifest" in (a.option_strings or []) for a in p._actions
            )
            assert declared == (verb in intercepts), (
                f"{tool} {verb}: declares --manifest={declared} but the "
                f"dispatcher intercepts it={verb in intercepts}"
            )


def test_manifest_children_are_pointed_at_the_callers_store():
    """A manifest run reads the store the caller named, so the children it spawns
    have to write to that one — otherwise create lands in the default store while
    --sync reconciles against the named one."""
    sys_steps = tdx._entry_steps(
        "tdx-system",
        {"name": "s1", "type": "mysql"},
        force=False,
        ports=None,
        store_dir_opt="/tmp/store",
    )
    assert ["--sys-dir", "/tmp/store"] == sys_steps[0][-2:]
    dom_steps = tdx._entry_steps(
        "tdx-domain",
        {"name": "d1", "sys": "s1", "model": "employees", "size": 1},
        force=False,
        ports=None,
        store_dir_opt="/tmp/dstore",
    )
    assert len(dom_steps) == 3  # create -> create-model -> create-data
    for step in dom_steps:
        assert "--domain-dir" in step and "/tmp/dstore" in step
    # and nothing is added when the caller did not name one
    plain = tdx._entry_steps(
        "tdx-system", {"name": "s1", "type": "mysql"}, force=False, ports=None
    )
    assert "--sys-dir" not in plain[0]


def test_full_lifecycle(stores, faked):
    assert _sys(["create", "--type", "mysql", "--sys", "sys1"]) == 0
    sdoc = descriptor.load(store.system_path("sys1"))
    assert sdoc.role == "system" and sdoc.system == "mysql"

    assert _dom(["create", "--sys", "sys1", "--domain", "dom1"]) == 0
    dpath = store.domain_path("dom1")
    ddoc = descriptor.load(dpath)
    assert ddoc.sys_name == "sys1" and ddoc.external_name == "d_u_0_dom1"
    meta = ddoc.meta
    assert meta["SQLALCHEMY_URI"].startswith("mysql+mysqlconnector://")
    # URI is the same target with no credentials
    assert meta["URI"].endswith("/d_u_0_dom1")
    assert "@" not in meta["URI"] and meta["PASSWORD"] not in meta["URI"]

    assert _dom(["create-model", "--domain", "dom1", "--model", "employees"]) == 0
    assert descriptor.load(dpath).model == "employees"

    assert _dom(["create-data", "--domain", "dom1", "--size", "1"]) == 0
    ddoc = descriptor.load(dpath)
    assert ddoc.data == "1" and ddoc.entities["employees"] == 100

    # Mix over 3 sets = insert + update + delete: employees nets 0 (100 stays)
    assert _dom(["modify-data", "--domain", "dom1", "--entity", "ALL",
                 "--sets", "3", "--ops", "Mix", "--seed", "AUTO"]) == 0
    ddoc = descriptor.load(dpath)
    assert ddoc.seed == 1 and ddoc.entities["employees"] == 100

    # 2 pure inserts push the recorded count to 102
    assert _dom(["modify-data", "--domain", "dom1", "--sets", "2",
                 "--ops", "Insert", "--seed", "AUTO"]) == 0
    ddoc = descriptor.load(dpath)
    assert ddoc.seed == 2 and ddoc.entities["employees"] == 102

    assert _dom(["status", "--domain", "dom1"]) == 0
    assert _sys(["status", "--sys", "sys1"]) == 0

    assert _dom(["destroy", "--domain", "dom1"]) == 0
    assert not descriptor.is_descriptor(dpath)
    assert _sys(["destroy", "--sys", "sys1"]) == 0
    assert not descriptor.is_descriptor(store.system_path("sys1"))


def test_create_twice_needs_force(stores, faked):
    assert _sys(["create", "--type", "mysql", "--sys", "s"]) == 0
    with pytest.raises(TdxError):
        _sys(["create", "--type", "mysql", "--sys", "s"])
    assert _sys(["create", "--type", "mysql", "--sys", "s", "-f"]) == 0


def test_domain_create_requires_sys(stores, faked):
    with pytest.raises(TdxError):
        _dom(["create", "--domain", "d"])


@pytest.mark.parametrize("sel", ["ALL", "mysql,postgres"])
def test_system_create_points_a_sys_batch_at_type(sel, stores, faked):
    """Every other verb batches on --sys, so `create --sys ALL` is an easy slip;
    it must not surface as a bare '--type is required'."""
    with pytest.raises(TdxError) as exc:
        _sys(["create", "--sys", sel, "--sys-hostname", "myhost"])
    msg = str(exc.value)
    assert "batches on --type" in msg
    assert f"create --type {sel} --sys-hostname myhost" in msg


def test_system_create_keeps_a_lone_sys_name(stores, faked):
    # only a batch selector is redirected; --sys still names one system
    assert _sys(["create", "--type", "mysql", "--sys", "ALLOY"]) == 0
    assert descriptor.is_descriptor(store.system_path("ALLOY"))


def test_name_in_system_overrides_prefix(stores, faked):
    _sys(["create", "--type", "mysql", "--sys", "s"])
    _dom(["create", "--sys", "s", "--domain", "d", "--name-in-system", "verbatim_db"])
    assert descriptor.load(store.domain_path("d")).external_name == "verbatim_db"


def test_modify_data_without_data_raises(stores, faked):
    _sys(["create", "--type", "mysql", "--sys", "s"])
    _dom(["create", "--sys", "s", "--domain", "d"])
    _dom(["create-model", "--domain", "d", "--model", "employees"])
    with pytest.raises(TdxError):
        _dom(["modify-data", "--domain", "d", "--ops", "Insert"])


def test_create_model_unknown_model_raises(stores, faked):
    _sys(["create", "--type", "mysql", "--sys", "s"])
    _dom(["create", "--sys", "s", "--domain", "d"])
    with pytest.raises(TdxError):
        _dom(["create-model", "--domain", "d", "--model", "nope"])


def test_purge_previews_then_drops(stores, faked):
    faked["entities"].extend(["d_u_0_stale", "someone_else_db"])
    _sys(["create", "--type", "mysql", "--sys", "s"])
    # preview (no -f): matches the default prefix, drops nothing
    assert _sys(["purge", "--sys", "s"]) == 0
    assert faked["purged"] == []
    # -f drops only the prefix-matched entity
    assert _sys(["purge", "--sys", "s", "-f"]) == 0
    assert faked["purged"] == ["d_u_0_stale"]


def test_out_json_reports_iud_counts(stores, faked, capsys):
    _sys(["create", "--type", "mysql", "--sys", "s"])
    _dom(["create", "--sys", "s", "--domain", "d"])
    capsys.readouterr()  # discard human output so far

    # create-model: every entity present, zero rows
    assert _dom(["create-model", "--domain", "d", "--model", "employees",
                 "--out", "json"]) == 0
    rep = json.loads(capsys.readouterr().out)
    assert set(rep) == set(datagen.entities_of("employees"))
    assert rep["employees"] == {"insert": 0, "update": 0, "delete": 0}

    # create-data: each entity's rows show up as inserts
    assert _dom(["create-data", "--domain", "d", "--size", "1", "--out", "json"]) == 0
    rep = json.loads(capsys.readouterr().out)
    assert rep["employees"] == {"insert": 100, "update": 0, "delete": 0}

    # modify-data: only touched tables, with their I/U/D counts (no human noise)
    assert _dom(["modify-data", "--domain", "d", "--sets", "2", "--ops", "Insert",
                 "--seed", "AUTO", "--out", "json"]) == 0
    out = capsys.readouterr().out
    assert json.loads(out) == {"employees": {"insert": 2, "update": 0, "delete": 0}}
    assert "applied" not in out and "skipped" not in out


def test_models_lists_employees(stores, faked, capsys):
    _sys(["create", "--type", "mysql", "--sys", "s"])
    _dom(["create", "--sys", "s", "--domain", "d"])
    assert _dom(["models", "--domain", "d"]) == 0
    assert "employees" in capsys.readouterr().out
