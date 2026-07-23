"""`tdx-system purge`: drop domain entities straight from a connected system,
WITHOUT reading any domain descriptor (so it cleans up domains left by crashed
runs). It scopes to what is ours: containers OWNED BY THE LOGIN USER on
owner-aware backends (snowflake / databricks / bigquery), else the containers
whose name matches the prefix — this run's own `d_<user>_<build>_` by default
(the build identity is pinned to `u_0` by conftest), or the raw `--prefix`.
Exercised end-to-end on the fs backend (no docker / creds), plus the match
predicate, both purge branches, per-base delegation, and registration."""
from __future__ import annotations

import os

import pytest

from tabsdatax.itest.tdksystems import store, tdx
from tabsdatax.itest.tdksystems.systems.base import SystemContext, Unsupported
from tabsdatax.itest.tdksystems.systems.fs.system import FsSystem

from .test_tdx import fs_root  # noqa: F401 - shared /tmp fs-root fixture


# ---- the match predicate ---------------------------------------------------

def test_purge_match_default_is_this_runs_prefix():
    # conftest pins the identity to u_0, so the default prefix is `d_u_0_`.
    assert tdx._purge_match("d_u_0_sales", None) is True
    assert tdx._purge_match("D_U_0_SALES", None) is True   # case-insensitive (oracle/db2)
    assert tdx._purge_match("d_other_0_x", None) is False  # a different user
    assert tdx._purge_match("d_u_1_x", None) is False      # a different build
    assert tdx._purge_match("information_schema", None) is False


def test_purge_match_prefix_is_raw_and_allows_empty():
    # An explicit --prefix is used verbatim (include `d_` yourself)...
    assert tdx._purge_match("d_ci_5_x", "d_ci_") is True
    assert tdx._purge_match("d_u_0_x", "d_ci_") is False
    assert tdx._purge_match("tucu_0_legacy", "tucu_") is True  # matches non-d_ too
    # ... and `--prefix ""` matches every container.
    assert tdx._purge_match("anything_at_all", "") is True


# ---- fit-to-length (trim + counter) ----------------------------------------

class _FakeSys:
    slug = "x"
    max_entity_name_length = 10

    def __init__(self, existing):
        self._existing = existing

    def list_domain_entities(self, ctx):
        return self._existing


def test_fit_entity_name_leaves_short_names_untouched():
    assert tdx._fit_entity_name(_FakeSys([]), None, "d_u_0_x") == "d_u_0_x"


def test_fit_entity_name_trims_and_appends_counter():
    # len 10 max -> trim to 6 + `__NN`; lowest free counter, case-insensitive.
    assert tdx._fit_entity_name(_FakeSys([]), None, "d_verylongname") == "d_very__00"
    assert (
        tdx._fit_entity_name(_FakeSys(["d_very__00"]), None, "d_verylongname")
        == "d_very__01"
    )
    assert (
        tdx._fit_entity_name(_FakeSys(["D_VERY__00"]), None, "d_verylongname")
        == "d_very__01"  # collision check ignores case
    )


# ---- registration ----------------------------------------------------------

def test_purge_registered_logged_and_latchable():
    assert "purge" in tdx._SYS_VERBS
    assert "purge" not in tdx._DOMAIN_VERBS
    assert "purge" in tdx._LOGGED_VERBS   # brief console + timestamped log
    assert "purge" in tdx._LATCH_VERBS    # long-running -> waitable


# ---- fs end-to-end (no-owner backend: prefix match) ------------------------

def _fs_with_entities(fs_root):  # noqa: F811
    tdx._sys_create(["--type", "fs", "--sys", "a", "--sys-path", str(fs_root), "-f"])
    # Default naming -> d_u_0_<domain> (u_0 pinned by conftest): purge-matched.
    tdx._domain_create(["--sys", "a", "--domain", "sales", "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "inv", "-f"])
    # Another user's container and a bare/system-ish one: NOT matched by default.
    (fs_root / "d_other_9_stale").mkdir()
    (fs_root / "config").mkdir()
    assert (fs_root / "d_u_0_sales").is_dir()
    assert (fs_root / "d_u_0_inv").is_dir()


def test_purge_dry_run_previews_this_runs_containers(stores, fs_root, capsys):  # noqa: F811
    _fs_with_entities(fs_root)
    capsys.readouterr()
    assert tdx._sys_purge(["--sys", "a"]) == 0
    out = capsys.readouterr().out
    # fs names its entity container a "folder" — the preview says so.
    assert "would drop folder: d_u_0_sales" in out
    assert "would drop folder: d_u_0_inv" in out
    assert "d_u_0_" in out                      # the prefix we're looking for is shown
    assert "dry-run" in out
    # nothing deleted, and other users'/system containers never previewed.
    assert (fs_root / "d_u_0_sales").is_dir()
    assert "d_other_9_stale" not in out
    assert "config" not in out


def test_purge_force_drops_this_runs_keeps_others(
    stores, fs_root, capsys, monkeypatch  # noqa: F811
):
    monkeypatch.setenv("TDX_PARALLELISM", "1")  # deterministic inline drops
    _fs_with_entities(fs_root)
    capsys.readouterr()
    assert tdx._sys_purge(["--sys", "a", "-f"]) == 0
    assert not (fs_root / "d_u_0_sales").exists()
    assert not (fs_root / "d_u_0_inv").exists()
    assert (fs_root / "d_other_9_stale").is_dir()  # another user -> untouched
    assert (fs_root / "config").is_dir()           # not d_-prefixed -> untouched
    assert "purged 2/2 folder container(s) from 'a'" in capsys.readouterr().out


def test_purge_raw_prefix_targets_another_identity(stores, fs_root, monkeypatch):  # noqa: F811
    monkeypatch.setenv("TDX_PARALLELISM", "1")
    _fs_with_entities(fs_root)
    # A raw --prefix cleans another user's leftovers (include the d_ yourself).
    assert tdx._sys_purge(["--sys", "a", "--prefix", "d_other_", "-f"]) == 0
    assert not (fs_root / "d_other_9_stale").exists()
    assert (fs_root / "d_u_0_sales").is_dir()      # ours left alone this time


def test_purge_empty_prefix_matches_everything(stores, fs_root, monkeypatch):  # noqa: F811
    monkeypatch.setenv("TDX_PARALLELISM", "1")
    _fs_with_entities(fs_root)
    # `--prefix ""` is allowed and matches every container (including non-d_).
    assert tdx._sys_purge(["--sys", "a", "--prefix", "", "-f"]) == 0
    assert not (fs_root / "d_u_0_sales").exists()
    assert not (fs_root / "d_other_9_stale").exists()
    assert not (fs_root / "config").exists()


def test_purge_ignores_domain_descriptors(
    stores, fs_root, capsys, monkeypatch  # noqa: F811
):
    # The core promise: purge does NOT read the domain store. A stale folder with
    # no descriptor at all (a crashed run) is still found and dropped.
    monkeypatch.setenv("TDX_PARALLELISM", "1")
    tdx._sys_create(["--type", "fs", "--sys", "a", "--sys-path", str(fs_root), "-f"])
    (fs_root / "d_u_0_ghost").mkdir()
    (fs_root / "d_u_0_ghost" / "f.txt").write_text("x")
    assert not store.domain_path("d_u_0_ghost").exists()  # no descriptor
    capsys.readouterr()
    assert tdx._sys_purge(["--sys", "a", "-f"]) == 0
    assert not (fs_root / "d_u_0_ghost").exists()


def test_purge_force_runs_in_process_not_forked(stores, fs_root, monkeypatch):  # noqa: F811
    # Regression: the drop phase must run IN-PROCESS (threads), never in forked
    # workers. The parent opens a backend client to list entities first, and
    # fork-hostile native clients (grpc/boto3/snowflake/psycopg2/pyodbc) segfault
    # when used after fork() on macOS. A forked drop would run the spy in a child
    # process, so the parent's `seen` list would stay empty.
    import tabsdatax.itest.tdksystems.systems._cloud as cloud_mod

    tdx._sys_create(["--type", "fs", "--sys", "a", "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "one", "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "two", "-f"])
    parent = os.getpid()
    seen: list[int] = []
    orig = cloud_mod.CloudBase.purge_domain_entity

    def spy(self, ctx, name):
        seen.append(os.getpid())
        return orig(self, ctx, name)

    monkeypatch.setattr(cloud_mod.CloudBase, "purge_domain_entity", spy)
    assert tdx._sys_purge(["--sys", "a", "-f"]) == 0
    assert seen == [parent, parent]  # both drops ran in THIS process, not forked


def test_purge_auto_across_systems(stores, fs_root, capsys, monkeypatch):  # noqa: F811
    monkeypatch.setenv("TDX_PARALLELISM", "1")
    tdx._sys_create(["--type", "fs", "--sys", "one",
                     "--sys-path", str(fs_root / "one"), "-f"])
    tdx._sys_create(["--type", "fs", "--sys", "two",
                     "--sys-path", str(fs_root / "two"), "-f"])
    (fs_root / "one" / "d_u_0_x").mkdir(parents=True)
    (fs_root / "two" / "d_u_0_y").mkdir(parents=True)
    capsys.readouterr()
    assert tdx._sys_purge(["--auto", "ALL", "-f"]) == 0
    assert not (fs_root / "one" / "d_u_0_x").exists()
    assert not (fs_root / "two" / "d_u_0_y").exists()
    assert "across 2 system(s)" in capsys.readouterr().out


def test_purge_sys_rejects_multi(stores, fs_root):  # noqa: F811
    tdx._sys_create(["--type", "fs", "--sys", "a", "--sys-path", str(fs_root), "-f"])
    with pytest.raises(tdx.DatagenError, match="single system"):
        tdx._sys_purge(["--sys", "a,b"])


def test_purge_fs_lists_child_entities(stores, fs_root):  # noqa: F811
    # CloudBase.list_domain_entities enumerates the immediate child folders under
    # the staging root (SYS_PATH) — no domain descriptor involved.
    tdx._sys_create(["--type", "fs", "--sys", "a", "--sys-path", str(fs_root), "-f"])
    (fs_root / "x").mkdir()
    (fs_root / "y").mkdir()
    (fs_root / "z.txt").write_text("not a folder")
    doc = tdx.descriptor.load(store.system_path("a"))
    sysobj = tdx.systems.get("fs")
    ctx = SystemContext("fs", "a", tdx.descriptor.resolve(doc))
    assert sysobj.list_domain_entities(ctx) == ["x", "y"]


# ---- owner-aware backend branch (owned-by-login-user, not prefix) ----------

def test_purge_owner_backend_scopes_to_owned(stores, fs_root, monkeypatch):  # noqa: F811
    # Pretend fs is an owner-aware backend that owns only mine_1/mine_2. Default
    # purge (no --prefix) drops the owned containers and never the rest.
    monkeypatch.setenv("TDX_PARALLELISM", "1")
    tdx._sys_create(["--type", "fs", "--sys", "a", "--sys-path", str(fs_root), "-f"])
    for d in ("mine_1", "mine_2", "theirs"):
        (fs_root / d).mkdir()
    monkeypatch.setattr(FsSystem, "owns_entities", True)
    monkeypatch.setattr(
        FsSystem, "list_owned_domain_entities", lambda self, ctx: ["mine_1", "mine_2"]
    )
    assert tdx._sys_purge(["--sys", "a", "-f"]) == 0
    assert not (fs_root / "mine_1").exists() and not (fs_root / "mine_2").exists()
    assert (fs_root / "theirs").is_dir()  # not owned -> untouched


def test_purge_owner_backend_prefix_narrows(stores, fs_root, capsys, monkeypatch):  # noqa: F811
    monkeypatch.setenv("TDX_PARALLELISM", "1")
    tdx._sys_create(["--type", "fs", "--sys", "a", "--sys-path", str(fs_root), "-f"])
    for d in ("ci_1", "ci_2", "dev_1"):
        (fs_root / d).mkdir()
    monkeypatch.setattr(FsSystem, "owns_entities", True)
    monkeypatch.setattr(
        FsSystem, "list_owned_domain_entities",
        lambda self, ctx: ["ci_1", "ci_2", "dev_1"],
    )
    capsys.readouterr()
    # --prefix narrows WITHIN the owned set.
    assert tdx._sys_purge(["--sys", "a", "--prefix", "ci_", "-f"]) == 0
    assert not (fs_root / "ci_1").exists() and not (fs_root / "ci_2").exists()
    assert (fs_root / "dev_1").is_dir()
    assert "owned by the login user" in capsys.readouterr().out


def test_owner_backends_declared():
    # The warehouses expose a per-container owner; everything else does not.
    owners = {s for s in tdx.systems.all_slugs() if tdx.systems.get(s).owns_entities}
    assert owners == {"snowflake", "databricks", "bigquery"}


# ---- entity-container-type label + per-base delegation ---------------------

@pytest.mark.parametrize("slug,flag", [
    ("postgres", "schema"),
    ("mysql", "database"),
    ("mongodb", "database"),
    ("s3", "prefix"),
    ("fs", "folder"),
    ("bigquery", "dataset"),
    ("snowflake", "schema"),
    ("redpanda", "topic"),
    ("druid", "datasource"),
])
def test_domain_flag_names_the_container_type(slug, flag):
    # purge's preview labels each container by the system's `domain_flag`
    # ("would drop <flag>: <name>"), so the label source must be right.
    assert tdx.systems.get(slug).domain_flag == flag


def test_container_purge_delegates_to_do_list_and_do_delete(monkeypatch):
    my = tdx.systems.get("mysql")
    monkeypatch.setattr(type(my), "do_list", lambda self, admin: ["d_u_0_x", "d_u_0_y"])
    dropped: list[str] = []
    monkeypatch.setattr(
        type(my), "do_delete", lambda self, admin, name: dropped.append(name)
    )
    ctx = SystemContext(
        "mysql", "s",
        {"HOST": "h", "PORT": "3306", "USER": "root", "PASSWORD": "pw"},
    )
    assert my.list_domain_entities(ctx) == ["d_u_0_x", "d_u_0_y"]
    my.purge_domain_entity(ctx, "d_u_0_x")
    assert dropped == ["d_u_0_x"]


def test_saas_purge_noop_for_credentials_clone():
    # salesforce (domain_is_named = False) has no entity container to enumerate
    # or drop — list is empty and purge is a silent no-op (never NotImplemented).
    sf = tdx.systems.get("salesforce")
    ctx = SystemContext("salesforce", "s", {})
    assert sf.list_domain_entities(ctx) == []
    sf.purge_domain_entity(ctx, "anything")


def test_default_list_is_empty_and_purge_unsupported():
    dev = tdx.systems.get("dev")
    ctx = SystemContext("dev", "s", {})
    assert dev.list_domain_entities(ctx) == []
    with pytest.raises(Unsupported):
        dev.purge_domain_entity(ctx, "x")
