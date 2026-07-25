#
#  Copyright 2026 Tabsdata Inc.
#

"""Mocked-client harness for the ``tdk`` write / mutating commands.

The read counterpart (``test_render_harness.py``) pins the ``-o/--output``
contract; this harness pins the *mutation* contract every write command shares:

* it calls the right ``TabsdataServer`` SDK method with the arguments mapped
  from the CLI flags,
* it reports success, and
* on SDK failure it surfaces a ``click.ClickException`` (non-zero exit) rather
  than a raw traceback.

Delete commands (and the irreversible plan/trx cancel) add a fourth: they must
not touch the server unless the ``--confirm`` guard word is given.

Same two seams as the render harness: ``initialise_tabsdata_server_connection``
is patched to inject a ``MagicMock`` client into ``ctx.obj["tabsdataserver"]``
(``verify_login_or_prompt`` then passes because it is truthy), and
``DEFAULT_TABSDATA_DIRECTORY`` is pointed at a tmp dir. Every command is invoked
with all required flags so no interactive prompt fires.

Adding a command is one :class:`MutCase`: argv, the expected SDK method and its
mapped args/kwargs, the success substring, and whether it is a delete guard.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import MagicMock, call, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _client() -> MagicMock:
    # spec=TabsdataServer so the mock rejects calls to methods the real SDK does
    # not have -- catching CLI<->SDK drift (a renamed/removed SDK method fails
    # the test instead of silently passing on a permissive mock). ``connection``
    # is set on the instance in __init__ (not on the class, so not in the spec);
    # the top-level ``cli`` group sets ``server.connection.project`` on every
    # invoke, so provide it explicitly.
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    return client


def _invoke(argv: list[str], *, client: MagicMock, tmp_path):
    """Run ``td <argv>`` with ``client`` injected as the server connection."""

    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv)


# ---------------------------------------------------------------------------
# The command matrix
# ---------------------------------------------------------------------------


@dataclass
class MutCase:
    id: str
    argv: list[str]
    # SDK method the command must call on ctx.obj["tabsdataserver"], and the
    # exact positional/keyword arguments the CLI flags should map to.
    method: str
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    # A substring the success output must contain.
    success: str = ""
    # A delete guarded by ``--confirm delete`` (the argv already includes it).
    confirm: bool = False


CASES: tuple[MutCase, ...] = (
    # --- create -----------------------------------------------------------
    MutCase(
        "user-create",
        ["user", "create", "-n", "u1", "-p", "pw", "-f", "Full Name", "-e", "u1@x.io"],
        "create_user",
        ("u1", "pw"),
        {"full_name": "Full Name", "email": "u1@x.io", "enabled": True},
        "User created successfully",
    ),
    MutCase(
        "group-create",
        ["group", "create", "-n", "g1", "--description", "desc"],
        "create_group",
        ("g1", "desc"),
        {},
        "Group created successfully",
    ),
    MutCase(
        "project-create",
        ["project", "create", "-n", "p1", "--description", "desc"],
        "create_project",
        ("p1", "desc"),
        {},
        "Project created successfully",
    ),
    # --- update -----------------------------------------------------------
    MutCase(
        "user-update",
        ["user", "update", "-n", "u1", "-f", "New Name"],
        "update_user",
        ("u1",),
        {"full_name": "New Name", "email": None, "password": None, "enabled": None},
        "User updated successfully",
    ),
    MutCase(
        "group-update",
        ["group", "update", "-n", "g1", "--new-name", "g2", "--description", "d"],
        "update_group",
        ("g1",),
        {"new_name": "g2", "new_description": "d"},
        "Group updated successfully",
    ),
    MutCase(
        "project-update",
        ["project", "update", "-n", "p1", "--new-name", "p2", "--description", "d"],
        "update_project",
        ("p1",),
        {"new_name": "p2", "new_description": "d"},
        "Project updated successfully",
    ),
    MutCase(
        "collection-update",
        ["collection", "update", "-n", "c1", "--new-name", "c2",
         "--description", "d", "--storage-path", "bp"],
        "update_collection", ("c1",),
        {"new_name": "c2", "new_description": "d", "new_base_path": "bp"},
        "Collection updated successfully",
    ),
    # --- fn lifecycle -----------------------------------------------------
    MutCase(
        "fn-enable",
        ["fn", "enable", "-n", "f1", "-c", "coll1"],
        "enable_function",
        (),
        {"collection_name": "coll1", "function_name": "f1", "enabled": True},
        "Function updated successfully",
    ),
    MutCase(
        "fn-disable",
        ["fn", "disable", "-n", "f1", "-c", "coll1"],
        "enable_function",
        (),
        {"collection_name": "coll1", "function_name": "f1", "enabled": False},
        "Function updated successfully",
    ),
    MutCase(
        # -d/--detach so it doesn't monitor the (mock) execution in a poll loop.
        "fn-trigger",
        ["fn", "trigger", "-n", "f1", "-c", "coll1", "-d"],
        "trigger_function",
        ("coll1", "f1"),
        {"execution_name": None},
        "Function triggered",
    ),
    # --- permission (CLI level; previously only API-tested) ---------------
    MutCase(
        "permission-grant",
        ["permission", "grant", "-u", "alice", "--permission", "manage_system"],
        "grant_permission",
        ("alice", "manage_system", None),
        {},
        "Permission granted successfully",
    ),
    MutCase(
        "permission-revoke",
        ["permission", "revoke", "-u", "alice", "--permission", "manage_system"],
        "revoke_permission",
        ("alice", "manage_system", None),
        {},
        "Permission revoked successfully",
    ),
    # --- plan / trx / run / worker (non-delete writes) --------------------
    MutCase(
        "plan-cancel",
        ["plan", "cancel", "--plan", "P1", "--confirm", "cancel"],
        "cancel_execution",
        ("P1",),
        {},
        "Plan canceled successfully",
        confirm=True,
    ),
    MutCase(
        "trx-cancel",
        ["trx", "cancel", "--trx", "T1", "--confirm", "cancel"],
        "cancel_transaction",
        ("T1",),
        {},
        "Transaction canceled successfully",
        confirm=True,
    ),
    MutCase(
        "plan-recover",
        ["plan", "recover", "--plan", "P1"],
        "recover_execution",
        ("P1",),
        {},
        "Plan recovered successfully",
    ),
    MutCase(
        "trx-recover",
        ["trx", "recover", "--trx", "T1"],
        "recover_transaction",
        ("T1",),
        {},
        "Transaction recovered successfully",
    ),
    MutCase(
        "run-replay",
        ["run", "replay", "--run", "LFR1"],
        "replay_logical_function_run",
        ("LFR1",),
        {},
        "Replay scheduled",
    ),
    MutCase(
        "table-download",
        ["table", "download", "-c", "coll1", "--name", "t1", "--file", "out.parquet"],
        "download_table",
        ("coll1", "t1", "out.parquet"),
        {"at": None, "at_trx": None, "version": None},
        "Table downloaded successfully",
    ),
    # --- delete (confirm-guarded) -----------------------------------------
    MutCase(
        "user-delete",
        ["user", "delete", "-n", "u1", "--confirm", "delete"],
        "delete_user",
        ("u1",),
        {},
        "User deleted successfully",
        confirm=True,
    ),
    MutCase(
        "group-delete",
        ["group", "delete", "-n", "g1", "--confirm", "delete"],
        "delete_group",
        ("g1",),
        {},
        "Group deleted successfully",
        confirm=True,
    ),
    MutCase(
        "project-delete",
        ["project", "delete", "-n", "p1", "--confirm", "delete"],
        "delete_project",
        ("p1",),
        {},
        "Project deleted successfully",
        confirm=True,
    ),
    MutCase(
        "collection-delete",
        ["collection", "delete", "-n", "c1", "--confirm", "delete"],
        "delete_collection",
        ("c1",),
        {},
        "Collection deleted successfully",
        confirm=True,
    ),
    MutCase(
        "fn-delete",
        ["fn", "delete", "-n", "f1", "-c", "coll1", "--confirm", "delete"],
        "delete_function",
        ("coll1", "f1"),
        {},
        "Function deleted successfully",
        confirm=True,
    ),
    MutCase(
        "table-delete",
        ["table", "delete", "-n", "t1", "-c", "coll1", "--confirm", "delete"],
        "delete_table",
        ("coll1", "t1"),
        {},
        "Table deleted successfully",
        confirm=True,
    ),
)

_IDS = [c.id for c in CASES]
_DELETE_IDS = [c.id for c in CASES if c.confirm]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", CASES, ids=_IDS)
def test_mutation_calls_sdk_and_reports_success(case: MutCase, tmp_path):
    client = _client()
    result = _invoke(case.argv, client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    method = getattr(client, case.method)
    assert method.call_count == 1, f"{case.method} called {method.call_count}x"
    assert method.call_args == call(*case.args, **case.kwargs), method.call_args
    assert case.success in result.output


@pytest.mark.parametrize("case", CASES, ids=_IDS)
def test_mutation_surfaces_sdk_error_as_clickexception(case: MutCase, tmp_path):
    client = _client()
    getattr(client, case.method).side_effect = RuntimeError("boom-xyz")
    result = _invoke(case.argv, client=client, tmp_path=tmp_path)
    # A failed mutation must be a clean CLI error, not a crash/traceback.
    assert result.exit_code != 0
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "boom-xyz" in result.output


@pytest.mark.parametrize("case", [c for c in CASES if c.confirm], ids=_DELETE_IDS)
def test_delete_requires_confirmation(case: MutCase, tmp_path):
    # Same argv but the guard word is wrong -> the server must not be touched.
    argv = list(case.argv)
    argv[argv.index("--confirm") + 1] = "not-the-word"
    client = _client()
    result = _invoke(argv, client=client, tmp_path=tmp_path)
    assert result.exit_code != 0
    assert getattr(client, case.method).call_count == 0
    assert "not confirmed" in result.output.lower()


# ---------------------------------------------------------------------------
# plan / trx / run / worker id-keyed commands: the id option is required, and
# the usage error fires before (or without) any server call.
# ---------------------------------------------------------------------------

_ID_KEYED_METHODS = (
    "cancel_execution",
    "cancel_transaction",
    "recover_execution",
    "recover_transaction",
    "get_execution",
    "get_transaction",
    "get_function_run",
    "get_worker",
    "get_worker_log",
)

_ID_KEYED_ARGVS = (
    ["plan", "get"],
    ["plan", "graph"],
    ["plan", "cancel"],
    ["plan", "recover"],
    ["plan", "monitor"],
    ["plan", "logs"],
    ["trx", "get"],
    ["trx", "cancel"],
    ["trx", "recover"],
    ["trx", "monitor"],
    ["trx", "logs"],
    ["run", "get"],
    ["run", "logs"],
    ["run", "recover"],
    ["worker", "logs"],
    ["worker", "recover"],
)


@pytest.mark.parametrize(
    "argv", _ID_KEYED_ARGVS, ids=["-".join(a) for a in _ID_KEYED_ARGVS]
)
def test_id_keyed_commands_require_their_id(argv, tmp_path):
    client = _client()
    result = _invoke(argv, client=client, tmp_path=tmp_path)
    assert result.exit_code != 0
    for method in _ID_KEYED_METHODS:
        assert getattr(client, method).call_count == 0, method


# ---------------------------------------------------------------------------
# run/worker recover have no server-side endpoint of their own: the CLI
# resolves the owning transaction and recovers it, saying so.
# ---------------------------------------------------------------------------


def test_run_recover_resolves_owning_transaction(tmp_path):
    client = _client()
    client.get_function_run.return_value.transaction.id = "T9"
    result = _invoke(
        ["run", "recover", "--run", "R1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert client.get_function_run.call_args == call("R1")
    assert client.recover_transaction.call_args == call("T9")
    assert "Recovering transaction 'T9' of function run 'R1'" in result.output


def test_worker_recover_resolves_owning_transaction(tmp_path):
    client = _client()
    client.get_worker.return_value.transaction.id = "T9"
    result = _invoke(
        ["worker", "recover", "--worker", "W1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert client.get_worker.call_args == call("W1")
    assert client.recover_transaction.call_args == call("T9")
    assert "Recovering transaction 'T9' of worker 'W1'" in result.output


# ---------------------------------------------------------------------------
# fn register / update / patch: these first materialise the user's function via
# ``load_info_and_project_root`` (a dynamic import of ``file.py::name``) and then
# call the SDK. We patch that resolver to a sentinel ``(info, project_root)`` so
# the test pins only the CLI-flag -> SDK-argument mapping, not bundle building.
# ---------------------------------------------------------------------------


class _FnInfo:
    """Stand-in for a ``*Info`` descriptor: exposes ``.func`` so the register
    ``--update`` path can derive the function name from ``info.func.__name__``."""

    def __init__(self):
        def f():
            pass

        self.func = f


_INFO = _FnInfo()
_ROOT = "/proj/root"


def _invoke_fn(argv: list[str], *, client: MagicMock, tmp_path):
    with patch(
        "tabsdatak.cli.td.fn_group.load_info_and_project_root",
        return_value=(_INFO, _ROOT),
    ):
        return _invoke(argv, client=client, tmp_path=tmp_path)


def test_fn_register_maps_flags_to_register_function(tmp_path):
    client = _client()
    result = _invoke_fn(
        [
            "fn", "register", "-c", "coll1", "--path", "/x.py::f",
            "--description", "desc", "--reuse-tables",
            "--storage-path", "bp",
        ],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    # --requirements and --local-pkg are temporarily disabled: the command
    # always forwards them as None.
    assert client.register_function.call_args == call(
        "coll1", _INFO, _ROOT,
        description="desc", local_packages=None, reuse_tables=True,
        enabled=True, requirements=None, base_path="bp",
    )
    assert "registered successfully" in result.output


def test_fn_update_maps_flags_to_update_function(tmp_path):
    client = _client()
    result = _invoke_fn(
        [
            "fn",
            "update",
            "-n",
            "f1",
            "-c",
            "coll1",
            "--path",
            "/x.py::f",
            "--description",
            "d",
            "--reset-state",
            "--reset-stage",
        ],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert client.update_function.call_args == call(
        "coll1",
        "f1",
        _INFO,
        _ROOT,
        description="d",
        local_packages=None,
        reuse_tables=False,
        enabled=True,
        requirements=None,
        base_path=None,
        reset_state=True,
        reset_stage=True,
    )
    assert "updated successfully" in result.output


def test_fn_patch_maps_flags_to_patch_function(tmp_path):
    client = _client()
    result = _invoke_fn(
        [
            "fn", "patch", "-n", "f1", "-c", "coll1", "--fn-version", "fv1",
            "--path", "/x.py::f", "--description", "d",
        ],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert client.patch_function.call_args == call(
        "coll1",
        "f1",
        _INFO,
        _ROOT,
        patch_of="fv1",
        description="d",
        local_packages=None,
        requirements=None,
    )
    assert "Patch registered successfully" in result.output


def test_fn_register_update_existing_updates_instead_of_registering(tmp_path):
    # --update + the function already exists (get_function succeeds) -> the
    # command updates rather than registers.
    client = _client()
    result = _invoke_fn(
        ["fn", "register", "-c", "coll1", "--path", "/x.py::f", "--update"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert client.update_function.call_count == 1
    assert client.register_function.call_count == 0


def test_fn_register_update_missing_falls_through_to_register(tmp_path):
    # --update but the function does not exist (get_function raises) -> the
    # command falls through to the normal register flow.
    from tabsdatak.sdk._apiserver import APIServerError

    client = _client()
    client.get_function.side_effect = APIServerError("does not exist")
    result = _invoke_fn(
        ["fn", "register", "-c", "coll1", "--path", "/x.py::f", "--update"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert client.register_function.call_count == 1
    assert client.update_function.call_count == 0
