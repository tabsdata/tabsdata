#
#  Copyright 2026 Tabsdata Inc.
#

"""Mocked-client harness for the ``tdk`` write / mutating commands.

The read counterpart (``test_render_harness.py``) pins the ``-o/--format``
contract; this harness pins the *mutation* contract every write command shares:

* it calls the right ``TabsdataServer`` SDK method with the arguments mapped
  from the CLI flags,
* it reports success,
* in a machine run it writes exactly one JSON document to stdout: the resulting
  DTO, a ``{"deleted": ...}`` or a ``{"done": ...}`` envelope, and
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
mapped args/kwargs, the success substring, whether it is a delete guard, the
machine-run envelope and the DTO the mocked SDK hands back for it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from unittest.mock import ANY, MagicMock, call, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import build_full_help, cli
from tabsdatak.sdk.tabsdata_server import ServerStatus, TabsdataServer


def _client() -> MagicMock:
    # spec=TabsdataServer so the mock rejects calls to methods the real SDK does
    # not have -- catching CLI<->SDK drift (a renamed/removed SDK method fails
    # the test instead of silently passing on a permissive mock). ``connection``
    # is set on the instance in __init__ (not on the class, so not in the spec);
    # the top-level ``cli`` group sets ``server.connection.project`` on every
    # invoke, so provide it explicitly.
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "p1"
    # `fn enable|disable` skips the call when the function is already in the
    # requested state; ``None`` matches neither, so the toggle always goes
    # through here. The already-in-state no-op is covered in
    # test_enable_disable.py.
    client.get_function.return_value = MagicMock(enabled=None)
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
    # The single JSON document the command writes to stdout in a machine run.
    envelope: dict = field(default_factory=dict)
    # What the mocked SDK returns: ``dto`` becomes ``<dto_method>()._data``, or
    # the return value itself when ``raw`` (SDK methods that return a dict).
    dto: dict | None = None
    dto_method: str | None = None
    raw: bool = False


def _done(action, kind, id, **details):
    return {"done": {"action": action, "kind": kind, "id": id, **details}}


def _deleted(kind, name, collection=None, project=None):
    return {
        "deleted": {
            "kind": kind,
            "name": name,
            "collection": collection,
            "project": project,
        }
    }


def _configure(client: MagicMock, case: "MutCase") -> None:
    if case.dto is None:
        return
    method = getattr(client, case.dto_method or case.method)
    if case.raw:
        method.return_value = case.dto
    else:
        method.return_value._data = case.dto


_REPLAY = {
    "logical_function_run_id": "fr20260930-2",
    "previous_function_run_id": "FR1",
    "new_function_run_id": "FR2",
    "function_version_id": "FV1",
}

CASES: tuple[MutCase, ...] = (
    # --- create -----------------------------------------------------------
    MutCase(
        "user-create",
        ["user", "create", "-n", "u1", "-p", "pw", "-f", "Full Name", "-e", "u1@x.io"],
        "create_user",
        ("u1", "pw"),
        {"full_name": "Full Name", "email": "u1@x.io", "enabled": True},
        "User created successfully",
        envelope={"name": "u1"},
        dto={"name": "u1"},
    ),
    MutCase(
        "group-create",
        ["group", "create", "-n", "g1", "--description", "desc"],
        "create_group",
        ("g1", "desc"),
        {},
        "Group created successfully",
        envelope={"name": "g1"},
        dto={"name": "g1"},
    ),
    MutCase(
        "project-create",
        ["project", "create", "-n", "p1", "--description", "desc"],
        "create_project",
        ("p1", "desc"),
        {"secrets": None},
        "Project created successfully",
        envelope={"name": "p1"},
        dto={"name": "p1"},
    ),
    # --- update -----------------------------------------------------------
    MutCase(
        "user-update",
        ["user", "update", "-n", "u1", "-f", "New Name"],
        "update_user",
        ("u1",),
        {"full_name": "New Name", "email": None, "password": None, "enabled": None},
        "User updated successfully",
        envelope={"name": "u1"},
        dto={"name": "u1"},
    ),
    MutCase(
        "group-update",
        ["group", "update", "-n", "g1", "--new-name", "g2", "--description", "d"],
        "update_group",
        ("g1",),
        {"new_name": "g2", "new_description": "d"},
        "Group updated successfully",
        envelope={"name": "g2"},
        dto={"name": "g2"},
    ),
    MutCase(
        "project-update",
        ["project", "update", "-n", "p1", "--new-name", "p2", "--description", "d"],
        "update_project",
        ("p1",),
        {"new_name": "p2", "new_description": "d", "new_secrets": None},
        "Project updated successfully",
        envelope={"name": "p2"},
        dto={"name": "p2"},
    ),
    MutCase(
        "collection-update",
        [
            "collection",
            "update",
            "-n",
            "c1",
            "--new-name",
            "c2",
            "--description",
            "d",
            "--storage-path",
            "bp",
        ],
        "update_collection",
        ("c1",),
        {"new_name": "c2", "new_description": "d", "new_base_path": "bp"},
        "Collection updated successfully",
        envelope={"name": "c2"},
        dto={"name": "c2"},
        dto_method="get_collection",
    ),
    # --- fn lifecycle -----------------------------------------------------
    MutCase(
        "fn-enable",
        ["fn", "enable", "-n", "f1", "-c", "coll1"],
        "enable_function",
        (),
        {"collection_name": "coll1", "function_name": "f1", "enabled": True},
        "Function updated successfully",
        envelope=_done("enable", "function", "f1", collection="coll1", changed=True),
    ),
    MutCase(
        "fn-disable",
        ["fn", "disable", "-n", "f1", "-c", "coll1"],
        "enable_function",
        (),
        {"collection_name": "coll1", "function_name": "f1", "enabled": False},
        "Function updated successfully",
        envelope=_done("disable", "function", "f1", collection="coll1", changed=True),
    ),
    # `cron`/`stream` enable/disable make the same call, but only for a function
    # that has the matching trigger -- they need a wired listing for that
    # precondition, so they live in test_trigger_group.py.
    MutCase(
        "fn-trigger",
        ["fn", "trigger", "-n", "f1", "-c", "coll1"],
        "trigger_function",
        ("coll1", "f1"),
        {"execution_name": None},
        "Function triggered",
        envelope={
            "id": "ep20260930-1",
            "status": "S",
            "status_name": "Scheduled",
            "health": "running",
            "final": False,
        },
        dto={"id": "ep20260930-1", "status": "S"},
    ),
    # --- permission (CLI level; previously only API-tested) ---------------
    MutCase(
        "permission-grant",
        ["permission", "grant", "-u", "alice", "--permission", "manage_system"],
        "grant_permission",
        ("alice", "manage_system", None),
        {},
        "Permission granted successfully",
        envelope=_done(
            "grant",
            "permission",
            "manage_system",
            user="alice",
            project=None,
        ),
    ),
    MutCase(
        "permission-revoke",
        ["permission", "revoke", "-u", "alice", "--permission", "manage_system"],
        "revoke_permission",
        ("alice", "manage_system", None),
        {},
        "Permission revoked successfully",
        envelope=_done(
            "revoke",
            "permission",
            "manage_system",
            user="alice",
            project=None,
        ),
    ),
    # --- plan / trx / run / worker (non-delete writes) --------------------
    MutCase(
        "plan-cancel",
        ["plan", "cancel", "--plan", "ep20260930-1", "--confirm", "cancel"],
        "cancel_execution",
        ("ep20260930-1",),
        {},
        "Plan canceled successfully",
        confirm=True,
        envelope=_done("cancel", "plan", "ep20260930-1", changed=True),
    ),
    MutCase(
        "trx-cancel",
        ["trx", "cancel", "--trx", "tx20260930-1", "--confirm", "cancel"],
        "cancel_transaction",
        ("tx20260930-1",),
        {},
        "Transaction canceled successfully",
        confirm=True,
        envelope=_done("cancel", "transaction", "tx20260930-1", changed=True),
    ),
    MutCase(
        "plan-recover",
        ["plan", "recover", "--plan", "ep20260930-1"],
        "recover_execution",
        ("ep20260930-1",),
        {},
        "Plan recovered successfully",
        envelope=_done("recover", "plan", "ep20260930-1", changed=True),
    ),
    MutCase(
        "trx-recover",
        ["trx", "recover", "--trx", "tx20260930-1"],
        "recover_transaction",
        ("tx20260930-1",),
        {},
        "Transaction recovered successfully",
        envelope=_done("recover", "transaction", "tx20260930-1", changed=True),
    ),
    MutCase(
        "fnrun-replay",
        ["fnrun", "replay", "--fnrun", "fr20260930-2"],
        "replay_logical_function_run",
        ("fr20260930-2",),
        {},
        "Replay scheduled",
        envelope=_REPLAY,
        dto=_REPLAY,
        raw=True,
    ),
    MutCase(
        "table-download",
        ["table", "download", "-c", "coll1", "--name", "t1", "--file", "out.parquet"],
        "download_table",
        ("coll1", "t1", "out.parquet"),
        {"at": None, "at_trx": None, "version": None, "max_bytes": ANY},
        "Table downloaded successfully",
        envelope=_done(
            "download",
            "table",
            "t1",
            collection="coll1",
            file="out.parquet",
        ),
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
        envelope=_deleted("user", "u1"),
    ),
    MutCase(
        "group-delete",
        ["group", "delete", "-n", "g1", "--confirm", "delete"],
        "delete_group",
        ("g1",),
        {},
        "Group deleted successfully",
        confirm=True,
        envelope=_deleted("group", "g1", project="p1"),
    ),
    MutCase(
        "project-delete",
        ["project", "delete", "-n", "p1", "--confirm", "delete"],
        "delete_project",
        ("p1",),
        {},
        "Project deleted successfully",
        confirm=True,
        envelope=_deleted("project", "p1"),
    ),
    MutCase(
        "collection-delete",
        ["collection", "delete", "-n", "c1", "--confirm", "delete"],
        "delete_collection",
        ("c1",),
        {},
        "Collection deleted successfully",
        confirm=True,
        envelope=_deleted("collection", "c1", project="p1"),
    ),
    MutCase(
        "fn-delete",
        ["fn", "delete", "-n", "f1", "-c", "coll1", "--confirm", "delete"],
        "delete_function",
        ("coll1", "f1"),
        {},
        "Function deleted successfully",
        confirm=True,
        envelope=_deleted("function", "f1", collection="coll1", project="p1"),
    ),
    MutCase(
        "table-delete",
        ["table", "delete", "-n", "t1", "-c", "coll1", "--confirm", "delete"],
        "delete_table",
        ("coll1", "t1"),
        {},
        "Table deleted successfully",
        confirm=True,
        envelope=_deleted("table", "t1", collection="coll1", project="p1"),
    ),
    MutCase(
        "collection-tag-set",
        ["collection", "tag", "set", "-n", "c1", "--key", "k", "--value", "v"],
        "set_tag",
        ("k", "v"),
        {"collection": "c1"},
        "Tag 'k' set",
        envelope={"key": "k", "value": "v", "kind": "U"},
        dto={"key": "k", "value": "v", "kind": "U"},
        raw=True,
    ),
    MutCase(
        "collection-tag-delete",
        ["collection", "tag", "delete", "-n", "c1", "--key", "k"],
        "delete_tag",
        ("k",),
        {"collection": "c1"},
        "Tag 'k' removed",
        envelope=_done("delete", "tag", "k", collection="c1"),
    ),
)

_IDS = [c.id for c in CASES]
_DELETE_IDS = [c.id for c in CASES if c.confirm]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _document(result) -> dict:
    lines = result.stdout.splitlines()
    assert len(lines) == 1, result.stdout
    return json.loads(lines[0])


@pytest.mark.parametrize("case", CASES, ids=_IDS)
def test_mutation_calls_sdk_and_reports_success(case: MutCase, tmp_path):
    client = _client()
    _configure(client, case)
    result = _invoke(case.argv, client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    method = getattr(client, case.method)
    assert method.call_count == 1, f"{case.method} called {method.call_count}x"
    assert method.call_args == call(*case.args, **case.kwargs), method.call_args
    assert case.success in result.stdout


@pytest.mark.parametrize("case", CASES, ids=_IDS)
def test_mutation_emits_one_envelope_in_a_machine_run(case: MutCase, tmp_path):
    client = _client()
    _configure(client, case)
    result = _invoke([*case.argv, "--machine"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert _document(result) == case.envelope
    assert getattr(client, case.method).call_args == call(*case.args, **case.kwargs)


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
    assert result.exit_code == 2
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
    ["plan", "cancel"],
    ["plan", "recover"],
    ["plan", "monitor"],
    ["plan", "logs"],
    ["trx", "get"],
    ["trx", "cancel"],
    ["trx", "recover"],
    ["trx", "monitor"],
    ["trx", "logs"],
    ["fnrun", "get"],
    ["fnrun", "logs"],
    ["fnrun", "recover"],
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
        ["fnrun", "recover", "--fnrun", "fr20260930-1"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert client.get_function_run.call_args == call("fr20260930-1")
    assert client.recover_transaction.call_args == call("T9")
    assert "Recovering transaction 'T9' of function run 'fr20260930-1'" in result.output


def test_worker_recover_resolves_owning_transaction(tmp_path):
    client = _client()
    client.get_worker.return_value.transaction.id = "T9"
    result = _invoke(
        ["worker", "recover", "--worker", "wkr20260930-1"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert client.get_worker.call_args == call("wkr20260930-1")
    assert client.recover_transaction.call_args == call("T9")
    assert "Recovering transaction 'T9' of worker 'wkr20260930-1'" in result.output


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
            "fn",
            "register",
            "-c",
            "coll1",
            "--path",
            "/x.py::f",
            "--description",
            "desc",
            "--reuse-tables",
            "--storage-path",
            "bp",
        ],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert client.register_function.call_args == call(
        "coll1",
        _INFO,
        _ROOT,
        description="desc",
        local_packages=None,
        reuse_tables=True,
        enabled=True,
        requirements=None,
        confine_to=None,
        base_path="bp",
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
        confine_to=None,
        base_path=None,
        reset_state=True,
        reset_stage=True,
    )
    assert "updated successfully" in result.output


def test_fn_patch_maps_flags_to_patch_function(tmp_path):
    client = _client()
    result = _invoke_fn(
        [
            "fn",
            "patch",
            "-n",
            "f1",
            "-c",
            "coll1",
            "--fn-version",
            "fv1",
            "--path",
            "/x.py::f",
            "--description",
            "d",
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
        confine_to=None,
    )
    assert "Patch registered successfully" in result.output


class _Destination:
    def _materialized(self) -> dict[str, str]:
        return {
            "if_table_exists": "replace",
            "schema_evolution": "update",
            "schema_evolution_engine": "iceberg",
        }


class _SubscriberInfo(_FnInfo):
    def __init__(self):
        from tabsdatak._info import KIND_SUBSCRIBER_DEF

        super().__init__()
        self.__kind__ = KIND_SUBSCRIBER_DEF
        self.destination = _Destination()


_WRITES = (
    "Destination writes with if_table_exists='replace', schema_evolution='update',"
    " schema_evolution_engine='iceberg'"
)


@pytest.mark.parametrize(
    "argv",
    [
        ["fn", "register", "-c", "coll1", "--path", "/x.py::f"],
        ["fn", "register", "--update", "-c", "coll1", "--path", "/x.py::f"],
        ["fn", "update", "-n", "f1", "-c", "coll1", "--path", "/x.py::f"],
        [
            "fn",
            "patch",
            "-n",
            "f1",
            "-c",
            "coll1",
            "--fn-version",
            "fv1",
            "--path",
            "/x.py::f",
        ],
    ],
    ids=["register", "register-update", "update", "patch"],
)
def test_fn_registration_reports_what_the_destination_writes_with(argv, tmp_path):
    with patch(
        "tabsdatak.cli.td.fn_group.load_info_and_project_root",
        return_value=(_SubscriberInfo(), _ROOT),
    ):
        result = _invoke(argv, client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert _WRITES in result.output


def test_fn_registration_reports_nothing_for_a_function_without_a_destination(
    tmp_path,
):
    result = _invoke_fn(
        ["fn", "register", "-c", "coll1", "--path", "/x.py::f"],
        client=_client(),
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert "Destination writes with" not in result.output


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


def test_fn_register_update_that_fails_does_not_register(tmp_path):
    from tabsdatak.sdk._apiserver import APIServerError

    client = _client()
    client.update_function.side_effect = APIServerError("update refused")
    result = _invoke_fn(
        ["fn", "register", "-c", "coll1", "--path", "/x.py::f", "--update"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code != 0
    assert client.register_function.call_count == 0


# ---------------------------------------------------------------------------
# Machine runs: every leaf reaches a JSON result. A leaf without ``-o/--format``
# renders its human view only, so in a machine run it would end in
# ``tdk::no_result``; ``shell`` and ``auth password-change`` are interactive only.
# Commands a variant contributes are its own to check.
# ---------------------------------------------------------------------------

_INTERACTIVE_ONLY = {"tdk shell", "tdk auth password-change"}
_BUILT_IN = {
    name
    for name, command in cli.commands.items()
    if command.callback is not None
    and command.callback.__module__.startswith("tabsdatak.cli.")
}


def _leaves(node: dict):
    if node.get("commands"):
        for sub in node["commands"]:
            yield from _leaves(sub)
        return
    yield node


def test_every_leaf_command_can_render_a_json_result():
    tree = json.loads(build_full_help(cli, "json"))
    missing = [
        leaf["path"]
        for leaf in _leaves(tree)
        if leaf["path"].split()[1] in _BUILT_IN
        and leaf["path"] not in _INTERACTIVE_ONLY
        and not any("--format" in option["opts"] for option in leaf["options"])
    ]
    assert missing == []


def _machine(argv, client, tmp_path) -> dict:
    result = _invoke([*argv, "--machine"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    return _document(result)


def test_collection_create_emits_the_collection(tmp_path):
    client = _client()
    client.create_collection.return_value._data = {"name": "c1"}
    argv = ["collection", "create", "-n", "c1", "--description", "d"]
    assert _machine(argv, client, tmp_path) == {"name": "c1"}


def test_connection_get_definition_emits_the_catalog_and_definition(tmp_path):
    client = _client()
    client.get_connection_catalog.return_value = {"collection": "c1"}
    client.get_collection_connection.return_value = {
        "connection": {"type": "x"},
        "secret_names": ["S1"],
    }
    argv = ["connection", "get", "-n", "c1", "--definition"]
    document = _machine(argv, client, tmp_path)
    assert document == {"collection": "c1", "definition": {"connection": {"type": "x"}}}


def test_fn_enable_already_enabled_is_an_unchanged_done(tmp_path):
    client = _client()
    client.get_function.return_value = MagicMock(enabled=True)
    document = _machine(["fn", "enable", "-n", "f1", "-c", "coll1"], client, tmp_path)
    assert document == _done(
        "enable", "function", "f1", collection="coll1", changed=False
    )
    assert client.enable_function.call_count == 0


@pytest.mark.parametrize("group", ["cron", "stream"])
@pytest.mark.parametrize("action", ["enable", "disable"])
def test_trigger_enable_disable_emit_done(group, action, tmp_path):
    client = _client()
    client.list_workers_generator.side_effect = lambda *a, **k: iter([MagicMock()])
    document = _machine([group, action, "-n", "f1", "-c", "coll1"], client, tmp_path)
    assert document == _done(action, "function", "f1", collection="coll1", changed=True)


def test_fn_bundle_emits_done_with_the_file(tmp_path):
    client = _client()
    argv = ["fn", "bundle", "--fn-version", "FV1", "-f", "b.tar.zst"]
    assert _machine(argv, client, tmp_path) == _done(
        "download", "bundle", "FV1", file="b.tar.zst"
    )
    assert client.download_function_bundle.call_args == call("FV1", "b.tar.zst")


def test_run_recover_emits_done_for_the_owning_transaction(tmp_path):
    client = _client()
    client.get_function_run.return_value.transaction.id = "T9"
    document = _machine(
        ["fnrun", "recover", "--fnrun", "fr20260930-1"], client, tmp_path
    )
    assert document == _done(
        "recover", "transaction", "T9", function_run="fr20260930-1", changed=True
    )


def test_worker_recover_emits_done_for_the_owning_transaction(tmp_path):
    client = _client()
    client.get_worker.return_value.transaction.id = "T9"
    document = _machine(
        ["worker", "recover", "--worker", "wkr20260930-1"], client, tmp_path
    )
    assert document == _done(
        "recover", "transaction", "T9", worker="wkr20260930-1", changed=True
    )


def test_a_cancel_or_recover_that_changes_nothing_says_so(tmp_path):
    client = _client()
    no_op = {
        "context": {"warnings": [{"code": "UpdateStatusRunError::0004"}]},
        "data": None,
    }
    client.cancel_execution.return_value.json.return_value = no_op
    client.recover_transaction.return_value.json.return_value = no_op
    document = _machine(
        ["plan", "cancel", "--plan", "ep20260930-1", "--confirm", "cancel"],
        client,
        tmp_path,
    )
    assert document == _done(
        "cancel",
        "plan",
        "ep20260930-1",
        changed=False,
        message=(
            "Nothing to cancel: every run of plan 'ep20260930-1' had already ended."
        ),
    )
    human = _invoke(
        ["trx", "recover", "--trx", "tx20260930-1", "--wait"],
        client=client,
        tmp_path=tmp_path,
    )
    assert human.exit_code == 0, human.output
    assert "Nothing to recover: no run of transaction 'tx20260930-1'" in human.output
    client.get_transaction.assert_not_called()


def test_cert_add_and_delete_emit_done(tmp_path):
    cert = tmp_path / "server.pem"
    source = tmp_path / "src.pem"
    source.write_text("PEM")
    server = "https://host:2457"
    with (
        patch(
            "tabsdatak.sdk._apiserver._obtain_certificate_file_path",
            return_value=str(cert),
        ),
        patch(
            "tabsdatak.sdk._apiserver.DEFAULT_TABSDATA_CERTIFICATE_FOLDER",
            str(tmp_path),
        ),
    ):
        added = _machine(
            ["auth", "add-cert", "-s", server, "--pem", str(source)],
            _client(),
            tmp_path,
        )
        deleted = _machine(["auth", "delete-cert", "-s", server], _client(), tmp_path)
        missing = _machine(["auth", "delete-cert", "-s", server], _client(), tmp_path)
    assert added == _done("add", "certificate", server, path=str(cert))
    assert deleted == _done(
        "delete", "certificate", server, path=str(cert), changed=True
    )
    assert missing == _done(
        "delete", "certificate", server, path=str(cert), changed=False
    )


def test_status_emits_the_server_status(tmp_path):
    client = _client()
    client.status = ServerStatus("Running", 1500, version="1.0")
    assert _machine(["status"], client, tmp_path) == {
        "status": "Running",
        "latency_as_nanos": 1500,
        "version": "1.0",
    }


def test_about_emits_the_captured_build_metadata(tmp_path):
    with patch(
        "tabsdatak.common._information.about.tdabout", return_value="build info\n"
    ) as about:
        document = _machine(["about"], _client(), tmp_path)
    assert document["text"] == "build info\n"
    assert about.call_args == call(capture=True)


@pytest.mark.parametrize(
    "command, title",
    [
        ("license", "License"),
        ("release-notes", "Release Notes"),
        ("third-party", "Third Party Dependencies"),
    ],
)
def test_info_emits_the_manifest_text(command, title, tmp_path):
    document = _machine(["info", command], _client(), tmp_path)
    assert document["title"] == title
    assert isinstance(document["text"], str)


def test_login_says_login_successful_once(tmp_path):
    with patch("tabsdatak.cli.td.cli.utils_login"):
        result = _invoke(
            ["login", "-s", "http://h:2457", "-u", "admin", "-p", "pw"],
            client=_client(),
            tmp_path=tmp_path,
        )
    assert result.exit_code == 0, result.output
    assert result.stdout.count("Login successful.") == 1


def test_set_and_unset_emit_the_resulting_settings(tmp_path):
    after_set = _machine(["set", "--hints", "false"], _client(), tmp_path)
    after_unset = _machine(["unset", "--hints"], _client(), tmp_path)
    assert after_set["hints"] == "disabled"
    assert after_unset["hints"] == "enabled"
    assert set(after_set) == {
        "profile",
        "hints",
        "prompt",
        "trigger_wait",
        "server",
        "pins",
    }


def test_set_json_keeps_its_messages_off_stdout(tmp_path):
    result = _invoke(
        ["set", "--hints", "false", "-o", "json"], client=_client(), tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["hints"] == "disabled"
    assert "Hints disabled" in result.stderr


def test_examples_extract_emits_done(tmp_path):
    root = tmp_path / "examples"
    (root / "demo" / "functions").mkdir(parents=True)
    (root / "demo" / "functions" / "f.py").write_text("")
    target = tmp_path / "out"
    with patch("tabsdatak.cli.td.examples_group.examples_root", return_value=root):
        document = _machine(
            ["examples", "extract", "-n", "demo", "-d", str(target)],
            _client(),
            tmp_path,
        )
    assert document == _done("extract", "example", "demo", dir=str(target))
    assert (target / "f.py").exists()


def test_fn_register_emits_the_function(tmp_path):
    client = _client()
    client.register_function.return_value._data = {"name": "f"}
    result = _invoke_fn(
        ["fn", "register", "-c", "coll1", "--path", "/x.py::f", "--machine"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert _document(result) == {"name": "f"}


def test_fn_patch_emits_the_patch_record(tmp_path):
    client = _client()
    record = {"id": "PV1", "parent_function_version_id": "fv1"}
    client.patch_function.return_value = record
    argv = ["fn", "patch", "-n", "f1", "-c", "coll1", "--fn-version", "fv1"]
    result = _invoke_fn(
        [*argv, "--path", "/x.py::f", "--machine"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert _document(result) == record
    human = _invoke_fn([*argv, "--path", "/x.py::f"], client=client, tmp_path=tmp_path)
    assert "Patch registered successfully\nPatch id: PV1\nPatches:  fv1\n" in (
        human.stdout
    )
