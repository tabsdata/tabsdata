#
#  Copyright 2026 Tabsdata Inc.
#

"""Mocked-client renderer harness for the ``tdk`` read commands.

Every ``tdk`` read command (``list`` / ``get``) shares one output contract:
``-o/--output {table,json,yaml}`` dispatched through
:func:`tabsdatak.cli.cli_utils.render`. This harness runs each such command
with a **mocked** ``TabsdataServer`` injected into ``ctx.obj`` and asserts, per
format, that:

* ``json``  -> stdout is valid JSON equal to the DTO payload (the ``_data``
  dicts the command hands to ``render``),
* ``yaml``  -> stdout is valid YAML equal to that same payload, and
* ``table`` -> the human-facing rich view renders (exit 0) and shows every
  expected value -- we assert the *content*, never the format (borders,
  alignment, column order, line wrapping), which is the renderer's business.

Two seams are patched so nothing touches a real server or the user's
``~/.tabsdata``:

* ``tabsdatak.cli.td.cli.initialise_tabsdata_server_connection`` -- replaced
  with an injector that drops the mock client into
  ``ctx.obj["tabsdataserver"]`` (the top-level group calls it right after it
  builds ``ctx.obj``; ``verify_login_or_prompt`` then passes because the client
  is truthy), and
* ``tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY`` -- pointed at a tmp dir.

Adding a command to the matrix is one :class:`RenderCase` entry: the argv, a
callback wiring the mock client, the expected machine payload, and a few
substrings the table view must contain.
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from click.testing import CliRunner
from ruamel.yaml import YAML

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer

# ---------------------------------------------------------------------------
# DTO / client stand-ins
# ---------------------------------------------------------------------------


class _DTO:
    """Minimal SDK-DTO stand-in.

    ``_data`` is the raw payload the machine formats serialize; every key is
    also exposed as an attribute, which is what the rich-table view reads
    (e.g. ``user.name``, ``grp.created_on_str``). One dict therefore drives
    both output paths, so the JSON/YAML assertions and the table view can never
    silently disagree about the same record.

    ``attrs`` overrides let a table view read a *richer* attribute than the flat
    ``_data`` value -- e.g. ``fn get`` renders ``[t.name for t in fn.tables]``,
    so ``_data["tables"]`` stays a plain list of names for JSON/YAML while the
    ``tables`` attribute is overridden with objects exposing ``.name``.
    """

    def __init__(self, data: dict[str, Any], **attrs: Any):
        self._data = data
        for key, value in data.items():
            setattr(self, key, value)
        for key, value in attrs.items():
            setattr(self, key, value)


def _client() -> MagicMock:
    # spec=TabsdataServer so the mock rejects access to methods the real SDK
    # does not have -- catching CLI<->SDK drift (a renamed/removed SDK method
    # fails the test instead of silently passing on a permissive mock).
    # ``connection`` is set on the instance in __init__ (not on the class, so
    # it is not in the spec); the top-level ``cli`` group sets
    # ``server.connection.project`` on every invoke, so provide it explicitly.
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    return client


# ---------------------------------------------------------------------------
# Fixture data -- each dict carries every field its command's table view reads,
# so it doubles as the exact ``_data`` payload the machine formats emit.
# ---------------------------------------------------------------------------


_USERS = [
    {"name": "alice", "full_name": "Alice A", "email": "alice@x.io", "enabled": True},
    {"name": "bob", "full_name": "Bob B", "email": "bob@x.io", "enabled": False},
]

_GROUPS = [
    {
        "name": "grp-a",
        "id": "GID_A",
        "description": "first group",
        "created_on_str": "2026-01-01",
        "created_by": "admin",
    },
]

_GROUP_INFO = {
    "name": "grp-a",
    "id": "GID_A",
    "system": False,
    "description": "first group",
    "created_on_str": "2026-01-01",
    "created_by": "admin",
}

_PROJECTS = [
    {
        "name": "proj-a",
        "id": "PID_A",
        "description": "first project",
        "created_on_str": "2026-01-02",
        "created_by": "admin",
    },
]

_COLLECTIONS = [
    {
        "name": "coll-a",
        "description": "first collection",
        "created_on_str": "2026-01-03",
        "created_by": "admin",
    },
]

# ``auth info`` is the odd one out: the SDK returns a plain dict (not a DTO) and
# the command passes it to ``render`` directly (``data=info``, not a lambda).
_AUTH_INFO = {"name": "alice", "email": "alice@x.io", "roles": ["sys_admin", "user"]}

# Functions carry a richer payload; ``fn list`` renders a flat table, while
# ``fn get`` renders nested ``tables``/``dependencies``/``triggers``. ``type``
# is the raw wire value ("P"); the machine formats keep it verbatim while the
# table maps it to "Publisher" -- a case where content legitimately differs by
# format, which is exactly why each format is validated on its own terms.
_FUNCTIONS = [
    {
        "name": "fn-a",
        "type": "P",
        "description": "publishes stuff",
        "defined_on_str": "2026-02-01",
        "defined_by": "admin",
    },
]

_FN_INFO = {
    "id": "FID_1",
    "name": "fn-a",
    "description": "publishes stuff",
    "defined_on_str": "2026-02-01",
    "defined_by": "admin",
    "dependencies": ["dep1", "dep2"],
    "triggers": ["trig1"],
    "tables": ["t1", "t2"],
}

# ``exe info`` nests ``collection``/``function`` objects
# (``execution.collection.name``); the machine payload keeps them as dicts.
_EXECUTION = {
    "id": "EID_1",
    "name": "plan-a",
    "collection": {"name": "coll-a"},
    "function": {"name": "fn-a"},
    "triggered_on_str": "2026-03-01",
    "status": "Done",
}

# ``table list`` nests a ``function`` object per table (``table.function.name``).
_TABLES = [{"name": "tbl-a", "function": {"name": "fn-a"}}]

# ``table schema`` returns a raw list of column dicts.
_TABLE_SCHEMA = [{"name": "id", "type": "Int64"}, {"name": "label", "type": "String"}]

# ``table sample`` returns a polars DataFrame: json/yaml serialize ``.to_dicts()``
# while the table view just echoes the frame's own repr.
_SAMPLE_DF = pl.DataFrame({"n": [1, 2], "label": ["x", "y"]})
_SAMPLE_ROWS = _SAMPLE_DF.to_dicts()

# ``metrics show`` aggregates ``nodes`` into a summary table; the machine formats
# emit the whole dict verbatim (edges included -- only the table drops them).
_METRICS = {
    "dim": "collection",
    "unit": "hour",
    "start_ms": 1000,
    "end_ms": 2000,
    "nodes": [
        {
            "collection_id": "C1", "collection_name": "coll-a",
            "group_id": "G1", "group_name": "grp-a",
            "status": "D",
            "fn_runs": 2, "transactions": 1,
            "table_versions_in": 3, "row_count_in": 555, "data_bytes_in": 0,
            "table_versions_out": 4, "row_count_out": 777, "data_bytes_out": 0,
        },
    ],
    "edges": [],
}

# ``metrics inflight`` renders ``rows``; the machine formats emit the whole dict.
_INFLIGHT = {
    "rows": [
        {
            "collection_id": "C1", "group_id": "G1",
            "collection_name": "coll-a", "group_name": "grp-a",
            "status": "R", "count": 42,
        },
    ],
}

# ``fn patches`` returns raw patch dicts. ``defined_on`` is epoch millis:
# json/yaml keep it verbatim while the table renders it as a UTC ISO string
# (1704067200000 ms -> 2024-01-01T00:00:00Z).
_PATCHES = [
    {
        "id": "PV_2",
        "parent_function_version_id": "PV_1",
        "description": "second patch",
        "defined_on": 1704067200000,
        "defined_by": "admin",
        "status": "published",
    },
]

# exe list-* return DTO lists: json/yaml emit ``[x._data ...]`` while the inline
# table view reads nested objects (execution/collection/function/transaction).
_EXECUTIONS = [{
    "id": "EID_1", "name": "plan-a",
    "collection": {"name": "coll-a"}, "function": {"name": "fn-a"},
    "triggered_on_str": "2026-03-01", "trigger_type": "manual", "status": "Done",
}]

_TRANSACTIONS = [{
    "id": "TID_1",
    "execution": {"id": "EID_1"}, "collection": {"name": "coll-a"},
    "triggered_on_str": "2026-03-01", "triggered_by": "admin", "status": "Done",
}]

_FUNCTION_RUNS = [{
    "id": "FR_1",
    "collection": {"name": "coll-a"}, "function": {"name": "fn-a"},
    "execution": {"name": "plan-a", "id": "EID_1"}, "transaction": {"id": "TID_1"},
    "started_on_str": "2026-03-01", "ended_on_str": "2026-03-02", "status": "Done",
}]

_WORKERS = [{
    "id": "WK_1",
    "collection": {"name": "coll-a"}, "function": {"name": "fn-a"},
    "execution": {"name": "plan-a", "id": "EID_1"}, "transaction": {"id": "TID_1"},
    "status": "Done",
}]

# ``table versions``: json/yaml emit ``[dv._data ...]``; the table reads nested
# function/execution objects.
_DATAVERSIONS = [{
    "id": "DV_1",
    "function": {"id": "FID_1"}, "execution": {"id": "EID_1"},
    "created_at_str": "2026-03-01", "status": "C",
}]

# ``permission list`` builds its payload from DTO *attributes* (not _data):
# ``--user`` -> [{permission, project} ...]; ``--proj`` -> [{user, permission,
# source} ...]. A None project is emitted as null (json) / "-" (table).
_PERMS_BY_USER = [
    {"permission": "read_collection", "project": "proj-a"},
    {"permission": "manage_system", "project": None},
]
_ACCESS_BY_PROJECT = [
    {"user": "alice", "permission": "read_collection", "source": "role:reader"},
]


# ---------------------------------------------------------------------------
# The command matrix
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RenderCase:
    id: str
    argv: list[str]
    # Wire the mock client for this command (set the property/return value the
    # command reads off ``ctx.obj["tabsdataserver"]``).
    setup: Callable[[MagicMock], None]
    # Expected machine payload handed to ``render`` (pre-serialization).
    payload: Any
    # Every value the ``table`` view must show. We assert the *content* (the
    # rendered cell/title values), never the layout -- borders, column order,
    # widths and padding are the renderer's business, not the test's.
    #
    # Caveat: this is a *presence* check (substring in the output), not a
    # positional one -- it confirms a value is rendered somewhere, not that it
    # landed in the right column or row. The exact structure/placement of every
    # field is already pinned by the json/yaml tests; the table test only
    # guarantees the human view surfaces the same underlying values. So list
    # only *distinctive* values here: a generic token (a single letter like a
    # status "R", or a number that is a substring of another value on screen)
    # can match incidentally and give a false pass. Prefer names/ids and values
    # that can't collide with titles, headers or other cells.
    table_values: tuple[str, ...]


def _set_user_list(c: MagicMock) -> None:
    c.users = [_DTO(u) for u in _USERS]


def _set_user_info(c: MagicMock) -> None:
    c.get_user.return_value = _DTO(_USERS[0])


def _set_group_list(c: MagicMock) -> None:
    c.groups = [_DTO(g) for g in _GROUPS]


def _set_group_info(c: MagicMock) -> None:
    c.get_group.return_value = _DTO(_GROUP_INFO)


def _set_project_list(c: MagicMock) -> None:
    c.projects = [_DTO(p) for p in _PROJECTS]


def _set_project_info(c: MagicMock) -> None:
    c.get_project.return_value = _DTO(_PROJECTS[0])


def _set_collection_list(c: MagicMock) -> None:
    c.collections = [_DTO(x) for x in _COLLECTIONS]


def _set_collection_info(c: MagicMock) -> None:
    c.get_collection.return_value = _DTO(_COLLECTIONS[0])


def _set_auth_info(c: MagicMock) -> None:
    c.auth_info.return_value = _AUTH_INFO


def _set_fn_list(c: MagicMock) -> None:
    c.list_functions.return_value = [_DTO(f) for f in _FUNCTIONS]


def _set_fn_info(c: MagicMock) -> None:
    # ``fn get`` reads ``[t.name for t in fn.tables]`` -- override the flat
    # ``tables`` attribute with objects while ``_data`` keeps plain names.
    c.get_function.return_value = _DTO(
        _FN_INFO, tables=[_DTO({"name": t}) for t in _FN_INFO["tables"]]
    )


def _set_exe_info(c: MagicMock) -> None:
    # ``exe info`` reads ``execution.collection.name`` / ``.function.name``.
    c.get_execution.return_value = _DTO(
        _EXECUTION,
        collection=_DTO({"name": "coll-a"}),
        function=_DTO({"name": "fn-a"}),
    )


def _set_table_list(c: MagicMock) -> None:
    c.list_tables.return_value = [_DTO(_TABLES[0], function=_DTO({"name": "fn-a"}))]


def _set_table_schema(c: MagicMock) -> None:
    c.get_table_schema.return_value = _TABLE_SCHEMA


def _set_table_sample(c: MagicMock) -> None:
    c.sample_table.return_value = _SAMPLE_DF


def _set_metrics_show(c: MagicMock) -> None:
    c.metrics.return_value = _METRICS


def _set_metrics_inflight(c: MagicMock) -> None:
    c.inflight.return_value = _INFLIGHT


def _set_fn_info_history(c: MagicMock) -> None:
    # ``fn get --show-history`` lists versions; same DTO shape as ``fn get``.
    c.list_function_history.return_value = [
        _DTO(_FN_INFO, tables=[_DTO({"name": t}) for t in _FN_INFO["tables"]])
    ]


def _set_fn_list_patches(c: MagicMock) -> None:
    c.list_function_patches.return_value = _PATCHES


def _set_exe_list_plan(c: MagicMock) -> None:
    c.list_executions.return_value = [
        _DTO(_EXECUTIONS[0], collection=_DTO({"name": "coll-a"}),
             function=_DTO({"name": "fn-a"}))
    ]


def _set_exe_list_trx(c: MagicMock) -> None:
    c.list_transactions.return_value = [
        _DTO(_TRANSACTIONS[0], execution=_DTO({"id": "EID_1"}),
             collection=_DTO({"name": "coll-a"}))
    ]


def _set_exe_list_fn_run(c: MagicMock) -> None:
    c.list_function_runs.return_value = [
        _DTO(_FUNCTION_RUNS[0], collection=_DTO({"name": "coll-a"}),
             function=_DTO({"name": "fn-a"}),
             execution=_DTO({"name": "plan-a", "id": "EID_1"}),
             transaction=_DTO({"id": "TID_1"}))
    ]


def _set_exe_list_worker(c: MagicMock) -> None:
    c.list_workers.return_value = [
        _DTO(_WORKERS[0], collection=_DTO({"name": "coll-a"}),
             function=_DTO({"name": "fn-a"}),
             execution=_DTO({"name": "plan-a", "id": "EID_1"}),
             transaction=_DTO({"id": "TID_1"}),
             # the Patch column reads the raw row via `.kwargs`
             kwargs={"logical_function_run_id": "LFR_1",
                     "function_version_patch_id": "FV_9"})
    ]


def _set_table_versions(c: MagicMock) -> None:
    c.list_dataversions.return_value = [
        _DTO(_DATAVERSIONS[0], function=_DTO({"id": "FID_1"}),
             execution=_DTO({"id": "EID_1"}))
    ]


def _set_perm_list_user(c: MagicMock) -> None:
    c.list_permissions_by_user.return_value = [_DTO(p) for p in _PERMS_BY_USER]


def _set_perm_list_project(c: MagicMock) -> None:
    c.list_permissions_by_project.return_value = [_DTO(e) for e in _ACCESS_BY_PROJECT]


CASES: tuple[RenderCase, ...] = (
    RenderCase(
        "user-list", ["user", "list"], _set_user_list, _USERS,
        ("alice", "Alice A", "alice@x.io", "True",
         "bob", "Bob B", "bob@x.io", "False"),
    ),
    RenderCase(
        "user-get", ["user", "get", "-n", "alice"], _set_user_info, _USERS[0],
        ("alice", "Alice A", "alice@x.io", "True"),
    ),
    RenderCase(
        "group-list", ["group", "list"], _set_group_list, _GROUPS,
        ("grp-a", "GID_A", "first group", "2026-01-01", "admin"),
    ),
    RenderCase(
        "group-get", ["group", "get", "-n", "grp-a"], _set_group_info, _GROUP_INFO,
        # ``system`` is rendered as ``str(bool(grp.system))`` -> "False".
        ("grp-a", "GID_A", "False", "first group", "2026-01-01", "admin"),
    ),
    RenderCase(
        "project-list", ["project", "list"], _set_project_list, _PROJECTS,
        ("proj-a", "PID_A", "first project", "2026-01-02", "admin"),
    ),
    RenderCase(
        "project-get", ["project", "get", "-n", "proj-a"], _set_project_info,
        _PROJECTS[0], ("proj-a", "PID_A", "first project", "2026-01-02", "admin"),
    ),
    RenderCase(
        "collection-list", ["collection", "list"], _set_collection_list, _COLLECTIONS,
        ("coll-a", "first collection", "2026-01-03", "admin"),
    ),
    RenderCase(
        "collection-get", ["collection", "get", "-n", "coll-a"], _set_collection_info,
        _COLLECTIONS[0], ("coll-a", "first collection", "2026-01-03", "admin"),
    ),
    RenderCase(
        # ``roles`` renders via beautify_list; assert the distinctive role value
        # ("user" is too generic -- it also appears in the table title).
        "auth-info", ["auth", "info"], _set_auth_info, _AUTH_INFO,
        ("alice", "alice@x.io", "sys_admin"),
    ),
    RenderCase(
        # raw type "P" is mapped to "Publisher" in the table view.
        "fn-list", ["fn", "list", "-c", "coll-a"], _set_fn_list, _FUNCTIONS,
        ("fn-a", "Publisher", "publishes stuff", "2026-02-01", "admin"),
    ),
    RenderCase(
        "fn-get", ["fn", "get", "-n", "fn-a", "-c", "coll-a"], _set_fn_info, _FN_INFO,
        ("fn-a", "FID_1", "publishes stuff", "2026-02-01", "admin",
         "dep1", "dep2", "trig1", "t1", "t2"),
    ),
    RenderCase(
        # exe info builds its table inline (not via render) for the table format,
        # and only calls render(data=execution._data) for json/yaml.
        "exe-info", ["exe", "info", "--plan", "EID_1"], _set_exe_info, _EXECUTION,
        ("plan-a", "coll-a", "fn-a", "2026-03-01", "Done"),
    ),
    RenderCase(
        "table-list", ["table", "list", "-c", "coll-a"], _set_table_list, _TABLES,
        ("tbl-a", "fn-a"),
    ),
    RenderCase(
        "table-schema", ["table", "schema", "-c", "coll-a", "--name", "tbl-a"],
        _set_table_schema, _TABLE_SCHEMA, ("id", "Int64", "label", "String"),
    ),
    RenderCase(
        # table sample's "table" view is just the polars frame's repr, which
        # carries the same rows json/yaml get from ``.to_dicts()``.
        "table-sample", ["table", "sample", "-c", "coll-a", "--name", "tbl-a"],
        _set_table_sample, _SAMPLE_ROWS, ("tbl-a", "label", "x", "y"),
    ),
    RenderCase(
        # rows_in/out are 555/777 -- distinctive values that can't be confused
        # with the window bounds (1000/2000) echoed in the summary header.
        "metrics-show", ["metrics", "show"], _set_metrics_show, _METRICS,
        ("coll-a", "grp-a", "555", "777"),
    ),
    RenderCase(
        "metrics-inflight", ["metrics", "inflight"], _set_metrics_inflight, _INFLIGHT,
        ("coll-a", "grp-a", "42"),
    ),
    RenderCase(
        # --show-history returns a list of versions via list_function_history.
        "fn-get-history",
        ["fn", "get", "-n", "fn-a", "-c", "coll-a", "--show-history"],
        _set_fn_info_history, [_FN_INFO],
        ("fn-a", "FID_1", "publishes stuff", "dep1", "trig1", "t1", "t2"),
    ),
    RenderCase(
        # epoch defined_on (1704067200000) is kept raw in json/yaml but rendered
        # as the UTC ISO string in the table.
        "fn-patches", ["fn", "patches", "-n", "fn-a", "-c", "coll-a"],
        _set_fn_list_patches, _PATCHES,
        ("PV_2", "PV_1", "second patch", "2024-01-01T00:00:00Z", "published"),
    ),
    RenderCase(
        # exe list-* emit [x._data ...] for json/yaml and build the table inline.
        "exe-list-plan", ["exe", "list-plan"], _set_exe_list_plan, _EXECUTIONS,
        ("EID_1", "plan-a", "coll-a", "fn-a", "2026-03-01", "manual", "Done"),
    ),
    RenderCase(
        "exe-list-trx", ["exe", "list-trx"], _set_exe_list_trx, _TRANSACTIONS,
        ("TID_1", "EID_1", "coll-a", "2026-03-01", "admin", "Done"),
    ),
    RenderCase(
        "exe-list-fn-run", ["exe", "list-fn-run"], _set_exe_list_fn_run, _FUNCTION_RUNS,
        ("FR_1", "coll-a", "fn-a", "plan-a", "EID_1", "TID_1",
         "2026-03-01", "2026-03-02", "Done"),
    ),
    RenderCase(
        "exe-list-worker", ["exe", "list-worker"], _set_exe_list_worker, _WORKERS,
        ("WK_1", "coll-a", "fn-a", "plan-a", "EID_1", "TID_1", "Done", "FV_9"),
    ),
    RenderCase(
        "table-versions", ["table", "versions", "-c", "coll-a", "--name", "tbl-a"],
        _set_table_versions, _DATAVERSIONS,
        ("DV_1", "FID_1", "EID_1", "2026-03-01"),
    ),
    RenderCase(
        # payload is built from attributes; a None project is null (json) / "-" (table).
        "permission-list-user", ["permission", "list", "--user", "alice"],
        _set_perm_list_user, _PERMS_BY_USER,
        ("read_collection", "proj-a", "manage_system"),
    ),
    RenderCase(
        "permission-list-project", ["permission", "list", "--proj", "proj-a"],
        _set_perm_list_project, _ACCESS_BY_PROJECT,
        ("alice", "read_collection", "role:reader"),
    ),
)

_IDS = [c.id for c in CASES]


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def _invoke(argv: list[str], *, client: MagicMock, tmp_path):
    """Run ``td <argv>`` with ``client`` injected as the server connection."""

    def _inject(ctx):
        # The top-level group has already built ctx.obj by the time it calls
        # initialise_tabsdata_server_connection; just drop the mock client in.
        ctx.obj["tabsdataserver"] = client

    with patch(
        "tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject
    ), patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)):
        # A wide terminal so the rich table never line-wraps a cell -- wrapping
        # is a layout artifact that would break content substring checks; we
        # assert on values, not on where the renderer chooses to break lines.
        return CliRunner().invoke(cli, argv, env={"COLUMNS": "200"})


def _normalize(payload: Any) -> Any:
    """Mirror ``render``: machine formats go through ``json(default=str)``."""
    return json.loads(json.dumps(payload, default=str))


def _load_yaml(text: str) -> Any:
    return YAML(typ="safe").load(io.StringIO(text))


@pytest.mark.parametrize("case", CASES, ids=_IDS)
def test_json_output_matches_payload(case: RenderCase, tmp_path):
    client = _client()
    case.setup(client)
    result = _invoke(case.argv + ["-o", "json"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _normalize(case.payload)


@pytest.mark.parametrize("case", CASES, ids=_IDS)
def test_yaml_output_matches_payload(case: RenderCase, tmp_path):
    client = _client()
    case.setup(client)
    result = _invoke(case.argv + ["-o", "yaml"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert _load_yaml(result.output) == _normalize(case.payload)


@pytest.mark.parametrize("case", CASES, ids=_IDS)
def test_table_output_shows_content(case: RenderCase, tmp_path):
    """The default (table) view must render every expected value. We validate
    *content* -- that each value appears -- not the table's format (borders,
    alignment, column order); those belong to the renderer, not the test."""
    client = _client()
    case.setup(client)
    # No -o flag -> the default "table" format.
    result = _invoke(case.argv, client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    for value in case.table_values:
        assert value in result.output, (
            f"{value!r} missing from `td {' '.join(case.argv)}` table output:\n"
            f"{result.output}"
        )


def test_invalid_output_format_is_rejected(tmp_path):
    """``-o`` is a fixed Choice(table, json, yaml); anything else is a usage
    error caught at parse time, so the command body (get_user) never runs."""
    client = _client()
    _set_user_info(client)
    result = _invoke(
        ["user", "get", "-n", "alice", "-o", "xml"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code != 0
    assert "xml" in result.output  # click names the offending value
    client.get_user.assert_not_called()


def test_json_and_yaml_agree_across_the_matrix(tmp_path):
    """The two machine formats must serialize identical data for every command
    -- a cross-check that neither path drops or reshapes fields."""
    for case in CASES:
        client = _client()
        case.setup(client)
        j = _invoke(case.argv + ["-o", "json"], client=client, tmp_path=tmp_path)
        y = _invoke(case.argv + ["-o", "yaml"], client=client, tmp_path=tmp_path)
        assert j.exit_code == 0 and y.exit_code == 0
        assert json.loads(j.output) == _load_yaml(y.output), case.id


# ---------------------------------------------------------------------------
# ``exe logs`` -- kept out of the matrix.
#
# Its payload is not a DTO: the command wraps ``get_worker_log`` output in a
# text envelope ("Logs from worker '<id>' underneath: ... ***") and hands it to
# render as ``{"logs": <blob>}`` (table view just echoes the blob). Pinning the
# exact envelope punctuation would duplicate production formatting and be brittle,
# so we assert the worker id and log body are *present* in every format instead.
# ---------------------------------------------------------------------------


def _set_worker_log(c: MagicMock) -> None:
    c.get_worker_log.return_value = "line-1\nline-2"


def test_exe_logs_carries_worker_log_in_every_format(tmp_path):
    # json / yaml wrap the constructed envelope under a single "logs" key.
    for fmt in ("json", "yaml"):
        client = _client()
        _set_worker_log(client)
        result = _invoke(
            ["exe", "logs", "--worker", "W1", "-o", fmt],
            client=client,
            tmp_path=tmp_path,
        )
        assert result.exit_code == 0, result.output
        payload = (
            json.loads(result.output)
            if fmt == "json"
            else _load_yaml(result.output)
        )
        assert set(payload) == {"logs"}, payload
        assert "W1" in payload["logs"]
        assert "line-1" in payload["logs"] and "line-2" in payload["logs"]

    # the default (table) view echoes the same envelope text.
    client = _client()
    _set_worker_log(client)
    result = _invoke(
        ["exe", "logs", "--worker", "W1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert "W1" in result.output
    assert "line-1" in result.output and "line-2" in result.output


def test_exe_logs_file_dash_writes_raw_logs_to_stdout(tmp_path):
    # ``--file -`` means stdout: the raw log text is emitted, with no "Saving
    # logs to file" chatter and no file literally named "-".
    client = _client()
    _set_worker_log(client)
    result = _invoke(
        ["exe", "logs", "--worker", "W1", "--file", "-"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert "W1" in result.output
    assert "line-1" in result.output and "line-2" in result.output
    assert "Saving logs to file" not in result.output


def _run_client(*workers: tuple[str, str]) -> MagicMock:
    """Client whose worker list resolves to ``workers`` (in the given order).

    ``latest_workers`` keeps the server order, so the last one passed here is
    treated as the latest. ``get_worker_log`` echoes the worker id so tests can
    assert which one was fetched.
    """
    client = _client()
    client.list_workers.return_value = [
        _DTO(
            {"id": wid, "status": status},
            kwargs={"function_version_patch_id": None},
        )
        for wid, status in workers
    ]
    client.get_worker_log.side_effect = lambda wid: f"log-of-{wid}"
    return client


def test_exe_logs_fn_run_returns_latest_worker(tmp_path):
    client = _run_client(("WKR20260713.1", "Failed"), ("WKR20260713.2", "Done"))
    result = _invoke(
        ["exe", "logs", "--fn-run", "LFR_1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    # the run's latest worker, and only it
    assert "log-of-WKR20260713.2" in result.output
    assert "log-of-WKR20260713.1" not in result.output


def test_exe_logs_by_function_name(tmp_path):
    client = _run_client(("WKR20260713.1", "Failed"), ("WKR20260713.2", "Done"))
    result = _invoke(
        ["exe", "logs", "--fn", "my_fn", "--coll", "my_coll"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert "log-of-WKR20260713.2" in result.output
    # the function name + collection were pushed into the workers filter
    sent = client.list_workers.call_args.kwargs["filter"]
    assert "function:eq:my_fn" in sent and "collection:eq:my_coll" in sent


def test_exe_logs_fn_run_with_no_workers_errors(tmp_path):
    client = _run_client()
    result = _invoke(
        ["exe", "logs", "--fn-run", "LFR_1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code != 0
    assert "no workers yet" in result.output


def test_exe_logs_requires_exactly_one_target(tmp_path):
    neither = _invoke(["exe", "logs"], client=_client(), tmp_path=tmp_path)
    assert neither.exit_code != 0 and "exactly one" in neither.output
    both = _invoke(
        ["exe", "logs", "--fn-run", "LFR_1", "--worker", "WKR20260713.1"],
        client=_client(),
        tmp_path=tmp_path,
    )
    assert both.exit_code != 0 and "exactly one" in both.output


# ---------------------------------------------------------------------------
# ``pin show`` -- kept out of the matrix.
#
# It is fully offline: it renders ``ctx.obj["pinned_objects"]`` (loaded from
# ``pinned.json`` in the tabsdata dir) and never calls the server. So instead of
# a mock client we seed the pinned-objects file in the patched tabsdata dir.
# ---------------------------------------------------------------------------


def test_pin_show_renders_pinned_store_offline(tmp_path):
    (tmp_path / "pinned.json").write_text(
        json.dumps({"project": "proj-a", "collection": "coll-a"})
    )
    payload = {"project": "proj-a", "collection": "coll-a"}

    for fmt in ("json", "yaml"):
        result = _invoke(
            ["pin", "show", "-o", fmt], client=_client(), tmp_path=tmp_path
        )
        assert result.exit_code == 0, result.output
        got = (
            json.loads(result.output)
            if fmt == "json"
            else _load_yaml(result.output)
        )
        assert got == payload

    # the default (table) view prints both pinned values.
    result = _invoke(["pin", "show"], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "proj-a" in result.output and "coll-a" in result.output
