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
        # Real SDK DTOs keep the raw row on ``.kwargs``; table views read fields
        # like patch/run ids off it. Mirror that (an explicit ``kwargs=`` attr
        # override still wins, for rows that carry ids only on the raw payload).
        self.kwargs = data
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
    # A concrete active project: form views (e.g. ``fn get``) render it, and a
    # non-None value keeps the list commands' spanning check unchanged.
    client.connection.project = "proj-a"
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
    # Real Function DTOs always carry patch_of (None when the version is not a
    # patch); ``fn get --show-history`` renders it as the "Patches" column.
    "patch_of": "FV_0",
    "name": "fn-a",
    "description": "publishes stuff",
    "type": "P",
    "status": "A",
    "enabled": True,
    "defined_on_str": "2026-02-01",
    "defined_by": "admin",
    "dependencies": ["dep1", "dep2"],
    "triggers": ["trig1"],
    "tables": ["t1", "t2"],
    "snippet": "@publisher()\ndef fn_a():\n    ...\n",
}

# ``plan get`` nests ``collection``/``function`` objects
# (``execution.collection.name``); the machine payload keeps them as dicts.
_EXECUTION = {
    "id": "EID_1",
    "name": "plan-a",
    "project": "proj-a",
    "collection": {"name": "coll-a"},
    "function": {"name": "fn-a"},
    "triggered_on_str": "2026-03-01",
    "started_on_str": "2026-03-01",
    "ended_on_str": "2026-03-02",
    "status": "Done",
}

# ``table list`` nests a ``function`` object per table (``table.function.name``);
# status/private/created ride on the raw row (read via ``.kwargs``).
_TABLES = [
    {
        "name": "tbl-a",
        "function": {"name": "fn-a"},
        "status": "A",
        "private": False,
        "defined_by": "admin",
        "defined_on": 1704067200000,
    }
]

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
            "collection_id": "C1",
            "collection_name": "coll-a",
            "group_id": "G1",
            "group_name": "grp-a",
            "status": "D",
            "fn_runs": 2,
            "transactions": 1,
            "table_versions_in": 3,
            "row_count_in": 555,
            "data_bytes_in": 0,
            "table_versions_out": 4,
            "row_count_out": 777,
            "data_bytes_out": 0,
        },
    ],
    "edges": [],
}

# ``metrics inflight`` renders ``rows``; the machine formats emit the whole dict.
_INFLIGHT = {
    "rows": [
        {
            "collection_id": "C1",
            "group_id": "G1",
            "collection_name": "coll-a",
            "group_name": "grp-a",
            "status": "R",
            "count": 42,
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

# plan/trx/run/worker list return DTO lists: json/yaml emit ``[x._data ...]``
# while the table view reads nested objects (execution/collection/function/
# transaction) and the always-present Project column.
_EXECUTIONS = [
    {
        "id": "EID_1",
        "name": "plan-a",
        "project": "proj-a",
        "collection": {"name": "coll-a"},
        "function": {"name": "fn-a"},
        "triggered_by": "admin",
        "triggered_on_str": "2026-03-01",
        "started_on_str": "2026-03-01",
        "ended_on_str": "2026-03-02",
        "trigger_type": "manual",
        "status": "Done",
    }
]

_TRANSACTIONS = [
    {
        "id": "TID_1",
        "project": "proj-a",
        "execution": {"id": "EID_1"},
        "collection": {"name": "coll-a"},
        "triggered_on_str": "2026-03-01",
        "triggered_by": "admin",
        "started_on_str": "2026-03-01",
        "ended_on_str": "2026-03-02",
        "status": "Done",
    }
]

_FUNCTION_RUNS = [
    {
        "id": "FR_1",
        "project": "proj-a",
        "collection": {"name": "coll-a"},
        "function": {"name": "fn-a"},
        "execution": {"name": "plan-a", "id": "EID_1"},
        "transaction": {"id": "TID_1"},
        "started_on_str": "2026-03-01",
        "ended_on_str": "2026-03-02",
        "status": "Done",
    }
]

_WORKERS = [
    {
        "id": "WK_1",
        "project": "proj-a",
        "collection": {"name": "coll-a"},
        "function": {"name": "fn-a"},
        "execution": {"name": "plan-a", "id": "EID_1"},
        "transaction": {"id": "TID_1"},
        "started_on_str": "2026-03-01",
        "ended_on_str": "2026-03-02",
        "status": "Done",
    }
]

# ``table versions``: json/yaml emit ``[dv._data ...]``; the table reads nested
# function/execution objects.
_DATAVERSIONS = [
    {
        "id": "DV_1",
        "function": "fn-a",
        "function_version_id": "FV_1",
        "execution_id": "EID_1",
        "transaction_id": "TID_1",
        "created_at_str": "2026-03-01",
        "schema_data_changed": False,
        "data_changed": True,
        "row_count": 42,
        "data_bytes": 1048576,
        "status": "C",
    }
]

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


def _wire_listing(c: MagicMock, name: str, rows: list) -> None:
    """Wire a paginated listing on the mock client.

    The list commands consume the ``*_generator`` twin (so ``--max-result`` can
    stop early), while the embedded listings inside ``get``-style commands still
    call the materializing method; both are wired to the same rows. The
    generator is a fresh iterator per call, as the real one is."""
    getattr(c, name).return_value = rows
    getattr(c, f"{name}_generator").side_effect = lambda *a, **k: iter(rows)


def _set_user_list(c: MagicMock) -> None:
    _wire_listing(c, "list_users", [_DTO(u) for u in _USERS])


def _set_user_info(c: MagicMock) -> None:
    c.get_user.return_value = _DTO(_USERS[0])


def _set_group_list(c: MagicMock) -> None:
    _wire_listing(c, "list_groups", [_DTO(g) for g in _GROUPS])


def _set_group_info(c: MagicMock) -> None:
    c.get_group.return_value = _DTO(_GROUP_INFO)
    # ``group get`` lists the collections whose ``group`` matches the group name.
    c.list_collections.return_value = [
        _DTO({"name": "coll-a", "group": "grp-a"}),
        _DTO({"name": "coll-z", "group": "grp-b"}),
    ]


def _set_project_list(c: MagicMock) -> None:
    _wire_listing(c, "list_projects", [_DTO(p) for p in _PROJECTS])


def _set_project_info(c: MagicMock) -> None:
    c.get_project.return_value = _DTO(_PROJECTS[0])


def _set_collection_list(c: MagicMock) -> None:
    _wire_listing(c, "list_collections", [_DTO(x) for x in _COLLECTIONS])


def _set_collection_info(c: MagicMock) -> None:
    c.get_collection.return_value = _DTO(_COLLECTIONS[0])


def _set_auth_info(c: MagicMock) -> None:
    c.auth_info.return_value = _AUTH_INFO


def _set_fn_list(c: MagicMock) -> None:
    _wire_listing(c, "list_functions", [_DTO(f) for f in _FUNCTIONS])


def _set_fn_info(c: MagicMock) -> None:
    # ``fn get`` reads ``[t.name for t in fn.tables]`` -- override the flat
    # ``tables`` attribute with objects while ``_data`` keeps plain names.
    c.get_function.return_value = _DTO(
        _FN_INFO, tables=[_DTO({"name": t}) for t in _FN_INFO["tables"]]
    )


def _set_plan_list(c: MagicMock) -> None:
    _wire_listing(
        c,
        "list_executions",
        [
            _DTO(
                _EXECUTIONS[0],
                collection=_DTO({"name": "coll-a"}),
                function=_DTO({"name": "fn-a"}),
            )
        ],
    )


def _set_trx_list(c: MagicMock) -> None:
    _wire_listing(
        c,
        "list_transactions",
        [
            _DTO(
                _TRANSACTIONS[0],
                execution=_DTO({"id": "EID_1"}),
                collection=_DTO({"name": "coll-a"}),
            )
        ],
    )


def _set_run_list(c: MagicMock) -> None:
    _wire_listing(
        c,
        "list_function_runs",
        [
            _DTO(
                _FUNCTION_RUNS[0],
                collection=_DTO({"name": "coll-a"}),
                function=_DTO({"name": "fn-a"}),
                execution=_DTO({"name": "plan-a", "id": "EID_1"}),
                transaction=_DTO({"id": "TID_1"}),
            )
        ],
    )


def _set_worker_list(c: MagicMock) -> None:
    _wire_listing(
        c,
        "list_workers",
        [
            _DTO(
                _WORKERS[0],
                collection=_DTO({"name": "coll-a"}),
                function=_DTO({"name": "fn-a"}),
                execution=_DTO({"name": "plan-a", "id": "EID_1"}),
                transaction=_DTO({"id": "TID_1"}),
                # the id/patch columns read the raw row via `.kwargs`
                kwargs={
                    "logical_function_run_id": "LFR_1",
                    "execution_id": "EID_1",
                    "transaction_id": "TID_1",
                    "function_version_patch_id": "FV_9",
                },
            )
        ],
    )


def _set_plan_info(c: MagicMock) -> None:
    # ``plan get`` reads ``execution.collection.name`` / ``.function.name``
    # and lists the plan's function runs and workers underneath.
    c.get_execution.return_value = _DTO(
        _EXECUTION,
        collection=_DTO({"name": "coll-a"}),
        function=_DTO({"name": "fn-a"}),
    )
    _set_run_list(c)
    _set_worker_list(c)


def _set_trx_info(c: MagicMock) -> None:
    c.get_transaction.return_value = _DTO(
        _TRANSACTIONS[0],
        execution=_DTO({"id": "EID_1"}),
        collection=_DTO({"name": "coll-a"}),
    )
    _set_run_list(c)
    _set_worker_list(c)


def _set_run_info(c: MagicMock) -> None:
    c.get_function_run.return_value = _DTO(
        _FUNCTION_RUNS[0],
        collection=_DTO({"name": "coll-a"}),
        function=_DTO({"name": "fn-a"}),
        execution=_DTO({"name": "plan-a", "id": "EID_1"}),
        transaction=_DTO({"id": "TID_1"}),
    )
    _set_worker_list(c)


def _set_table_list(c: MagicMock) -> None:
    _wire_listing(
        c, "list_tables", [_DTO(_TABLES[0], function=_DTO({"name": "fn-a"}))]
    )


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
    _wire_listing(c, "list_function_patches", _PATCHES)


def _set_table_versions(c: MagicMock) -> None:
    _wire_listing(c, "list_dataversions", [_DTO(_DATAVERSIONS[0])])


def _set_perm_list_user(c: MagicMock) -> None:
    c.list_permissions_by_user.return_value = [_DTO(p) for p in _PERMS_BY_USER]


def _set_perm_list_project(c: MagicMock) -> None:
    c.list_permissions_by_project.return_value = [_DTO(e) for e in _ACCESS_BY_PROJECT]


CASES: tuple[RenderCase, ...] = (
    RenderCase(
        "user-list",
        ["user", "list"],
        _set_user_list,
        _USERS,
        ("alice", "Alice A", "alice@x.io", "True", "bob", "Bob B", "bob@x.io", "False"),
    ),
    RenderCase(
        "user-get", ["user", "get", "-n", "alice"], _set_user_info, _USERS[0],
        ("alice", "Alice A", "alice@x.io", "True"),
    ),
    RenderCase(
        "group-list",
        ["group", "list"],
        _set_group_list,
        _GROUPS,
        ("grp-a", "first group", "2026-01-01", "admin"),
    ),
    RenderCase(
        "group-get", ["group", "get", "-n", "grp-a"], _set_group_info, _GROUP_INFO,
        # Form view: name/description/created + the collections in the group
        # ("coll-a" belongs to grp-a; "coll-z" is in another group).
        ("grp-a", "first group", "2026-01-01", "admin", "coll-a"),
    ),
    RenderCase(
        "project-list",
        ["project", "list"],
        _set_project_list,
        _PROJECTS,
        ("proj-a", "first project", "2026-01-02", "admin"),
    ),
    RenderCase(
        "project-get", ["project", "get", "-n", "proj-a"], _set_project_info,
        _PROJECTS[0], ("proj-a", "first project", "2026-01-02", "admin"),
    ),
    RenderCase(
        "collection-list",
        ["collection", "list"],
        _set_collection_list,
        _COLLECTIONS,
        ("coll-a", "first collection", "2026-01-03", "admin"),
    ),
    RenderCase(
        "collection-get", ["collection", "get", "-n", "coll-a"], _set_collection_info,
        _COLLECTIONS[0], ("coll-a", "first collection", "2026-01-03", "admin"),
    ),
    RenderCase(
        # ``roles`` renders via beautify_list; assert the distinctive role value
        # ("user" is too generic -- it also appears in the table title).
        "auth-info",
        ["auth", "info"],
        _set_auth_info,
        _AUTH_INFO,
        ("alice", "alice@x.io", "sys_admin"),
    ),
    RenderCase(
        # raw type "P" is mapped to "Publisher" in the table view.
        "fn-list",
        ["fn", "list", "-c", "coll-a"],
        _set_fn_list,
        _FUNCTIONS,
        ("fn-a", "Publisher", "publishes stuff", "2026-02-01", "admin"),
    ),
    RenderCase(
        "fn-get", ["fn", "get", "-n", "fn-a", "-c", "coll-a"], _set_fn_info, _FN_INFO,
        # Form view: no ID row; project/collection/status(->"Active") are shown.
        ("fn-a", "proj-a", "coll-a", "publishes stuff", "2026-02-01", "admin",
         "Active", "dep1", "dep2", "trig1", "t1", "t2"),
    ),
    RenderCase(
        # plan get builds its tables inline for the table format and hands a
        # combined {plan, function_runs, workers} payload to render for json/yaml.
        "plan-get",
        ["plan", "get", "--plan", "EID_1"],
        _set_plan_info,
        {"plan": _EXECUTION, "function_runs": _FUNCTION_RUNS, "workers": _WORKERS},
        (
            "plan-a",
            "proj-a",
            "coll-a",
            "fn-a",
            "2026-03-01",
            "Done",
            "FR_1",
            "WK_1",
            "TID_1",
        ),
    ),
    RenderCase(
        "trx-get",
        ["trx", "get", "--trx", "TID_1"],
        _set_trx_info,
        {
            "transaction": _TRANSACTIONS[0],
            "function_runs": _FUNCTION_RUNS,
            "workers": _WORKERS,
        },
        ("TID_1", "proj-a", "coll-a", "2026-03-01", "admin", "Done", "FR_1", "WK_1"),
    ),
    RenderCase(
        "fnrun-get",
        ["fnrun", "get", "--fnrun", "FR_1"],
        _set_run_info,
        {"function_run": _FUNCTION_RUNS[0], "workers": _WORKERS},
        (
            "FR_1",
            "proj-a",
            "coll-a",
            "fn-a",
            "plan-a",
            "EID_1",
            "TID_1",
            "2026-03-01",
            "2026-03-02",
            "Done",
            "WK_1",
        ),
    ),
    RenderCase(
        "table-list",
        ["table", "list", "-c", "coll-a"],
        _set_table_list,
        _TABLES,
        ("tbl-a", "fn-a", "Active", "2024-01-01T00:00:00Z", "admin"),
    ),
    RenderCase(
        "table-schema",
        ["table", "schema", "-c", "coll-a", "--name", "tbl-a"],
        _set_table_schema,
        _TABLE_SCHEMA,
        ("id", "Int64", "label", "String"),
    ),
    RenderCase(
        # table sample's "table" view is just the polars frame's repr, which
        # carries the same rows json/yaml get from ``.to_dicts()``.
        "table-sample",
        ["table", "sample", "-c", "coll-a", "--name", "tbl-a"],
        _set_table_sample,
        _SAMPLE_ROWS,
        ("tbl-a", "label", "x", "y"),
    ),
    RenderCase(
        # rows_in/out are 555/777 -- distinctive values that can't be confused
        # with the window bounds (1000/2000) echoed in the summary header.
        "metrics-show",
        ["metrics", "show"],
        _set_metrics_show,
        _METRICS,
        ("coll-a", "grp-a", "555", "777"),
    ),
    RenderCase(
        "metrics-inflight",
        ["metrics", "inflight"],
        _set_metrics_inflight,
        _INFLIGHT,
        ("coll-a", "grp-a", "42"),
    ),
    RenderCase(
        # --show-history returns a list of versions via list_function_history.
        "fn-get-history",
        ["fn", "get", "-n", "fn-a", "-c", "coll-a", "--show-history"],
        _set_fn_info_history, [_FN_INFO],
        ("fn-a", "FID_1", "FV_0", "publishes stuff", "dep1", "trig1", "t1", "t2"),
    ),
    RenderCase(
        # epoch defined_on (1704067200000) is kept raw in json/yaml but rendered
        # as the UTC ISO string in the table.
        "fn-patches", ["fn", "patches", "-n", "fn-a", "-c", "coll-a"],
        _set_fn_list_patches, _PATCHES,
        ("PV_2", "PV_1", "second patch", "2024-01-01T00:00:00Z", "published"),
    ),
    RenderCase(
        # plan/trx/run/worker list emit [x._data ...] for json/yaml and build
        # the table inline; the Project column always renders.
        "plan-list",
        ["plan", "list"],
        _set_plan_list,
        _EXECUTIONS,
        ("EID_1", "plan-a", "proj-a", "coll-a", "fn-a", "2026-03-01", "admin", "Done"),
    ),
    RenderCase(
        "trx-list",
        ["trx", "list"],
        _set_trx_list,
        _TRANSACTIONS,
        ("TID_1", "EID_1", "proj-a", "coll-a", "2026-03-01", "admin", "Done"),
    ),
    RenderCase(
        "fnrun-list",
        ["fnrun", "list"],
        _set_run_list,
        _FUNCTION_RUNS,
        (
            "FR_1",
            "proj-a",
            "coll-a",
            "fn-a",
            "EID_1",
            "TID_1",
            "2026-03-01",
            "2026-03-02",
            "Done",
        ),
    ),
    RenderCase(
        "worker-list",
        ["worker", "list"],
        _set_worker_list,
        _WORKERS,
        (
            "WK_1",
            "LFR_1",
            "proj-a",
            "coll-a",
            "fn-a",
            "EID_1",
            "TID_1",
            "Done",
            "FV_9",
        ),
    ),
    RenderCase(
        "table-versions",
        ["table", "versions", "-c", "coll-a", "--name", "tbl-a"],
        _set_table_versions,
        _DATAVERSIONS,
        ("DV_1", "fn-a", "FV_1", "EID_1", "TID_1", "2026-03-01", "42", "1.00"),
    ),
    RenderCase(
        # payload is built from attributes; a None project is null (json) / "-" (table).
        "permission-list-user",
        ["permission", "list", "--user", "alice"],
        _set_perm_list_user,
        _PERMS_BY_USER,
        ("read_collection", "proj-a", "manage_system"),
    ),
    RenderCase(
        "permission-list-project",
        ["permission", "list", "--proj", "proj-a"],
        _set_perm_list_project,
        _ACCESS_BY_PROJECT,
        ("alice", "read_collection", "role:reader"),
    ),
)

_IDS = [c.id for c in CASES]


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def _invoke(argv: list[str], *, client: MagicMock, tmp_path, input: str | None = None):
    """Run ``td <argv>`` with ``client`` injected as the server connection."""

    def _inject(ctx):
        # The top-level group has already built ctx.obj by the time it calls
        # initialise_tabsdata_server_connection; just drop the mock client in.
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        # A wide terminal so the rich table never line-wraps a cell -- wrapping
        # is a layout artifact that would break content substring checks; we
        # assert on values, not on where the renderer chooses to break lines.
        return CliRunner().invoke(cli, argv, env={"COLUMNS": "200"}, input=input)


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
# ``worker/run/plan/trx logs`` -- kept out of the matrix.
#
# Their payload is not a DTO: the command wraps ``get_worker_log`` output in a
# text envelope ("Worker <id> ...") and hands it to render as ``{"logs":
# <blob>}`` (table view just echoes the blob). Pinning the exact envelope
# punctuation would duplicate production formatting and be brittle, so we
# assert the worker id and log body are *present* in every format instead.
# The plan/trx/run variants resolve the worker via the picker: a single worker
# streams directly; several render an indexed listing and prompt for a choice.
# ---------------------------------------------------------------------------


def _set_worker_log(c: MagicMock) -> None:
    c.get_worker_log.return_value = "line-1\nline-2"


def test_worker_logs_carries_worker_log_in_every_format(tmp_path):
    # json / yaml wrap the constructed envelope under a single "logs" key.
    for fmt in ("json", "yaml"):
        client = _client()
        _set_worker_log(client)
        result = _invoke(
            ["worker", "logs", "--worker", "W1", "-o", fmt],
            client=client,
            tmp_path=tmp_path,
        )
        assert result.exit_code == 0, result.output
        payload = (
            json.loads(result.output) if fmt == "json" else _load_yaml(result.output)
        )
        assert set(payload) == {"logs"}, payload
        assert "W1" in payload["logs"]
        assert "line-1" in payload["logs"] and "line-2" in payload["logs"]

    # the default (table) view echoes the same envelope text.
    client = _client()
    _set_worker_log(client)
    result = _invoke(
        ["worker", "logs", "--worker", "W1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert "W1" in result.output
    assert "line-1" in result.output and "line-2" in result.output


def test_worker_logs_file_dash_writes_raw_logs_to_stdout(tmp_path):
    # ``--file -`` means stdout: the raw log text is emitted, with no "Saving
    # logs to file" chatter and no file literally named "-".
    client = _client()
    _set_worker_log(client)
    result = _invoke(
        ["worker", "logs", "--worker", "W1", "--file", "-"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert "W1" in result.output
    assert "line-1" in result.output and "line-2" in result.output
    assert "Saving logs to file" not in result.output


# 26-char all-caps ids, as ``is_valid_id`` requires for ID-based picking.
_WID_1 = "A" * 25 + "1"
_WID_2 = "A" * 25 + "2"


def _picker_client(*workers: tuple[str, str]) -> MagicMock:
    """Client whose worker list resolves to full worker DTOs (the picker
    renders the whole workers table). ``get_worker_log`` echoes the worker id
    so tests can assert which one was fetched."""
    client = _client()
    client.list_workers.return_value = [
        _DTO(
            {
                "id": wid,
                "project": "proj-a",
                "status": status,
                "started_on_str": "2026-03-01",
                "ended_on_str": "2026-03-02",
            },
            collection=_DTO({"name": "coll-a"}),
            function=_DTO({"name": "fn-a"}),
            execution=_DTO({"name": "plan-a", "id": "EID_1"}),
            transaction=_DTO({"id": "TID_1"}),
            kwargs={"function_version_patch_id": None},
        )
        for wid, status in workers
    ]
    client.get_worker_log.side_effect = lambda wid: f"log-of-{wid}"
    return client


def test_run_logs_single_worker_streams_directly(tmp_path):
    client = _picker_client((_WID_1, "Done"))
    result = _invoke(
        ["fnrun", "logs", "--fnrun", "LFR_1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert f"log-of-{_WID_1}" in result.output
    # the run id was pushed into the workers filter
    sent = client.list_workers.call_args.kwargs["filter"]
    assert "logical_function_run_id:eq:LFR_1" in sent


def test_run_logs_multiple_workers_prompts_by_index(tmp_path):
    client = _picker_client((_WID_1, "Failed"), (_WID_2, "Done"))
    result = _invoke(
        ["fnrun", "logs", "--fnrun", "LFR_1"],
        client=client,
        tmp_path=tmp_path,
        input="2\n",
    )
    assert result.exit_code == 0, result.output
    # both candidates were listed, the chosen one was fetched
    assert _WID_1 in result.output and _WID_2 in result.output
    assert f"log-of-{_WID_2}" in result.output
    assert f"log-of-{_WID_1}" not in result.output


def test_plan_logs_multiple_workers_accepts_worker_id(tmp_path):
    client = _picker_client((_WID_1, "Failed"), (_WID_2, "Done"))
    result = _invoke(
        ["plan", "logs", "--plan", "EID_1"],
        client=client,
        tmp_path=tmp_path,
        input=f"{_WID_1}\n",
    )
    assert result.exit_code == 0, result.output
    assert f"log-of-{_WID_1}" in result.output
    sent = client.list_workers.call_args.kwargs["filter"]
    assert "execution_id:eq:EID_1" in sent


def test_trx_logs_multiple_workers_no_prompt_lists_and_fails(tmp_path):
    client = _picker_client((_WID_1, "Failed"), (_WID_2, "Done"))
    result = _invoke(
        ["--no-prompt", "trx", "logs", "--trx", "TID_1"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code != 0
    # the candidates are still listed, with the direct command to use
    assert _WID_1 in result.output and _WID_2 in result.output
    assert "tdk worker logs" in result.output
    client.get_worker_log.assert_not_called()


def test_run_logs_with_no_workers_errors(tmp_path):
    client = _picker_client()
    result = _invoke(
        ["fnrun", "logs", "--fnrun", "LFR_1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code != 0
    assert "no workers yet" in result.output


def test_list_status_echoes_stay_out_of_machine_output(tmp_path):
    # run/worker list echo the related plan/trx/run status in table mode only;
    # with -o json the echo must not pollute stdout (it has to stay parseable).
    client = _client()
    _set_run_list(client)
    _set_worker_list(client)
    for argv in (
        ["fnrun", "list", "--plan", "EID_1", "-o", "json"],
        ["worker", "list", "--trx", "TID_1", "-o", "json"],
    ):
        result = _invoke(argv, client=client, tmp_path=tmp_path)
        assert result.exit_code == 0, result.output
        json.loads(result.output)
    client.get_execution.assert_not_called()
    client.get_transaction.assert_not_called()


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
        got = json.loads(result.output) if fmt == "json" else _load_yaml(result.output)
        assert got == payload

    # the default (table) view prints both pinned values.
    result = _invoke(["pin", "show"], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "proj-a" in result.output and "coll-a" in result.output
