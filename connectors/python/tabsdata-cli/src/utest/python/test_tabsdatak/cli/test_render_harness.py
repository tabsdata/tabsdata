#
#  Copyright 2026 Tabsdata Inc.
#

"""Mocked-client renderer harness for the ``tdk`` read commands.

Every ``tdk`` read command (``list`` / ``get``) shares one output contract:
``-o/--format {table,json,yaml}`` dispatched through
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
import os
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
    (e.g. ``user.name``, ``grp.defined_on_str``). One dict therefore drives
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
        "id": "GVID_A",
        "group_id": "GID_A",
        "description": "first group",
        "defined_on_str": "2026-01-01",
        "defined_by": "admin",
    },
]

_GROUP_INFO = {
    "name": "grp-a",
    "id": "GVID_A",
    "group_id": "GID_A",
    "system": False,
    "description": "first group",
    "defined_on_str": "2026-01-01",
    "defined_by": "admin",
}

_PROJECTS = [
    {
        "name": "proj-a",
        "id": "PVID_A",
        "project_id": "PID_A",
        "description": "first project",
        "defined_on_str": "2026-01-02",
        "defined_by": "admin",
    },
]

_COLLECTIONS = [
    {
        "name": "coll-a",
        "id": "CVID_A",
        "collection_id": "CID_A",
        "description": "first collection",
        "defined_on_str": "2026-01-03",
        "defined_by": "admin",
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

# ``fn get --show-history`` lists version rows; the history endpoint carries no
# per-version tables, dependencies or triggers.
_FN_VERSION = {
    "id": "FID_1",
    "patch_of": "FV_0",
    "name": "fn-a",
    "description": "publishes stuff",
    "type": "P",
    "status": "A",
    "enabled": True,
    "defined_on_str": "2026-02-01",
    "defined_by": "admin",
    "snippet": "@publisher()\ndef fn_a():\n    ...\n",
}

# ``plan get`` nests ``collection``/``function`` objects
# (``execution.collection.name``); the machine payload keeps them as dicts.
_EXECUTION = {
    "id": "ep20260301-1",
    "name": "plan-a",
    "project": "proj-a",
    "collection": {"name": "coll-a"},
    "function": {"name": "fn-a"},
    "triggered_by": "admin",
    "triggered_on_str": "2026-03-01",
    "started_on_str": "2026-03-01",
    "ended_on_str": "2026-03-02",
    "status": "Finished",
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

# ``table data`` returns a polars DataFrame: jsonb gives columns and rows,
# json/yaml ``.to_dicts()``.
_TABLE_DATA_DF = pl.DataFrame({"n": [1, 2], "label": ["x", "y"]})
_TABLE_DATA_TABULAR = {
    "columns": [
        {"name": name, "dtype": str(dtype)}
        for name, dtype in _TABLE_DATA_DF.schema.items()
    ],
    "rows": [list(row) for row in _TABLE_DATA_DF.rows()],
    "row_count": _TABLE_DATA_DF.height,
    "truncated": False,
}


def _listing(items) -> dict:
    return {"items": items, "truncated": False, "next": None}


# ``metrics show`` aggregates ``nodes`` into a summary table, one row per
# collection, group and run status: each node carries its measures per status
# in ``by_status``, as the server's NodeRow does. The machine formats emit the
# whole dict verbatim (edges included -- only the table drops them).
_METRICS = {
    "dim": "collection",
    "start_ms": 1000,
    "end_ms": 2000,
    "nodes": [
        {
            "project_id": "P1",
            "project_name": "proj-a",
            "kind": "Dim",
            "collection_id": "C1",
            "collection_name": "coll-a",
            "group_id": "G1",
            "group_name": "grp-a",
            "logical_tables_out": 1,
            "tables_unread": 0,
            "logical_tables_in": 1,
            "by_status": {
                "D": {
                    "fn_runs": 2,
                    "table_versions_out": 4,
                    "logical_tables_out": 1,
                    "versions_unconsumed": 0,
                    "row_count_out": 777,
                    "data_bytes_out": 0,
                    "table_versions_in": 3,
                    "logical_tables_in": 1,
                    "row_count_in": 555,
                    "data_bytes_in": 0,
                },
                "F": {
                    "fn_runs": 9,
                    "table_versions_out": 0,
                    "logical_tables_out": 0,
                    "versions_unconsumed": 0,
                    "row_count_out": 0,
                    "data_bytes_out": 0,
                    "table_versions_in": 1,
                    "logical_tables_in": 1,
                    "row_count_in": 333,
                    "data_bytes_in": 0,
                },
            },
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
        "id": "ep20260301-1",
        "name": "plan-a",
        "project": "proj-a",
        "collection": {"name": "coll-a"},
        "function": {"name": "fn-a"},
        "triggered_by": "admin",
        "triggered_on_str": "2026-03-01",
        "started_on_str": "2026-03-01",
        "ended_on_str": "2026-03-02",
        "trigger_type": "manual",
        "status": "Finished",
    }
]

_TRANSACTIONS = [
    {
        "id": "tx20260301-1",
        "project": "proj-a",
        "execution": {"id": "ep20260301-1"},
        "collection": {"name": "coll-a"},
        "triggered_on_str": "2026-03-01",
        "triggered_by": "admin",
        "started_on_str": "2026-03-01",
        "ended_on_str": "2026-03-02",
        "status": "Committed",
    }
]

_FUNCTION_RUNS = [
    {
        "id": "FR_1",
        "project": "proj-a",
        "collection": {"name": "coll-a"},
        "function": {"name": "fn-a"},
        "execution": {"name": "plan-a", "id": "ep20260301-1"},
        "transaction": {"id": "tx20260301-1"},
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
        "execution": {"name": "plan-a", "id": "ep20260301-1"},
        "transaction": {"id": "tx20260301-1"},
        "started_on_str": "2026-03-01",
        "ended_on_str": "2026-03-02",
        "status": "Done",
    }
]

# ``cron list`` / ``stream list``: trigger workers. json/yaml emit the raw rows
# the listing returned (``[t.kwargs ...]``), which for the stand-in DTO is the
# fixture dict itself.
_TRIGGERS = [
    {
        "id": "WK_T_1",
        "project": "proj-a",
        "collection": {"name": "coll-a"},
        "function": {"name": "fn-a"},
        "worker_type": "CT",
        "started_on_str": "2026-03-01",
        "ended_on_str": "2026-03-02",
        "status": "Running",
    }
]

# ``table versions``: json/yaml emit ``[dv._data ...]``; the table reads nested
# function/execution objects.
_DATAVERSIONS = [
    {
        "id": "DV_1",
        "function": "fn-a",
        "function_version_id": "FV_1",
        "execution_id": "ep20260301-1",
        "transaction_id": "tx20260301-1",
        "created_at_str": "2026-03-01",
        "schema_data_changed": False,
        "data_changed": True,
        "row_count": 42,
        "data_bytes": 1048576,
        "status": "C",
    }
]


def _annotated(row: dict, name: str, health: str, final: bool) -> dict:
    return {**row, "status_name": name, "health": health, "final": final}


# json/yaml rows of the status-carrying entities gain status_name/health/final.
_EXECUTION_OUT = _annotated(_EXECUTION, "Finished", "ok", True)
_EXECUTIONS_OUT = [_annotated(e, "Finished", "ok", True) for e in _EXECUTIONS]
_TRANSACTIONS_OUT = [_annotated(t, "Committed", "ok", True) for t in _TRANSACTIONS]
_FUNCTION_RUNS_OUT = [_annotated(r, "Done", "running", False) for r in _FUNCTION_RUNS]
_WORKERS_OUT = [_annotated(w, "Done", "ok", True) for w in _WORKERS]
_TRIGGERS_OUT = [_annotated(t, "Running", "running", False) for t in _TRIGGERS]
_DATAVERSIONS_OUT = [_annotated(d, "Committed", "ok", True) for d in _DATAVERSIONS]

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
    # What ``-o json|yaml`` give when it is not ``payload``: a listing's bare
    # items unless set.
    compat: Any = None


_LISTING_KEYS = {"items", "truncated", "next"}


def _compat(case: RenderCase) -> Any:
    if case.compat is not None:
        return case.compat
    if isinstance(case.payload, dict) and set(case.payload) == _LISTING_KEYS:
        return case.payload["items"]
    return case.payload


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
    c.list_tags.return_value = [{"key": "team", "value": "blue-team"}]
    # ``group get`` lists the collections whose ``group`` matches the group name.
    c.list_collections.return_value = [
        _DTO({"name": "coll-a", "group": "grp-a"}),
        _DTO({"name": "coll-z", "group": "grp-b"}),
    ]


def _set_project_list(c: MagicMock) -> None:
    _wire_listing(c, "list_projects", [_DTO(p) for p in _PROJECTS])


def _set_project_info(c: MagicMock) -> None:
    c.get_project.return_value = _DTO(_PROJECTS[0])


# ``project graph`` renders the server's ProjectGraphResponse verbatim in the
# machine formats; the table view lists the nodes with their topology tags and
# the edges, merging a dependency that also triggers and leaving system tables
# out.
def _relation(function: str, table: str, kind: str) -> dict:
    return {
        "function_version_id": function,
        "table_version_id": table,
        "edge": {kind: {"versions": "HEAD"}},
    }


_PROJECT_GRAPH = {
    "version": "TOPOV_1",
    "at": 1767225600000,
    "all_functions": {
        "FV_pub": {"collection": "raw", "name": "fn-pub", "tags": {}},
        "FV_clean": {
            "collection": "sales",
            "name": "fn-clean",
            "tags": {"_parent": "FN_ORIGIN"},
        },
    },
    "all_tables": {
        "TV_raw": {"collection": "raw", "name": "tbl-raw", "tags": {}},
        "TV_sys": {"collection": "raw", "name": "tbl-sys", "tags": {}},
    },
    "system_tables": ["TV_sys"],
    "user_tables": ["TV_raw"],
    "relations": [
        _relation("FV_pub", "TV_raw", "Output"),
        _relation("FV_pub", "TV_sys", "Output"),
        _relation("FV_clean", "TV_raw", "Dependency"),
        _relation("FV_clean", "TV_raw", "Trigger"),
    ],
    "collections": {},
    "groups": {},
}

_TOPOLOGY_VERSIONS = [
    {"id": "TOPOV_1", "valid_from": 1767225600000, "valid_to": 1767312000000},
    {"id": "TOPOV_2", "valid_from": 1767312000000, "valid_to": None},
]


def _set_project_graph(c: MagicMock) -> None:
    c.project_graph.return_value = _PROJECT_GRAPH


def _set_topology_versions(c: MagicMock) -> None:
    _wire_listing(c, "list_topology_versions", _TOPOLOGY_VERSIONS)


def _set_tag_keys(c: MagicMock) -> None:
    c.list_tag_keys.return_value = ["_parent", "team"]


def _set_tag_values(c: MagicMock) -> None:
    c.list_tag_values.return_value = ["blue-team", "red-team"]


def _set_collection_list(c: MagicMock) -> None:
    _wire_listing(c, "list_collections", [_DTO(x) for x in _COLLECTIONS])


def _set_collection_info(c: MagicMock) -> None:
    c.get_collection.return_value = _DTO(_COLLECTIONS[0])
    c.list_tags.return_value = [{"key": "stage", "value": "gold"}]


def _set_auth_info(c: MagicMock) -> None:
    c.auth_info.return_value = dict(_AUTH_INFO)
    c.connection.bearer_token = "eyJhbGciOiJIUzI1NiJ9.e30.sig"


def _set_fn_list(c: MagicMock) -> None:
    _wire_listing(c, "list_functions", [_DTO(f) for f in _FUNCTIONS])


def _set_fn_info(c: MagicMock) -> None:
    c.list_tags.return_value = [{"key": "_parent", "value": "FN_ORIGIN"}]
    c.list_project_functions_generator.return_value = [
        _DTO({"name": "fn-origin", "collection": "coll-a", "function_id": "FN_ORIGIN"})
    ]
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
                execution=_DTO({"id": "ep20260301-1"}),
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
                execution=_DTO({"name": "plan-a", "id": "ep20260301-1"}),
                transaction=_DTO({"id": "tx20260301-1"}),
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
                execution=_DTO({"name": "plan-a", "id": "ep20260301-1"}),
                transaction=_DTO({"id": "tx20260301-1"}),
                # the id/patch columns read the raw row via `.kwargs`
                kwargs={
                    "logical_function_run_id": "fr20260301-1",
                    "execution_id": "ep20260301-1",
                    "transaction_id": "tx20260301-1",
                    "function_version_patch_id": "FV_9",
                },
            )
        ],
    )


def _set_trigger_list(c: MagicMock) -> None:
    _wire_listing(
        c,
        "list_workers",
        [
            _DTO(
                _TRIGGERS[0],
                collection=_DTO({"name": "coll-a"}),
                function=_DTO({"name": "fn-a"}),
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
        execution=_DTO({"id": "ep20260301-1"}),
        collection=_DTO({"name": "coll-a"}),
    )
    _set_run_list(c)
    _set_worker_list(c)


def _set_run_info(c: MagicMock) -> None:
    c.get_function_run.return_value = _DTO(
        _FUNCTION_RUNS[0],
        collection=_DTO({"name": "coll-a"}),
        function=_DTO({"name": "fn-a"}),
        execution=_DTO({"name": "plan-a", "id": "ep20260301-1"}),
        transaction=_DTO({"id": "tx20260301-1"}),
    )
    _set_worker_list(c)


def _set_table_list(c: MagicMock) -> None:
    _wire_listing(c, "list_tables", [_DTO(_TABLES[0], function=_DTO({"name": "fn-a"}))])


def _set_table_schema(c: MagicMock) -> None:
    c.get_table_schema.return_value = _TABLE_SCHEMA


def _set_table_data(c: MagicMock) -> None:
    c.sample_table.return_value = _TABLE_DATA_DF


def _set_metrics_show(c: MagicMock) -> None:
    c.metrics.return_value = _METRICS


def _set_metrics_inflight(c: MagicMock) -> None:
    c.inflight.return_value = _INFLIGHT


def _set_fn_info_history(c: MagicMock) -> None:
    c.list_function_history.return_value = [_DTO(_FN_VERSION)]


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
        _listing(_USERS),
        ("alice", "Alice A", "alice@x.io", "True", "bob", "Bob B", "bob@x.io", "False"),
    ),
    RenderCase(
        "user-get",
        ["user", "get", "-n", "alice"],
        _set_user_info,
        _USERS[0],
        ("alice", "Alice A", "alice@x.io", "True"),
    ),
    RenderCase(
        "group-list",
        ["group", "list"],
        _set_group_list,
        _listing(_GROUPS),
        ("grp-a", "first group", "2026-01-01", "admin"),
    ),
    RenderCase(
        "group-get",
        ["group", "get", "-n", "grp-a"],
        _set_group_info,
        {**_GROUP_INFO, "tags": {"team": "blue-team"}},
        # Form view: name/description/created + the collections in the group
        # ("coll-a" belongs to grp-a; "coll-z" is in another group).
        ("grp-a", "first group", "2026-01-01", "admin", "coll-a", "team=blue-team"),
    ),
    RenderCase(
        "project-list",
        ["project", "list"],
        _set_project_list,
        _listing(_PROJECTS),
        ("proj-a", "first project", "2026-01-02", "admin"),
    ),
    RenderCase(
        "project-get",
        ["project", "get", "-n", "proj-a"],
        _set_project_info,
        _PROJECTS[0],
        ("proj-a", "first project", "2026-01-02", "admin"),
    ),
    RenderCase(
        "project-graph",
        ["project", "graph"],
        _set_project_graph,
        _PROJECT_GRAPH,
        (
            "TOPOV_1",
            "2026-01-01T00:00:00Z",
            "function raw/fn-pub",
            "_parent=FN_ORIGIN",
            "table raw/tbl-raw",
            "dependency, trigger",
            "System tables left out: 1",
        ),
    ),
    RenderCase(
        "project-topology-list",
        ["project", "topology", "list"],
        _set_topology_versions,
        _listing(_TOPOLOGY_VERSIONS),
        ("TOPOV_1", "2026-01-02T00:00:00Z", "TOPOV_2", "current"),
    ),
    RenderCase(
        "project-tag-keys",
        ["project", "tag", "keys"],
        _set_tag_keys,
        _listing(["_parent", "team"]),
        ("_parent", "team"),
    ),
    RenderCase(
        "project-tag-values",
        ["project", "tag", "values", "--key", "team"],
        _set_tag_values,
        _listing(["blue-team", "red-team"]),
        ("team", "blue-team", "red-team"),
    ),
    RenderCase(
        "collection-list",
        ["collection", "list"],
        _set_collection_list,
        _listing(_COLLECTIONS),
        ("coll-a", "first collection", "2026-01-03", "admin"),
    ),
    RenderCase(
        "collection-get",
        ["collection", "get", "-n", "coll-a"],
        _set_collection_info,
        {**_COLLECTIONS[0], "tags": {"stage": "gold"}},
        ("coll-a", "first collection", "2026-01-03", "admin", "stage=gold"),
    ),
    RenderCase(
        # ``roles`` renders via beautify_list; assert the distinctive role value
        # ("user" is too generic -- it also appears in the table title).
        "auth-info",
        ["auth", "info"],
        _set_auth_info,
        {**_AUTH_INFO, "credential": "session"},
        ("alice", "alice@x.io", "sys_admin"),
    ),
    RenderCase(
        # raw type "P" is mapped to "Publisher" in the table view.
        "fn-list",
        ["fn", "list", "-c", "coll-a"],
        _set_fn_list,
        _listing(_FUNCTIONS),
        ("fn-a", "Publisher", "publishes stuff", "2026-02-01", "admin"),
    ),
    RenderCase(
        "fn-get",
        ["fn", "get", "-n", "fn-a", "-c", "coll-a"],
        _set_fn_info,
        {
            **_FN_INFO,
            "tags": {"_parent": "FN_ORIGIN"},
            "parent": {
                "function_id": "FN_ORIGIN",
                "collection": "coll-a",
                "name": "fn-origin",
            },
        },
        # Form view: no ID row; project/collection/status(->"Active") are shown.
        (
            "fn-a",
            "proj-a",
            "coll-a",
            "publishes stuff",
            "2026-02-01",
            "admin",
            "Active",
            "dep1",
            "dep2",
            "trig1",
            "t1",
            "t2",
            "_parent=FN_ORIGIN",
            "coll-a/fn-origin",
        ),
    ),
    RenderCase(
        # plan get builds its tables inline for the table format and hands a
        # combined {plan, function_runs, workers} payload to render for json/yaml.
        "plan-get",
        ["plan", "get", "--plan", "ep20260301-1"],
        _set_plan_info,
        {
            "plan": _EXECUTION_OUT,
            "function_runs": _FUNCTION_RUNS_OUT,
            "workers": _WORKERS_OUT,
        },
        (
            "plan-a",
            "proj-a",
            "coll-a",
            "fn-a",
            "2026-03-01",
            "Done",
            "FR_1",
            "WK_1",
            "tx20260301-1",
        ),
    ),
    RenderCase(
        "trx-get",
        ["trx", "get", "--trx", "tx20260301-1"],
        _set_trx_info,
        {
            "transaction": _TRANSACTIONS_OUT[0],
            "function_runs": _FUNCTION_RUNS_OUT,
            "workers": _WORKERS_OUT,
        },
        (
            "tx20260301-1",
            "proj-a",
            "coll-a",
            "2026-03-01",
            "admin",
            "Committed",
            "FR_1",
            "WK_1",
        ),
    ),
    RenderCase(
        "fnrun-get",
        ["fnrun", "get", "--fnrun", "fr20260301-1"],
        _set_run_info,
        {"function_run": _FUNCTION_RUNS_OUT[0], "workers": _WORKERS_OUT},
        (
            "fr20260301-1",
            "proj-a",
            "coll-a",
            "fn-a",
            "plan-a",
            "ep20260301-1",
            "tx20260301-1",
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
        _listing(_TABLES),
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
        # table data's "table" view is just the polars frame's repr, which
        # carries the same rows json/yaml get from ``.to_dicts()``.
        "table-data",
        ["table", "data", "-c", "coll-a", "--name", "tbl-a"],
        _set_table_data,
        _TABLE_DATA_TABULAR,
        ("tbl-a", "label", "x", "y"),
        _TABLE_DATA_DF.to_dicts(),
    ),
    RenderCase(
        # rows_in/out are 555/777 -- distinctive values that can't be confused
        # with the window bounds (1000/2000) echoed in the summary header.
        "metrics-show",
        ["metrics", "show"],
        _set_metrics_show,
        _METRICS,
        ("coll-a", "grp-a", "555", "777", "333"),
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
        _set_fn_info_history,
        _listing([_FN_VERSION]),
        ("fn-a", "FID_1", "FV_0", "publishes stuff", "Active", "admin", "2026-02-01"),
    ),
    RenderCase(
        # epoch defined_on (1704067200000) is kept raw in json/yaml but rendered
        # as the UTC ISO string in the table.
        "fn-patches",
        ["fn", "patches", "-n", "fn-a", "-c", "coll-a"],
        _set_fn_list_patches,
        _listing(_PATCHES),
        ("PV_2", "PV_1", "second patch", "2024-01-01T00:00:00Z", "published"),
    ),
    RenderCase(
        # plan/trx/run/worker list emit [x._data ...] for json/yaml and build
        # the table inline; the Project column always renders.
        "plan-list",
        ["plan", "list"],
        _set_plan_list,
        _listing(_EXECUTIONS_OUT),
        (
            "ep20260301-1",
            "plan-a",
            "proj-a",
            "coll-a",
            "fn-a",
            "2026-03-01",
            "admin",
            "Finished",
        ),
    ),
    RenderCase(
        "trx-list",
        ["trx", "list"],
        _set_trx_list,
        _listing(_TRANSACTIONS_OUT),
        ("tx20260301-1", "ep20260301-1", "proj-a", "coll-a", "2026-03-01", "admin", "Committed"),
    ),
    RenderCase(
        "fnrun-list",
        ["fnrun", "list"],
        _set_run_list,
        _listing(_FUNCTION_RUNS_OUT),
        (
            "FR_1",
            "proj-a",
            "coll-a",
            "fn-a",
            "ep20260301-1",
            "tx20260301-1",
            "2026-03-01",
            "2026-03-02",
            "Done",
        ),
    ),
    RenderCase(
        "worker-list",
        ["worker", "list"],
        _set_worker_list,
        _listing(_WORKERS_OUT),
        (
            "WK_1",
            "fr20260301-1",
            "proj-a",
            "coll-a",
            "fn-a",
            "ep20260301-1",
            "tx20260301-1",
            "Done",
            "FV_9",
        ),
    ),
    RenderCase(
        "cron-list",
        ["cron", "list"],
        _set_trigger_list,
        _listing(_TRIGGERS_OUT),
        ("WK_T_1", "coll-a", "fn-a", "Running", "2026-03-01", "2026-03-02"),
    ),
    RenderCase(
        "stream-list",
        ["stream", "list"],
        _set_trigger_list,
        _listing(_TRIGGERS_OUT),
        ("WK_T_1", "coll-a", "fn-a", "Running", "2026-03-01", "2026-03-02"),
    ),
    RenderCase(
        "cron-plans",
        ["cron", "plans"],
        _set_plan_list,
        _listing(_EXECUTIONS_OUT),
        (
            "ep20260301-1",
            "plan-a",
            "proj-a",
            "coll-a",
            "fn-a",
            "2026-03-01",
            "admin",
            "Finished",
        ),
    ),
    RenderCase(
        "stream-plans",
        ["stream", "plans"],
        _set_plan_list,
        _listing(_EXECUTIONS_OUT),
        (
            "ep20260301-1",
            "plan-a",
            "proj-a",
            "coll-a",
            "fn-a",
            "2026-03-01",
            "admin",
            "Finished",
        ),
    ),
    RenderCase(
        "table-versions",
        ["table", "versions", "-c", "coll-a", "--name", "tbl-a"],
        _set_table_versions,
        _listing(_DATAVERSIONS_OUT),
        ("DV_1", "fn-a", "FV_1", "ep20260301-1", "tx20260301-1", "2026-03-01", "42", "1.00"),
    ),
    RenderCase(
        # payload is built from attributes; a None project is null (json) / "-" (table).
        "permission-list-user",
        ["permission", "list", "--user", "alice"],
        _set_perm_list_user,
        _listing(_PERMS_BY_USER),
        ("read_collection", "proj-a", "manage_system"),
    ),
    RenderCase(
        "permission-list-project",
        ["permission", "list", "--proj", "proj-a"],
        _set_perm_list_project,
        _listing(_ACCESS_BY_PROJECT),
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
def test_jsonb_output_matches_payload(case: RenderCase, tmp_path):
    client = _client()
    case.setup(client)
    result = _invoke(case.argv + ["-o", "jsonb"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _normalize(case.payload)


@pytest.mark.parametrize("case", CASES, ids=_IDS)
def test_json_output_keeps_the_compat_shape(case: RenderCase, tmp_path):
    client = _client()
    case.setup(client)
    result = _invoke(case.argv + ["-o", "json"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _normalize(_compat(case))


@pytest.mark.parametrize("case", CASES, ids=_IDS)
def test_yaml_output_keeps_the_compat_shape(case: RenderCase, tmp_path):
    client = _client()
    case.setup(client)
    result = _invoke(case.argv + ["-o", "yaml"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert _load_yaml(result.output) == _normalize(_compat(case))


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
    """``-o`` is a fixed Choice(table, json, jsonb, yaml); anything else is a usage
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
# Their payload is not a DTO: the command renders the SDK's worker log entries
# as the list envelope ``{"items", "truncated", "next"}`` in jsonb and as
# ``{"logs": text}`` in json and yaml; the table view
# echoes a "Worker <id> ..." header and the log. Pinning the exact header
# punctuation would duplicate production formatting and be brittle, so we
# assert the worker id and log body are *present* in the table view instead.
# The plan/trx/run variants resolve the worker via the picker: a single worker
# streams directly; several render an indexed listing and prompt for a choice.
# ---------------------------------------------------------------------------


def _log_entry(worker_id: str, log: str) -> dict:
    return {
        "worker_id": worker_id,
        "function_run_id": None,
        "collection": None,
        "function": None,
        "status": None,
        "status_name": None,
        "health": None,
        "attempt": None,
        "lines_total": len(log.splitlines()),
        "log_truncated": False,
        "log_unavailable": None,
        "signature": None,
        "log": log,
    }


def _set_worker_log(c: MagicMock) -> None:
    c.worker_log_entry.return_value = _log_entry("W1", "line-1\nline-2")


def test_worker_logs_carries_worker_log_in_every_format(tmp_path):
    envelope = {
        "items": [_log_entry("W1", "line-1\nline-2")],
        "truncated": False,
        "next": None,
    }
    compat = {"logs": f"Worker W1{os.linesep}line-1\nline-2"}
    for fmt, load, expected in (
        ("jsonb", json.loads, envelope),
        ("json", json.loads, compat),
        ("yaml", _load_yaml, compat),
    ):
        client = _client()
        _set_worker_log(client)
        result = _invoke(
            ["worker", "logs", "--worker", "wkr20260301-1", "-o", fmt],
            client=client,
            tmp_path=tmp_path,
        )
        assert result.exit_code == 0, result.output
        assert load(result.output) == expected, fmt

    # the default (table) view echoes the same envelope text.
    client = _client()
    _set_worker_log(client)
    result = _invoke(
        ["worker", "logs", "--worker", "wkr20260301-1"], client=client, tmp_path=tmp_path
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
        ["worker", "logs", "--worker", "wkr20260301-1", "--file", "-"],
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
            execution=_DTO({"name": "plan-a", "id": "ep20260301-1"}),
            transaction=_DTO({"id": "tx20260301-1"}),
            kwargs={"function_version_patch_id": None},
        )
        for wid, status in workers
    ]
    client.worker_log_entry.side_effect = lambda worker, **_: _log_entry(
        worker.id, f"log-of-{worker.id}"
    )
    return client


def test_run_logs_single_worker_streams_directly(tmp_path):
    client = _picker_client((_WID_1, "Done"))
    result = _invoke(
        ["fnrun", "logs", "--fnrun", "fr20260301-1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert f"log-of-{_WID_1}" in result.output
    # the run id was pushed into the workers filter
    sent = client.list_workers.call_args.kwargs["filter"]
    assert "logical_function_run_id:eq:fr20260301-1" in sent


def test_run_logs_multiple_workers_prompts_by_index(tmp_path):
    client = _picker_client((_WID_1, "Failed"), (_WID_2, "Done"))
    result = _invoke(
        ["fnrun", "logs", "--fnrun", "fr20260301-1"],
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
        ["plan", "logs", "--plan", "ep20260301-1"],
        client=client,
        tmp_path=tmp_path,
        input=f"{_WID_1}\n",
    )
    assert result.exit_code == 0, result.output
    assert f"log-of-{_WID_1}" in result.output
    sent = client.list_workers.call_args.kwargs["filter"]
    assert "execution_id:eq:ep20260301-1" in sent


def test_trx_logs_multiple_workers_no_prompt_lists_and_fails(tmp_path):
    client = _picker_client((_WID_1, "Failed"), (_WID_2, "Done"))
    result = _invoke(
        ["--no-prompt", "trx", "logs", "--trx", "tx20260301-1"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code != 0
    # the candidates are still listed, with the direct command to use
    assert _WID_1 in result.output and _WID_2 in result.output
    assert "tdk worker logs" in result.output
    client.worker_log_entry.assert_not_called()


def test_run_logs_with_no_workers_errors(tmp_path):
    client = _picker_client()
    result = _invoke(
        ["fnrun", "logs", "--fnrun", "fr20260301-1"], client=client, tmp_path=tmp_path
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
        ["fnrun", "list", "--plan", "ep20260301-1", "-o", "json"],
        ["worker", "list", "--trx", "tx20260301-1", "-o", "json"],
    ):
        result = _invoke(argv, client=client, tmp_path=tmp_path)
        assert result.exit_code == 0, result.output
        json.loads(result.output)
    client.get_execution.assert_not_called()
    client.get_transaction.assert_not_called()


def test_pinned_collection_notice_stays_out_of_machine_output(tmp_path):
    (tmp_path / "options.json").write_text(
        json.dumps({"pins": {"collection": "coll-a"}})
    )
    client = _client()
    _set_collection_info(client)
    result = _invoke(
        ["collection", "get", "-o", "json"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == _normalize(
        {**_COLLECTIONS[0], "tags": {"stage": "gold"}}
    )
    assert "Using currently pinned collection: coll-a" in result.stderr


def test_fn_get_keeps_the_id_of_a_parent_the_project_no_longer_lists(tmp_path):
    client = _client()
    _set_fn_info(client)
    client.list_project_functions_generator.return_value = []
    result = _invoke(
        ["fn", "get", "-n", "fn-a", "-c", "coll-a", "-o", "json"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["parent"] == {"function_id": "FN_ORIGIN"}


# ---------------------------------------------------------------------------
# ``settings`` -- kept out of the matrix.
#
# It is fully offline: it renders the persisted settings (loaded from
# ``tdk-settings.json`` and ``options.json`` in the tabsdata dir) and never
# calls the server. So instead of a mock client we seed the pins file in the
# patched tabsdata dir.
# ---------------------------------------------------------------------------


def test_settings_renders_settings_store_offline(tmp_path):
    (tmp_path / "options.json").write_text(
        json.dumps({"pins": {"project": "proj-a", "collection": "coll-a"}})
    )
    payload = {
        "profile": None,
        "hints": "enabled",
        "prompt": "enabled",
        "trigger_wait": "enabled",
        "server": None,
        "pins": {"project": "proj-a", "collection": "coll-a"},
    }

    for fmt in ("json", "yaml"):
        result = _invoke(["settings", "-o", fmt], client=_client(), tmp_path=tmp_path)
        assert result.exit_code == 0, result.output
        got = json.loads(result.output) if fmt == "json" else _load_yaml(result.output)
        assert got == payload

    # the default (table) view prints both pinned values.
    result = _invoke(["settings"], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "proj-a" in result.output and "coll-a" in result.output


@pytest.mark.parametrize(
    "options, named",
    [
        (["--wrap", "-o", "json"], "--wrap"),
        (["-o", "yaml", "--max-width", "10"], "--max-width"),
        (["--print", "-o", "json"], "--print"),
        (["-o", "json", "--browse", "--wrap"], "--browse"),
    ],
)
def test_table_data_view_options_need_the_table_format(tmp_path, options, named):
    client = _client()
    _set_table_data(client)
    argv = ["table", "data", "-c", "coll-a", "--name", "tbl-a", *options]

    result = _invoke(argv, client=client, tmp_path=tmp_path)

    assert result.exit_code == 2
    assert f"The '{named}' option only applies to '-o table'." in " ".join(
        result.output.split()
    )
    client.sample_table.assert_not_called()


def test_table_data_wide_is_a_deprecated_max_width_0(tmp_path):
    client = _client()
    _set_table_data(client)
    argv = ["table", "data", "-c", "coll-a", "--name", "tbl-a", "--wide"]

    with patch("tabsdatak.common._tabular.show") as show:
        result = _invoke([*argv, "--print"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert show.call_args.args[1].max_width == 0
    assert "'--wide' is deprecated" in result.stderr

    result = _invoke([*argv, "-o", "json"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == _TABLE_DATA_DF.to_dicts()


@pytest.mark.parametrize(
    "argv, call",
    [
        (["table", "data", "-c", "coll-a", "--name", "tbl-a"], "sample_table"),
        (["table", "schema", "-c", "coll-a", "--name", "tbl-a"], "get_table_schema"),
    ],
)
def test_table_reads_say_when_the_table_holds_no_data(tmp_path, argv, call):
    client = _client()
    getattr(client, call).return_value = None

    result = _invoke(argv, client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "The table holds no data at that version" in result.output

    result = _invoke([*argv, "-o", "jsonb"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) is None

    result = _invoke([*argv, "-o", "json"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == []


@pytest.mark.parametrize(
    "rows, served",
    [
        (pl.DataFrame({"a": [1]}), "showing data from an earlier committed version"),
        (None, "no earlier committed version holds data"),
    ],
)
def test_table_data_warns_when_the_version_is_not_committed(tmp_path, rows, served):
    client = _client()
    client.sample_table.return_value = rows
    client.list_dataversions.return_value = [MagicMock(status="Failed")]

    argv = ["table", "data", "-c", "coll-a", "--name", "tbl-a", "--version", "dv-6"]
    result = _invoke([*argv, "-o", "jsonb"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert f"Warning: version dv-6 failed; {served}" in result.stderr
    expected = (
        None
        if rows is None
        else {
            "columns": [{"name": "a", "dtype": "Int64"}],
            "rows": [[1]],
            "row_count": 1,
            "truncated": False,
        }
    )
    assert json.loads(result.stdout) == expected


def test_table_data_does_not_warn_for_a_committed_version(tmp_path):
    client = _client()
    client.sample_table.return_value = pl.DataFrame({"a": [1]})
    client.list_dataversions.return_value = [MagicMock(status="Committed")]

    argv = ["table", "data", "-c", "coll-a", "--name", "tbl-a", "--version", "dv-6"]
    result = _invoke(argv, client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "Warning" not in result.stderr


def test_fn_patches_names_its_author_only_from_defined_by(tmp_path):
    client = _client()
    row = {**_PATCHES[0], "defined_by": None, "defined_by_id": "UID_AUTHOR"}
    _wire_listing(client, "list_function_patches", [row])
    result = _invoke(
        ["fn", "patches", "-n", "fn-a", "-c", "coll-a"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert "UID_AUTHOR" not in result.output


def test_metrics_show_sums_each_status_across_nodes(tmp_path):
    client = _client()
    node = _METRICS["nodes"][0]
    client.metrics.return_value = {**_METRICS, "nodes": [node, node]}
    result = _invoke(["metrics", "show"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "1554" in result.output
    assert "666" in result.output
    assert "Txns" not in result.output
    assert "Unit" in result.output


def test_project_graph_passes_its_pin(tmp_path):
    client = _client()
    _set_project_graph(client)
    version = "0" * 26
    result = _invoke(
        ["project", "graph", "--topology-version", version, "-o", "json"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    client.project_graph.assert_called_once_with(topology_version=version, at=None)


@pytest.mark.parametrize(
    "pin",
    [
        ["--topology-version", "0" * 26, "--at", "2026-01-01"],
        ["--topology-version", "not-an-id"],
    ],
)
def test_project_graph_rejects_a_bad_pin(pin, tmp_path):
    client = _client()
    result = _invoke(["project", "graph", *pin], client=client, tmp_path=tmp_path)
    assert result.exit_code == 2, result.output
    client.project_graph.assert_not_called()


def test_project_topology_list_passes_paging(tmp_path):
    client = _client()
    _set_topology_versions(client)
    result = _invoke(
        ["project", "topology", "list", "--max-result", "1", "-o", "jsonb"]
        + ["--filter", "valid_from:gt:0", "--order-by", "valid_from-"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    kwargs = client.list_topology_versions_generator.call_args.kwargs
    assert kwargs["filter"] == ["valid_from:gt:0"]
    assert kwargs["order_by"] == "valid_from-"
    document = json.loads(result.stdout)
    assert document["items"] == _TOPOLOGY_VERSIONS[:1]
    assert document["truncated"] is True


def test_project_tag_facets_ask_for_the_server_maximum(tmp_path):
    client = _client()
    _set_tag_keys(client)
    _set_tag_values(client)
    for argv in (["keys"], ["values", "-k", "team"]):
        result = _invoke(["project", "tag", *argv], client=client, tmp_path=tmp_path)
        assert result.exit_code == 0, result.output
    client.list_tag_keys.assert_called_once_with(len=1000)
    client.list_tag_values.assert_called_once_with("team", len=1000)


@pytest.mark.parametrize("entity", ["collection", "group", "fn", "table"])
def test_only_the_project_tag_group_has_facets(entity):
    tag = cli.commands[entity].commands["tag"]
    assert sorted(tag.commands) == ["delete", "list", "set"]
