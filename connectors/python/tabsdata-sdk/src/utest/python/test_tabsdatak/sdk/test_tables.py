#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import random
from unittest.mock import MagicMock

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.sdk import _tables
from tabsdatak.sdk._apiserver import APIServerError, NotFound
from tabsdatak.sdk.tabsdata_server import DownloadTooLarge, Table, TabsdataServer


def _response(data) -> MagicMock:
    response = MagicMock()
    response.json.return_value = {"data": data}
    return response


def _page(rows: list[dict]) -> MagicMock:
    return _response({"data": rows, "next_pagination_id": None, "next": None})


def _version(id: str, created_at: int, **fields) -> dict:
    return {
        "id": id,
        "collection": "sales",
        "created_at": created_at,
        "ended_on": created_at + 10,
        "status": "C",
        "data_changed": True,
        "schema_hash": "h1",
        "function": "pub_orders",
        "execution_id": f"E_{id}",
        "row_count": 5000,
        "data_bytes": 2048,
        **fields,
    }


VERSIONS = [_version("V3", 3000), _version("V2", 2000), _version("V1", 1000)]
SCHEMA = [
    {"name": "id", "type": "i64"},
    {"name": "label", "type": "str"},
    {"name": "tags", "type": "list[str]"},
]
LISTED = {
    "name": "orders",
    "collection": "sales",
}


def _connection(versions=VERSIONS, schema=SCHEMA) -> MagicMock:
    connection = MagicMock()

    def dataversion_list(coll, table, request_filter=None, request_len=None, **kw):
        rows = list(versions)
        for condition in request_filter or []:
            field, _, value = condition.split(":", 2)
            rows = [r for r in rows if str(r.get(field)) == value]
        return _page(rows[:request_len] if request_len else rows)

    connection.dataversion_list.side_effect = dataversion_list
    connection.table_get_schema.return_value = _response({"fields": schema})
    connection.table_list.return_value = _page([LISTED])
    return connection


def _table(connection) -> Table:
    return Table(connection, "sales", "orders")


def test_stats_reads_the_newest_committed_version():
    connection = _connection()
    document = _tables.stats(_table(connection))
    assert document["version"]["id"] == "V3"
    assert document["row_count"] == 5000
    assert document["data_bytes"] == 2048
    assert document["column_count"] == 3
    assert document["schema"] == SCHEMA
    first = connection.dataversion_list.call_args_list[0].kwargs
    assert first["request_filter"] == ["status:eq:C"]
    assert first["order_by"] == "created_at-"
    assert first["request_len"] == 1
    listed = connection.table_list.call_args.kwargs
    assert listed["request_filter"] == ["name:eq:orders"]
    assert connection.table_get_schema.call_args.kwargs["at"] == 3000


def test_stats_pins_a_version_by_id_with_the_filter_grammar():
    connection = _connection()
    document = _tables.stats(_table(connection), version="V2")
    assert document["version"]["id"] == "V2"
    assert connection.dataversion_list.call_args_list[0].kwargs["request_filter"] == [
        "id:eq:V2"
    ]
    assert connection.table_get_schema.call_args.kwargs["at"] == 2000


@pytest.mark.parametrize(
    "ref, expected", [("HEAD", "V3"), ("HEAD^", "V2"), ("HEAD~2", "V1")]
)
def test_head_refs_count_back_over_committed_versions(ref, expected):
    pinned = _tables.resolve_version(_table(_connection()), ref)
    assert pinned.id == expected


def test_resolve_version_passes_the_pin_instant():
    connection = _connection()
    _tables.resolve_version(_table(connection), None, at=2500)
    assert connection.dataversion_list.call_args.kwargs["at"] == 2500


def test_an_uncommitted_version_is_not_found():
    versions = [*VERSIONS, _version("V4", 4000, status="P")]
    with pytest.raises(NotFound, match="not committed"):
        _tables.resolve_version(_table(_connection(versions)), "V4")


def test_unknown_version_is_not_found():
    with pytest.raises(NotFound, match="V9"):
        _tables.resolve_version(_table(_connection()), "V9")


def test_unlisted_table_is_not_found():
    connection = _connection()
    connection.table_list.return_value = _page([])
    with pytest.raises(NotFound, match="orders"):
        _tables.listed_row(_table(connection))
    with pytest.raises(NotFound, match="orders"):
        Table(connection, "sales", "orders")._data


def test_table_without_committed_versions_is_not_found():
    with pytest.raises(NotFound, match="no committed data version"):
        _tables.profile(_table(_connection(versions=[])))


def test_column_stats_gate_on_the_schema_type():
    assert _tables.column_stats("i64") == ("non_null", "distinct", "min", "max", "mean")
    assert _tables.column_stats("Float64")[-1] == "mean"
    assert _tables.column_stats("decimal[10,2]")[-1] == "mean"
    assert _tables.column_stats("datetime[μs, UTC]") == (
        "non_null",
        "distinct",
        "min",
        "max",
    )
    assert _tables.column_stats("str")[-1] == "max"
    assert _tables.column_stats("bool") == ("non_null", "distinct")
    assert _tables.column_stats("list[i64]") == ("non_null",)
    assert _tables.column_stats("struct[2]") == ("non_null",)
    assert _tables.column_stats("binary") == ("non_null",)


def test_profile_sql_quotes_identifiers_and_aliases_by_position():
    sql, aliases = _tables.profile_sql(
        'my"table', [{"name": 'a"b', "type": "i64"}, {"name": "t", "type": "list[i64]"}]
    )
    assert sql == (
        'SELECT COUNT(*) AS "__rows", COUNT("a""b") AS "c0__non_null", '
        'COUNT(DISTINCT "a""b") AS "c0__distinct", MIN("a""b") AS "c0__min", '
        'MAX("a""b") AS "c0__max", AVG("a""b") AS "c0__mean", '
        'COUNT("t") AS "c1__non_null" FROM "my""table"'
    )
    assert aliases[0] == ("c0__non_null", 0, "non_null")
    assert aliases[-1] == ("c1__non_null", 1, "non_null")


def _aggregate(sql: str) -> pl.DataFrame:
    values = {"__rows": 5000}
    for part in sql.split(", "):
        alias = part.rsplit(" AS ", 1)[1].strip('"').split('" FROM')[0]
        if alias.endswith("__non_null"):
            values[alias] = 4000
        elif alias.endswith("__distinct"):
            values[alias] = 7
        elif alias.endswith(("__min", "__max", "__mean")):
            values[alias] = 1
    return pl.DataFrame({k: [v] for k, v in values.items()})


def test_exact_profile_batches_columns_and_reports_null_rates(monkeypatch):
    monkeypatch.setattr(_tables, "PROFILE_BATCH", 2)
    connection = _connection()
    sqls = []

    def sample(coll, table, sql=None, len=None, **kw):
        sqls.append(sql)
        response = MagicMock()
        response.content = _parquet(_aggregate(sql))
        return response

    connection.table_get_sample.side_effect = sample
    document = _tables.profile(_table(connection))
    assert len(sqls) == 2
    assert document["approximate"] is False
    assert document["row_count"] == 5000
    assert document["version_id"] == "V3"
    by_name = {c["name"]: c for c in document["columns"]}
    assert by_name["id"] == {
        "name": "id",
        "type": "i64",
        "non_null": 4000,
        "distinct": 7,
        "min": 1,
        "max": 1,
        "mean": 1,
        "null_rate": 0.2,
    }
    assert set(by_name["tags"]) == {"name", "type", "non_null", "null_rate"}


def _parquet(frame: pl.DataFrame) -> bytes:
    import io

    buffer = io.BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()


@pytest.mark.parametrize(
    "error, reason",
    [
        (APIServerError({"code": "SampleError::2"}, status=400), "scan_cap"),
        (APIServerError({}, status=408), "timeout"),
        (APIServerError({}, status=429), "busy"),
    ],
)
def test_refused_full_scan_falls_back_to_random_windows(error, reason):
    connection = _connection()
    frame = pl.DataFrame({"id": list(range(1000)), "label": ["x"] * 1000})
    frame = frame.with_columns(tags=pl.lit(None, dtype=pl.List(pl.String)))

    def sample(coll, table, sql=None, offset=None, len=None, **kw):
        if sql:
            raise error
        response = MagicMock()
        response.content = _parquet(frame.head(len))
        return response

    connection.table_get_sample.side_effect = sample
    document = _tables.profile(_table(connection), rng=random.Random(0))
    assert document["approximate"] is True
    assert document["reason"] == reason
    assert document["sample_size"] == 1000
    by_name = {c["name"]: c for c in document["columns"]}
    assert by_name["id"]["distinct"] == 1000
    assert by_name["tags"]["null_rate"] == 1.0


def test_other_errors_are_not_masked_by_the_fallback():
    connection = _connection()
    connection.table_get_sample.side_effect = APIServerError({}, status=403)
    with pytest.raises(APIServerError):
        _tables.profile(_table(connection))


def test_profile_rejects_unknown_columns():
    with pytest.raises(_tables.UnknownColumn, match="'nope'"):
        _tables.profile(_table(_connection()), columns=["nope"])


def test_sample_windows_cover_small_tables_whole():
    assert _tables.sample_windows(2500, 3000, random.Random(0)) == [
        (0, 1000),
        (1000, 1000),
        (2000, 500),
    ]
    assert _tables.sample_windows(None, 1500, random.Random(0)) == [
        (0, 1000),
        (1000, 500),
    ]


def test_sample_windows_are_random_offsets_one_per_stratum():
    windows = _tables.sample_windows(100_000, 2500, random.Random(1))
    assert [length for _, length in windows] == [1000, 1000, 500]
    stratum = 100_000 / 3
    for index, (offset, length) in enumerate(windows):
        assert index * stratum <= offset
        assert offset + length <= (index + 1) * stratum + 1


def _edge(kind: str, **extra) -> dict:
    return {kind: {"versions": {"inner": [], "original": ""}, **extra}}


def _relation(function: str, table: str, edge: dict) -> dict:
    return {"function_version_id": function, "table_version_id": table, "edge": edge}


GRAPH = {
    "table": "T_orders",
    "all_functions": {
        "F_pub": {"collection": "raw", "name": "pub", "enabled": True},
        "F_clean": {"collection": "sales", "name": "clean", "enabled": True},
        "F_daily": {"collection": "bi", "name": "daily", "enabled": False},
    },
    "all_tables": {
        "T_raw": {"collection": "raw", "name": "orders_raw", "system": False},
        "T_orders": {"collection": "sales", "name": "orders", "system": False},
        "T_daily": {"collection": "bi", "name": "daily_sales", "system": False},
        "T_sys": {"collection": "sales", "name": "$td.sys", "system": True},
    },
    "system_tables": ["T_sys"],
    "user_tables": ["T_raw", "T_orders", "T_daily"],
    "relations": [
        _relation("F_pub", "T_raw", _edge("Output", output={"output_pos": 0})),
        _relation("F_clean", "T_raw", _edge("Dependency", dependency={"dep_pos": 0})),
        _relation("F_clean", "T_raw", _edge("Trigger")),
        _relation("F_clean", "T_orders", _edge("Output", output={"output_pos": 0})),
        _relation("F_clean", "T_sys", _edge("Output", output={"output_pos": -1})),
        _relation(
            "F_clean", "T_orders", _edge("Dependency", dependency={"dep_pos": -1})
        ),
        _relation("F_daily", "T_orders", _edge("Trigger")),
        _relation("F_daily", "T_daily", _edge("Output", output={"output_pos": 0})),
    ],
}


def test_flow_walks_both_directions_and_merges_trigger_edges():
    document = _tables.flow(GRAPH, "sales", "orders")
    assert document["focus"] == {
        "collection": "sales",
        "table": "orders",
        "table_version_id": "T_orders",
    }
    assert document["upstream"] == [
        {"collection": "raw", "table": "orders_raw", "distance": 2}
    ]
    assert document["downstream"] == [
        {"collection": "bi", "table": "daily_sales", "distance": 2}
    ]
    edges = {(e["from"], e["to"]): e for e in document["edges"]}
    assert edges[("t:T_raw", "f:F_clean")] == {
        "from": "t:T_raw",
        "to": "f:F_clean",
        "kind": "dependency",
        "trigger": True,
    }
    assert edges[("t:T_orders", "f:F_daily")]["kind"] == "trigger"
    assert edges[("f:F_clean", "t:T_orders")]["kind"] == "output"
    assert ("t:T_orders", "f:F_clean") in edges
    assert all("T_sys" not in node["id"] for node in document["nodes"])
    daily = next(n for n in document["nodes"] if n["id"] == "f:F_daily")
    assert daily == {
        "id": "f:F_daily",
        "kind": "function",
        "collection": "bi",
        "name": "daily",
        "enabled": False,
    }


def test_flow_honours_direction_depth_and_system_tables():
    up = _tables.flow(GRAPH, "sales", "orders", direction="up", depth=1)
    assert up["upstream"] == []
    assert {n["id"] for n in up["nodes"]} == {"t:T_orders", "f:F_clean"}
    down = _tables.flow(GRAPH, "sales", "orders", direction="down")
    assert down["upstream"] == []
    assert [t["table"] for t in down["downstream"]] == ["daily_sales"]
    system = _tables.flow(GRAPH, "sales", "orders", include_system=True)
    assert {"collection": "sales", "table": "$td.sys", "distance": 2} in system[
        "upstream"
    ] + system["downstream"]
    with pytest.raises(ValueError):
        _tables.flow(GRAPH, "sales", "orders", direction="sideways")


def test_diff_schemas_reports_added_removed_and_retyped():
    old = [{"name": "id", "type": "i32"}, {"name": "gone", "type": "str"}]
    new = [{"name": "id", "type": "i64"}, {"name": "discount", "type": "f64"}]
    assert _tables.diff_schemas(old, new) == {
        "added": [{"name": "discount", "type": "f64"}],
        "removed": [{"name": "gone", "type": "str"}],
        "retyped": [{"name": "id", "from": "i32", "to": "i64"}],
    }


def _history_connection(hashes: list[str | None]) -> MagicMock:
    versions = [
        _version(f"V{n}", n * 1000, schema_hash=h)
        for n, h in zip(range(len(hashes), 0, -1), hashes)
    ]
    schemas = {
        "a": [{"name": "id", "type": "i32"}],
        "b": [{"name": "id", "type": "i64"}],
        "c": [{"name": "id", "type": "i64"}, {"name": "x", "type": "str"}],
    }
    connection = _connection(versions=versions)
    at_hash = {v["created_at"]: v["schema_hash"] for v in versions}
    connection.table_get_schema.side_effect = lambda c, t, at=None, **kw: _response({
        "fields": schemas[at_hash[at]]
    })
    return connection


def test_schema_history_collapses_runs_and_reads_one_schema_per_run():
    connection = _history_connection(["c", "c", None, "b", "b", "a"])
    document = _tables.schema_history(_table(connection))
    assert document["current_schema_hash"] == "c"
    assert document["capped"] is False
    assert [e["version_id"] for e in document["events"]] == ["V5", "V2"]
    assert document["events"][0]["added"] == [{"name": "x", "type": "str"}]
    assert document["events"][1]["retyped"] == [
        {"name": "id", "from": "i32", "to": "i64"}
    ]
    assert connection.table_get_schema.call_count == 3


def test_schema_history_caps_the_events():
    connection = _history_connection(["c", "b", "a"])
    document = _tables.schema_history(_table(connection), max_events=1)
    assert document["capped"] is True
    assert [e["version_id"] for e in document["events"]] == ["V3"]


def test_schema_diff_compares_two_tables_at_their_newest_versions():
    connection = _connection()
    other = Table(connection, "bi", "orders_copy")
    connection.table_get_schema.side_effect = lambda c, t, **kw: _response({
        "fields": SCHEMA if t == "orders" else SCHEMA[:1]
    })
    document = _tables.schema_diff(_table(connection), other)
    assert document["from"] == {
        "collection": "sales",
        "table": "orders",
        "version_id": "V3",
    }
    assert document["to"]["table"] == "orders_copy"
    assert [c["name"] for c in document["removed"]] == ["label", "tags"]


def test_download_streams_and_enforces_the_byte_cap(tmp_path):
    connection = MagicMock()
    response = connection.table_download.return_value
    response.__enter__.return_value = response
    response.iter_content.return_value = [b"a" * 10, b"b" * 10]
    target = tmp_path / "t.parquet"
    assert _table(connection).download(str(target)) == 20
    assert target.read_bytes() == b"a" * 10 + b"b" * 10
    with pytest.raises(DownloadTooLarge):
        _table(connection).download(str(target), max_bytes=15)
    assert target.read_bytes() == b"a" * 10 + b"b" * 10
    assert list(tmp_path.iterdir()) == [target]


def test_project_tables_list_the_project_listing_with_rows_kept():
    server = TabsdataServer.__new__(TabsdataServer)
    server.connection = MagicMock()
    server.connection.project_table_list.return_value = _page([LISTED])
    [table] = server.list_project_tables(filter=["name:lk:ord*"])
    assert table.name == "orders"
    assert table.collection.name == "sales"
    assert table._data == LISTED
    kwargs = server.connection.project_table_list.call_args.kwargs
    assert kwargs["request_filter"] == ["name:lk:ord*"]


def test_pin_ms_reads_times_and_transaction_ends():
    from tabsdatak.sdk.tabsdata_server import Transaction

    assert _tables.pin_ms(MagicMock(), "1750074554472", None) == 1750074554472
    assert _tables.pin_ms(MagicMock(), None, None) is None
    ended = MagicMock(spec=Transaction, id="X1", ended_on=42)
    assert _tables.pin_ms(MagicMock(), None, ended) == 42
    running = MagicMock(spec=Transaction, id="X2", ended_on=None)
    with pytest.raises(ValueError, match="has not ended"):
        _tables.pin_ms(MagicMock(), None, running)
    with pytest.raises(ValueError, match="Only one"):
        _tables.pin_ms(MagicMock(), 1, "X")


class _CaptureGet:
    def __init__(self):
        self.calls = []

    def __call__(self, path, params=None, **kwargs):
        self.calls.append((path, params, kwargs))
        response = MagicMock()
        response.status_code = 200
        return response


@pytest.fixture
def captured(monkeypatch):
    from tabsdatak.sdk._apiserver import APIServer

    server = APIServer("host")
    server.project = "proj"
    capture = _CaptureGet()
    monkeypatch.setattr(server, "get", capture)
    monkeypatch.setattr(server, "raise_for_status", lambda response: response)
    return server, capture


def test_table_listings_send_the_list_parameters(captured):
    server, capture = captured
    server.project_table_list(request_filter=["name:lk:c*"], request_len=5)
    server.table_list("sales")
    assert capture.calls[0][:2] == (
        "/projects/proj/tables",
        {"len": 5, "filter": ["name:lk:c*"]},
    )
    assert capture.calls[1][:2] == ("/projects/proj/collections/sales/tables", {})


def test_table_flow_and_download_paths(captured):
    server, capture = captured
    server.table_flow("sales", "orders", at=7)
    server.table_flow("sales", "orders")
    server.table_download("sales", "orders", at=9)
    server.table_flow("sales", "orders", topology_version="TV")
    assert capture.calls[0][:2] == (
        "/projects/proj/collections/sales/tables/orders/flow",
        {"at": 7},
    )
    assert capture.calls[1][1] == {}
    assert capture.calls[3][1] == {"topology_version": "TV"}
    assert capture.calls[2] == (
        "/projects/proj/collections/sales/tables/orders/download",
        {"at": 9},
        {"stream": True},
    )


def test_project_graph_and_topology_version_paths(captured):
    server, capture = captured
    server.project_graph(topology_version="TV")
    server.project_graph(at=7)
    server.topology_version_list(
        request_filter=["valid_from:gt:0"], order_by="valid_from-"
    )
    assert capture.calls[0][:2] == ("/projects/proj/graph", {"topology_version": "TV"})
    assert capture.calls[1][:2] == ("/projects/proj/graph", {"at": 7})
    assert capture.calls[2][:2] == (
        "/projects/proj/topology-versions",
        {"filter": ["valid_from:gt:0"], "order_by": "valid_from-"},
    )


def test_get_passes_stream_to_requests(monkeypatch):
    from tabsdatak.sdk import _apiserver as a

    seen = {}

    def fake_get(url, **kwargs):
        seen.update(kwargs)
        return object()

    monkeypatch.setattr(a.GET_HTTP_SESSION, "get", fake_get)
    server = a.APIServer("http://localhost:2457")
    server.get("/x", refresh_if_needed=False)
    assert seen["stream"] is False
    server.get("/x", refresh_if_needed=False, stream=True)
    assert seen["stream"] is True
