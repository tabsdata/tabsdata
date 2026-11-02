#
# Copyright 2026 Tabsdata Inc.
#

"""Table analytics behind :class:`tabsdatak.sdk.tabsdata_server.Table`: stats,
column profiles, the lineage graph and schema history, built from
the table listings, data versions, schemas, samples and the flow graph."""

from __future__ import annotations

import base64
import datetime
import decimal
import math
import random
import re
from collections import defaultdict, deque
from typing import TYPE_CHECKING

from tabsdatak.sdk._apiserver import APIServerError, NotFound

if TYPE_CHECKING:
    import polars as pl

    from tabsdatak.sdk._apiserver import APIServer
    from tabsdatak.sdk.tabsdata_server import DataVersion, Table, Transaction

COMMITTED = "status:eq:C"
PROFILE_BATCH = 32
SAMPLE_WINDOW = 1000

_HEAD = re.compile(r"HEAD(?:(?P<carets>\^+)|~(?P<back>\d+))?")
_NUMERIC = re.compile(r"(?:u?int\d+|float\d+|[iuf]\d+|decimal\b.*)", re.IGNORECASE)
_ORDERED = re.compile(
    r"(?:date|datetime\b.*|time|duration\b.*|str|string|utf8)", re.IGNORECASE
)
_COUNTABLE = re.compile(r"(?:bool|boolean|cat|categorical\b.*|enum\b.*)", re.IGNORECASE)
_SCAN_CAP_CODE = "SampleError::2"
_APPROX_STATUSES = {408: "timeout", 429: "busy"}


class UnknownColumn(ValueError):
    """A column asked for by name that the table's schema does not have."""


def pin_ms(
    connection: APIServer,
    at: int | str | None,
    at_trx: Transaction | str | None,
) -> int | None:
    """The instant ``at`` or the end of ``at_trx`` names, in epoch ms."""
    from tabsdatak.sdk.tabsdata_server import Transaction, _top_and_convert_to_timestamp

    if at is not None and at_trx is not None:
        raise ValueError("Only one of 'at' or 'at_trx' can be provided at a time.")
    if at is not None:
        return _top_and_convert_to_timestamp(at) if isinstance(at, str) else at
    if at_trx is None:
        return None
    if not isinstance(at_trx, Transaction):
        at_trx = Transaction(connection, at_trx)
    if at_trx.ended_on is None:
        raise ValueError(f"Transaction '{at_trx.id}' has not ended yet.")
    return at_trx.ended_on


def _data_version(table: Table, row: dict) -> DataVersion:
    from tabsdatak.sdk.tabsdata_server import DataVersion

    version = DataVersion(table.connection, table=table, **row)
    version._data = row
    return version


def resolve_version(
    table: Table,
    version: DataVersion | str | None = None,
    at: int | None = None,
) -> DataVersion | None:
    """The data version a read pins: ``version`` itself (the id of a
    committed version, or ``HEAD``, ``HEAD^``, ``HEAD~N`` counted back over
    the committed versions as of ``at``), else the newest committed version
    as of ``at``. ``None`` when the table has no such committed version;
    :class:`NotFound` for an id that is unknown or not committed, since
    the server reads a version by its instant, not by its id."""
    from tabsdatak.sdk.tabsdata_server import DataVersion

    if isinstance(version, DataVersion):
        return version
    back = 0
    if version is not None:
        head = _HEAD.fullmatch(version)
        if head is None:
            rows = _versions(table, [f"id:eq:{version}"], None, 1, None)
            if not rows:
                raise NotFound(
                    f"Data version '{version}' not found for table '{table.name}' "
                    f"in collection '{table.collection.name}'."
                )
            status = rows[0].get("status")
            if status != "C":
                raise NotFound(
                    f"Data version '{version}' of table '{table.name}' in "
                    f"collection '{table.collection.name}' is not committed "
                    f"(status {status}); only committed versions can be read."
                )
            return _data_version(table, rows[0])
        back = len(head["carets"] or "") or int(head["back"] or 0)
    rows = _versions(table, [COMMITTED], "created_at-", back + 1, at)
    return _data_version(table, rows[back]) if len(rows) > back else None


def _versions(table: Table, filters, order_by, length, at) -> list[dict]:
    response = table.connection.dataversion_list(
        table.collection.name,
        table.name,
        request_len=length,
        request_filter=filters,
        order_by=order_by,
        at=at,
    )
    return (response.json().get("data") or {}).get("data") or []


def version_summary(version: DataVersion | None) -> dict | None:
    if version is None:
        return None
    row = version._data or {}
    return {
        "id": version.id,
        "created_at": row.get("created_at"),
        "ended_on": row.get("ended_on"),
        "status": row.get("status"),
        "data_changed": row.get("data_changed"),
        "schema_hash": row.get("schema_hash"),
        "function": row.get("function"),
        "execution_id": row.get("execution_id"),
    }


def listed_row(table: Table) -> dict:
    response = table.connection.table_list(
        table.collection.name,
        request_filter=[f"name:eq:{table.name}"],
    )
    rows = (response.json().get("data") or {}).get("data") or []
    if not rows:
        raise NotFound(
            f"Table '{table.name}' not found in collection '{table.collection.name}'."
        )
    return rows[0]


def stats(
    table: Table,
    version: DataVersion | str | None = None,
    at: int | str | None = None,
    at_trx: Transaction | str | None = None,
) -> dict:
    listed_row(table)
    pinned = resolve_version(table, version, pin_ms(table.connection, at, at_trx))
    schema = (table.get_schema(version=pinned) or []) if pinned is not None else []
    row = (pinned._data or {}) if pinned is not None else {}
    return {
        "collection": table.collection.name,
        "table": table.name,
        "version": version_summary(pinned),
        "row_count": row.get("row_count"),
        "column_count": len(schema),
        "data_bytes": row.get("data_bytes"),
        "schema": schema,
    }


def json_safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode("ascii")
    return value


def column_stats(type_name: str) -> tuple[str, ...]:
    """What a profile computes for a column of this schema type: every column
    gets ``non_null``; nested, binary and null columns get nothing else."""
    if _NUMERIC.fullmatch(type_name):
        return ("non_null", "distinct", "min", "max", "mean")
    if _ORDERED.fullmatch(type_name):
        return ("non_null", "distinct", "min", "max")
    if _COUNTABLE.fullmatch(type_name):
        return ("non_null", "distinct")
    return ("non_null",)


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


_AGGREGATES = {
    "non_null": "COUNT({})",
    "distinct": "COUNT(DISTINCT {})",
    "min": "MIN({})",
    "max": "MAX({})",
    "mean": "AVG({})",
}


def profile_sql(
    table_name: str, columns: list[dict], offset: int = 0
) -> tuple[str, list[tuple[str, int, str]]]:
    """The aggregate query profiling ``columns`` in one server-side scan, and
    the ``(alias, column index, stat)`` each result column holds. Aliases are
    positional (``c<index>__<stat>``), so no column name reaches them."""
    selected = ['COUNT(*) AS "__rows"']
    aliases = []
    for index, column in enumerate(columns, start=offset):
        for stat in column_stats(column["type"]):
            alias = f"c{index}__{stat}"
            selected.append(
                f'{_AGGREGATES[stat].format(quote(column["name"]))} AS "{alias}"'
            )
            aliases.append((alias, index, stat))
    return f"SELECT {', '.join(selected)} FROM {quote(table_name)}", aliases


def _with_rates(entry: dict, rows: int | None) -> dict:
    non_null = entry.get("non_null")
    entry["null_rate"] = (
        None
        if rows is None or non_null is None
        else (round(1 - non_null / rows, 6) if rows else 0.0)
    )
    return entry


def _exact_profile(table, version, columns) -> tuple[int | None, list[dict]]:
    entries = [{"name": c["name"], "type": c["type"]} for c in columns]
    rows = None
    for start in range(0, len(columns), PROFILE_BATCH):
        sql, aliases = profile_sql(
            table.name, columns[start : start + PROFILE_BATCH], start
        )
        frame = table.sample(version=version, len=1, sql=sql)
        result = frame.row(0, named=True) if frame is not None and frame.height else {}
        rows = result.get("__rows", rows)
        for alias, index, stat in aliases:
            entries[index][stat] = json_safe(result.get(alias))
    return rows, [_with_rates(entry, rows) for entry in entries]


def sample_windows(
    row_count: int | None, sample_rows: int, rng: random.Random
) -> list[tuple[int, int]]:
    """The ``(offset, len)`` windows an approximate profile reads: the whole
    table when it holds at most ``sample_rows`` rows, else one window of at
    most :data:`SAMPLE_WINDOW` rows at a random offset in each of as many
    equal strata of the table, so append-ordered data is not read head
    first."""
    if row_count is None or row_count <= sample_rows:
        total = sample_rows if row_count is None else row_count
        return [
            (offset, min(SAMPLE_WINDOW, total - offset))
            for offset in range(0, total, SAMPLE_WINDOW)
        ]
    count = math.ceil(sample_rows / SAMPLE_WINDOW)
    stratum = row_count / count
    windows = []
    left = sample_rows
    for index in range(count):
        length = min(SAMPLE_WINDOW, left, int(stratum))
        low = int(index * stratum)
        high = max(low, int((index + 1) * stratum) - length)
        windows.append((rng.randint(low, high), length))
        left -= length
    return windows


def _frame_stats(frame: pl.DataFrame, column: dict) -> dict:
    series = frame[column["name"]]
    entry = {"name": column["name"], "type": column["type"]}
    for stat in column_stats(column["type"]):
        if stat == "non_null":
            value = series.len() - series.null_count()
        elif stat == "distinct":
            value = series.drop_nulls().n_unique()
        else:
            value = getattr(series, stat)()
        entry[stat] = json_safe(value)
    return entry


def _approx_profile(table, version, columns, row_count, sample_rows, rng):
    import polars as pl

    frames = []
    for offset, length in sample_windows(row_count, sample_rows, rng):
        frame = table.sample(version=version, offset=offset, len=length)
        if frame is None:
            break
        frames.append(frame.select([c["name"] for c in columns]))
        if frame.height < length:
            break
    sample = pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame()
    size = sample.height
    entries = [_with_rates(_frame_stats(sample, c), size) for c in columns]
    return size, entries


def _approx_reason(error) -> str | None:
    if not isinstance(error, APIServerError):
        return None
    if error.code == _SCAN_CAP_CODE:
        return "scan_cap"
    return _APPROX_STATUSES.get(error.status)


def profile(
    table: Table,
    columns: list[str] | None = None,
    version: DataVersion | str | None = None,
    at: int | str | None = None,
    at_trx: Transaction | str | None = None,
    approx: bool = False,
    sample_rows: int = SAMPLE_WINDOW,
    rng: random.Random | None = None,
) -> dict:
    """Per-column non-null and distinct counts, ranges and means of a data
    version. Exact by default: aggregate queries over the whole version,
    :data:`PROFILE_BATCH` columns each. With ``approx``, or when the server
    refuses the full scan (too big, timed out or busy), the stats come from
    random windows of ``sample_rows`` rows in all and are labelled
    ``approximate`` unless the sample holds every row."""
    pinned = resolve_version(table, version, pin_ms(table.connection, at, at_trx))
    if pinned is None:
        raise NotFound(
            f"Table '{table.name}' in collection '{table.collection.name}' has no "
            "committed data version to profile."
        )
    schema = table.get_schema(version=pinned)
    if schema is None:
        raise NotFound(
            f"Table '{table.name}' in collection '{table.collection.name}' holds "
            "no data at that version to profile."
        )
    if columns:
        known = {c["name"]: c for c in schema}
        missing = [name for name in columns if name not in known]
        if missing:
            raise UnknownColumn(
                f"Table '{table.name}' has no column {', '.join(map(repr, missing))}."
            )
        schema = [known[name] for name in columns]
    row_count = (pinned._data or {}).get("row_count")
    result = {
        "collection": table.collection.name,
        "table": table.name,
        "version_id": pinned.id,
    }
    reason = None
    if not approx:
        try:
            counted, entries = _exact_profile(table, pinned, schema)
        except Exception as error:
            reason = _approx_reason(error)
            if reason is None:
                raise
        else:
            return {
                **result,
                "row_count": counted if counted is not None else row_count,
                "approximate": False,
                "sample_size": None,
                "reason": None,
                "columns": entries,
            }
    size, entries = _approx_profile(
        table, pinned, schema, row_count, sample_rows, rng or random.Random()
    )
    return {
        **result,
        "row_count": row_count,
        "approximate": row_count is None or size < row_count,
        "sample_size": size,
        "reason": reason,
        "columns": entries,
    }


_DIRECTIONS = ("up", "down", "both")


def _reach(start: str, adjacency: dict, depth: int | None) -> dict[str, int]:
    distances = {start: 0}
    queue = deque([start])
    while queue:
        node = queue.popleft()
        if depth is not None and distances[node] >= depth:
            continue
        for neighbour in sorted(adjacency.get(node, ())):
            if neighbour not in distances:
                distances[neighbour] = distances[node] + 1
                queue.append(neighbour)
    return distances


def flow(
    graph: dict,
    collection: str,
    table: str,
    direction: str = "both",
    depth: int | None = None,
    include_system: bool = False,
) -> dict:
    """What feeds and what consumes a table, from its server flow graph: a
    breadth-first walk from the table along ``Output`` edges (function to
    table) and ``Dependency``/``Trigger`` edges (table to function),
    upstream, downstream or both, at most ``depth`` edges away. System tables
    are left out unless ``include_system``."""
    if direction not in _DIRECTIONS:
        raise ValueError(f"direction must be one of {', '.join(_DIRECTIONS)}.")
    tables = graph.get("all_tables") or {}
    functions = graph.get("all_functions") or {}
    system = set(graph.get("system_tables") or ())
    focus = f"t:{graph['table']}"
    kinds: dict[tuple[str, str], set[str]] = defaultdict(set)
    downstream: dict[str, set[str]] = defaultdict(set)
    upstream: dict[str, set[str]] = defaultdict(set)
    for relation in graph.get("relations") or ():
        function_id = relation["function_version_id"]
        table_id = relation["table_version_id"]
        if table_id in system and not include_system:
            continue
        kind = next(iter(relation["edge"]))
        if kind == "Output":
            source, target = f"f:{function_id}", f"t:{table_id}"
        else:
            source, target = f"t:{table_id}", f"f:{function_id}"
        kinds[(source, target)].add(kind)
        downstream[source].add(target)
        upstream[target].add(source)
    reached_up = _reach(focus, upstream, depth) if direction != "down" else {}
    reached_down = _reach(focus, downstream, depth) if direction != "up" else {}
    included = {focus, *reached_up, *reached_down}

    def node(node_id: str) -> dict:
        kind, key = node_id.split(":", 1)
        if kind == "t":
            info = tables.get(key) or {}
            return {
                "id": node_id,
                "kind": "table",
                "collection": info.get("collection"),
                "name": info.get("name"),
                "system": key in system,
            }
        info = functions.get(key) or {}
        return {
            "id": node_id,
            "kind": "function",
            "collection": info.get("collection"),
            "name": info.get("name"),
            "enabled": info.get("enabled"),
        }

    def edge(source: str, target: str, found: set[str]) -> dict:
        if "Output" in found:
            return {"from": source, "to": target, "kind": "output"}
        return {
            "from": source,
            "to": target,
            "kind": "dependency" if "Dependency" in found else "trigger",
            "trigger": "Trigger" in found,
        }

    def tables_of(reached: dict[str, int]) -> list[dict]:
        found = [
            {
                "collection": (tables.get(n[2:]) or {}).get("collection"),
                "table": (tables.get(n[2:]) or {}).get("name"),
                "distance": distance,
            }
            for n, distance in reached.items()
            if n.startswith("t:") and n != focus
        ]
        return sorted(
            found,
            key=lambda t: (t["distance"], t["collection"] or "", t["table"] or ""),
        )

    focus_info = tables.get(graph["table"]) or {}
    return {
        "focus": {
            "collection": focus_info.get("collection", collection),
            "table": focus_info.get("name", table),
            "table_version_id": graph["table"],
        },
        "direction": direction,
        "depth": depth,
        "nodes": [node(n) for n in sorted(included)],
        "edges": [
            edge(source, target, found)
            for (source, target), found in sorted(kinds.items())
            if source in included and target in included
        ],
        "upstream": tables_of(reached_up),
        "downstream": tables_of(reached_down),
    }


def diff_schemas(old: list[dict], new: list[dict]) -> dict:
    before = {c["name"]: c["type"] for c in old}
    after = {c["name"]: c["type"] for c in new}
    return {
        "added": [c for c in new if c["name"] not in before],
        "removed": [c for c in old if c["name"] not in after],
        "retyped": [
            {"name": c["name"], "from": before[c["name"]], "to": c["type"]}
            for c in new
            if c["name"] in before and before[c["name"]] != c["type"]
        ],
    }


def schema_history(table: Table, max_events: int = 10) -> dict:
    """The schema changes of a table, newest first: its committed versions
    collapsed into runs of one schema hash, each run boundary an event with
    the columns it added, removed or retyped. One schema read per run. At
    most ``max_events`` events; ``capped`` when older ones exist."""
    segments: list[dict] = []
    capped = False
    for version in table.list_dataversions_generator(
        filter=[COMMITTED], order_by="created_at-"
    ):
        schema_hash = version.kwargs.get("schema_hash")
        if schema_hash is None:
            continue
        if segments and segments[-1]["hash"] == schema_hash:
            segments[-1]["first"] = version
            continue
        if len(segments) > max_events:
            capped = True
            break
        segments.append({"hash": schema_hash, "first": version})
    schemas = [table.get_schema(version=s["first"]) or [] for s in segments]
    events = []
    for index in range(len(segments) - 1):
        first = segments[index]["first"]
        row = first.kwargs
        events.append({
            "version_id": first.id,
            "created_at": row.get("created_at"),
            "ended_on": row.get("ended_on"),
            "function": row.get("function"),
            "execution_id": row.get("execution_id"),
            **diff_schemas(schemas[index + 1], schemas[index]),
        })
    return {
        "collection": table.collection.name,
        "table": table.name,
        "current_schema_hash": segments[0]["hash"] if segments else None,
        "capped": capped,
        "events": events,
    }


def _side(table: Table, version) -> tuple[dict, list[dict]]:
    pinned = resolve_version(table, version)
    if pinned is None:
        raise NotFound(
            f"Table '{table.name}' in collection '{table.collection.name}' has no "
            "committed data version."
        )
    side = {
        "collection": table.collection.name,
        "table": table.name,
        "version_id": pinned.id,
    }
    return side, table.get_schema(version=pinned) or []


def schema_diff(
    table: Table,
    other: Table | None = None,
    from_version: DataVersion | str | None = None,
    to_version: DataVersion | str | None = None,
) -> dict:
    """How the schema of ``table`` at ``from_version`` differs from that of
    ``other`` (the same table when omitted) at ``to_version``; each version
    defaults to the newest committed one."""
    left, old = _side(table, from_version)
    right, new = _side(other or table, to_version)
    return {"from": left, "to": right, **diff_schemas(old, new)}
