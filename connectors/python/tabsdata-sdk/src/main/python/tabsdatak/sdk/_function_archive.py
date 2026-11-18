#
# Copyright 2026 Tabsdata Inc.
#

"""What registering a function sends the apiserver: the bundle built from its
``Info`` and the metadata the ``function_register`` payload carries."""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Union

# noinspection PyProtectedMember
from tabsdatak import _connectors

# noinspection PyProtectedMember
from tabsdatak._api import qualified_class
from tabsdatak._info import (
    KIND_PUBLISHER_DEF,
    KIND_STREAM_PUBLISHER_DEF,
    KIND_SUBSCRIBER_DEF,
    KIND_TRANSFORMER_DEF,
    PublisherInfo,
    StreamPublisherInfo,
    SubscriberInfo,
    TransformerInfo,
)
from tabsdatak.api import AllTablesTrigger, CronTrigger, TablesTrigger
from tabsdatak.common._archive.bundle import build_bundle, capture_source_snippet

if TYPE_CHECKING:
    from tabsdatak.common._actions.dataquality.processor import TrendTransformerInfo

FunctionInfo = Union[
    PublisherInfo, StreamPublisherInfo, TransformerInfo, SubscriberInfo
]


@dataclass
class _FunctionArchive:
    """Result of bundling a :class:`FunctionInfo` for registration: the
    bundle and the fields of the ``function_register`` / ``function_patch``
    payloads."""

    tables: list[str]
    table_types: dict[str, str]
    parent_tables: dict[str, str]
    string_dependencies: list[str]
    trigger_string_list: list[str] | None
    function_snippet: str
    context_location: str
    function_name: str
    function_type: str
    connector: str | None
    connection: str | None
    workers: dict[str, str]
    trend_transformers_info: list[TrendTransformerInfo] | None = None


# Stream publishers register as publishers; the runtime tells them apart by
# the bundle's ``function_def.json``.
_KIND_TO_API_TYPE: dict[str, str] = {
    KIND_PUBLISHER_DEF: "P",
    KIND_STREAM_PUBLISHER_DEF: "P",
    KIND_TRANSFORMER_DEF: "T",
    KIND_SUBSCRIBER_DEF: "S",
}


def _workers(info: FunctionInfo) -> dict[str, str]:
    """The workers payload ``function_register`` stores on the function:
    ``{"CT": {"mask": ...}}`` for a ``CronTrigger``, ``{"ST": {}}`` (every
    ``StageTriggerConfig`` field has a server default) for a
    ``@stream_publisher``, else ``{}``; each config is JSON-encoded.

    A stream publisher must emit ``"ST"``: without it the server never
    schedules its long-running worker, and the function gets no rows.
    """
    trigger_by = getattr(info, "trigger_by", None)
    if isinstance(trigger_by, CronTrigger):
        return {"CT": json.dumps({"mask": trigger_by.mask})}
    if getattr(info, "__kind__", None) == KIND_STREAM_PUBLISHER_DEF:
        return {"ST": json.dumps({})}
    return {}


def _trigger_string_list(info: FunctionInfo) -> list[str] | None:
    """Extract the ``trigger_string_list`` field the legacy
    ``function_register`` payload expects.

    ``None`` for ``trigger_by=None`` or :class:`AllTablesTrigger`, which
    leaves the server to derive the triggers from the function's input
    tables; the explicit table list, possibly empty, for
    :class:`TablesTrigger`; and no tables for a :class:`CronTrigger`,
    whose schedule travels in the workers map."""
    trigger_by = getattr(info, "trigger_by", None)
    if trigger_by is None or isinstance(trigger_by, AllTablesTrigger):
        return None
    if isinstance(trigger_by, TablesTrigger):
        return list(trigger_by.tables)
    return []


def _connector(info: FunctionInfo) -> str | None:
    """The function's source/destination class FQN -- the ``Src`` /
    ``StreamSrc`` / ``Dest`` the function binds to -- as a
    ``"module:qualname"`` string (e.g. ``"tabsdatak.conn.mysql:MySQLSrc"``).

    Discriminates on ``info.__kind__`` rather than ``isinstance`` so
    duck-typed Info objects (used in tests) work the same way. ``None`` for
    transformers (no connector).
    """
    kind = getattr(info, "__kind__", None)
    if kind in (KIND_PUBLISHER_DEF, KIND_STREAM_PUBLISHER_DEF):
        source = getattr(info, "source", None)
        return qualified_class(type(source)) if source is not None else None
    if kind == KIND_SUBSCRIBER_DEF:
        destination = getattr(info, "destination", None)
        return qualified_class(type(destination)) if destination is not None else None
    return None


def _connection(info: FunctionInfo) -> str | None:
    """The function's connection class FQN -- the concrete :class:`Conn`
    subclass its source/destination binds to -- as a ``"module:qualname"``
    string (e.g. ``"tabsdatak.conn.mysql:MySQLSrcConn"``).

    Resolved from the connector registry by the source/destination type
    (``def.conn``), *not* from a ``conn`` declared on the decorator. The
    server matches it against the collection's connection on register/update.
    ``None`` for transformers (no connection)."""
    kind = getattr(info, "__kind__", None)
    if kind == KIND_PUBLISHER_DEF:
        source = getattr(info, "source", None)
        conn_cls = (
            _connectors.get_src(type(source)).conn if source is not None else None
        )
    elif kind == KIND_STREAM_PUBLISHER_DEF:
        source = getattr(info, "source", None)
        conn_cls = (
            _connectors.get_stream_src(type(source)).conn
            if source is not None
            else None
        )
    elif kind == KIND_SUBSCRIBER_DEF:
        destination = getattr(info, "destination", None)
        conn_cls = (
            _connectors.get_dest(type(destination)).conn
            if destination is not None
            else None
        )
    else:
        conn_cls = None
    return qualified_class(conn_cls) if conn_cls is not None else None


def _input_tables(info: FunctionInfo) -> list[str]:
    """For transformers / subscribers, the cross-collection
    ``"collection/table"`` references. For publishers, an empty list."""
    return list(getattr(info, "input_tables", []) or [])


def _output_tables(info: FunctionInfo) -> list[str]:
    """For publishers / transformers, the declared output table
    names. For subscribers (no outputs), an empty list."""
    return list(getattr(info, "output_tables", []) or [])


def _dq_derived(
    collection: str, info: FunctionInfo
) -> tuple[list[str], dict[str, str], dict[str, str], list[TrendTransformerInfo]]:
    """Derive everything a function's data-quality actions imply.

    Returns the derived table names, their catalog type codes (so
    registration records each derived table under its ``TableType``), the
    output table each derived table derives from -- summaries and the trend
    tables alike, so registration parents them -- and the system transformers
    that maintain the summary trend tables. All four are empty when the
    function declares no data-quality action.

    One :class:`DataQualityProcessor` serves all four, so the action set is
    resolved and validated exactly once per registration.
    """
    from tabsdatak.common._actions.dataquality.processor import (
        DataQualityProcessor,
    )
    from tabsdatak.dataquality import DataQuality
    from tabsdatak.dataquality._tableinfo import TableKind

    on_tables = getattr(info, "on_tables", None) or []
    actions = [action for action in on_tables if isinstance(action, DataQuality)]
    if not actions:
        return [], {}, {}, []

    processor = DataQualityProcessor.from_actions(
        collection=collection,
        output_tables=_output_tables(info),
        actions=actions,
    )
    _, output_table_infos = processor.table_names()
    names = [table_info.table for table_info in output_table_infos]
    types = {
        table_info.table: table_info.kind.type.value
        for table_info in output_table_infos
    }
    trends = processor.build_trend_transformers_info()
    parents = {
        table_info.table: table_info.from_table
        for table_info in output_table_infos
        if table_info.kind is TableKind.DQ_SUMMARY
    } | {
        table: trend.parent_table
        for trend in trends
        for table in trend.info.output_tables
    }
    return names, types, parents, trends


def _cleanup_trend_roots(trend_transformers) -> None:
    """Discard the generated-module directories built for trend transformers.

    ``build_bundle`` copies each generated module into its bundle's
    ``user_code/`` during registration, so once registration has been attempted
    the directories are dead weight. Called from a ``finally`` so a failed
    registration leaves nothing behind either.
    """
    from tabsdatak.common._actions.dataquality.trend import cleanup_trend_tfr_mod

    for trend in trend_transformers or []:
        cleanup_trend_tfr_mod(trend.project_root)


def _create_archive(
    collection: str,
    info: FunctionInfo,
    project_root: str | Path,
    temporary_directory: tempfile.TemporaryDirectory | str,
    local_packages: list[str] | None = None,
    valid_python_versions: list[str] | None = None,
    requirements: str | Path | None = None,
    confine_to: str | Path | None = None,
) -> _FunctionArchive:
    """Bundle ``info`` (a :class:`FunctionInfo`) for registration.

    Builds the ``.tar.zst`` with
    :func:`tabsdatak.common._archive.bundle.build_bundle` and assembles the
    metadata fields of the ``function_register`` payload.

    Data-quality actions declared in ``info.on_tables`` contribute their
    derived tables to ``tables`` and their catalog kinds to ``table_types``
    (see :func:`_dq_derived`), so a data-quality function's derived outputs are
    registered under the right :class:`TableType`.
    """
    save_location = (
        temporary_directory.name
        if isinstance(temporary_directory, tempfile.TemporaryDirectory)
        else temporary_directory
    )
    bundle_path = Path(save_location) / "bundle.tar.zst"

    build_bundle(
        info,
        Path(project_root),
        bundle_path,
        local_packages=[Path(p) for p in local_packages] if local_packages else None,
        valid_python_versions=valid_python_versions,
        requirements_yaml=Path(requirements) if requirements else None,
        confine_to=Path(confine_to) if confine_to else None,
    )

    function_name = getattr(info.func, "__name__", "<anonymous>")
    function_type = _KIND_TO_API_TYPE.get(info.__kind__, "U")
    function_snippet = capture_source_snippet(info)

    dq_names, dq_types, dq_parents, trend_transformers = _dq_derived(collection, info)

    return _FunctionArchive(
        tables=_output_tables(info) + dq_names,
        table_types=dq_types,
        parent_tables=dq_parents,
        string_dependencies=_input_tables(info),
        trigger_string_list=_trigger_string_list(info),
        function_snippet=function_snippet,
        context_location=str(bundle_path),
        function_name=function_name,
        function_type=function_type,
        connector=_connector(info),
        connection=_connection(info),
        workers=_workers(info),
        trend_transformers_info=trend_transformers,
    )
