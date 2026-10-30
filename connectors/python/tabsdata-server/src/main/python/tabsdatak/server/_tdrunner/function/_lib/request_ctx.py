#
# Copyright 2026 Tabsdata Inc.
#

"""Shared request-driven context implementation.

Two layers, matching the API / SPI context hierarchy:

* :class:`FromFlowRequestCtx` — flow-level surface only (``work_dir``
  + ``project_id`` / ``project`` / ``collection_id`` / ``collection``
  / ``function_id`` / ``function`` / ``function_version_id``) plus the
  per-flow scratch store. Mirrors :class:`tabsdatak.spi.PluginCtx`,
  which extends :class:`tabsdatak.api.FlowCtx` *without*
  :class:`TransactionCtx`. The streamer-pod context uses this base — a
  streamer has no transaction or execution.
* :class:`FromRequestCtx` — extends :class:`FromFlowRequestCtx` with
  the trx-level identifiers (``execution_id`` / ``execution`` /
  ``transaction_id`` / ``logical_function_run_id`` /
  ``physical_function_run_id`` / ``triggered_on``). Mirrors
  :class:`SrcPluginCtx` / :class:`DestContext` / :class:`FunctionCtx`,
  which extend :class:`TransactionCtx`. The per-command contexts driven by
  function-run requests use this base.

The split matches the API (``api``) / SPI (``spi``) inheritance:

    api.FlowCtx                            ← project / collection / function ids
    api.FunctionCtx(TransactionCtx, FlowCtx)       ← user fn: TransactionCtx
    spi.PluginCtx(FlowCtx)                 ← + work_dir
    spi.StreamSrcPluginCtx(PluginCtx)      ← stream sources: NO TransactionCtx
    spi.SrcPluginCtx(TransactionCtx, PluginCtx)    ← batch sources: TransactionCtx
    spi.DestContext(TransactionCtx, PluginCtx)     ← subscribers: TransactionCtx

Each per-command class subclasses
``(FromRequestCtx, <role-base>)`` (trx-ful) or
``(FromFlowRequestCtx, <role-base>)`` (flow-only). The name keeps
"Ctx" rather than "PluginCtx" because the user-function context is
*not* a plugin (see TD-2124) yet shares the same backing data.

State plumbed:

* Every identifier is populated from ``request.info`` (or from the
  streamer-specific envelope) under the same name, which is also the
  name the server API uses. ``triggered_on`` is the timezone-aware
  UTC form of the request's epoch-millisecond ``triggered_on``.
* ``get_attr`` / ``set_attr`` — per-flow scratch state backed by
  an in-memory dict. The runtime populates the dict at invocation
  start (:meth:`hydrate`) and reads it back at the end
  (:meth:`drain`) to round-trip through Parquet. Writes via
  ``set_attr`` are write-through to the in-memory dict; no I/O
  happens at the call site.

State *not yet* plumbed (falls back to a documented stub):

* ``add_metadata`` is a no-op — the trx-level observability sink
  isn't wired through tdrunner yet, but consumers that touch it
  shouldn't crash.

:meth:`hydrate` and :meth:`drain` are runtime-only — they're not
declared on the abstract bases (:class:`tabsdatak.api.TransactionCtx`,
:class:`tabsdatak.api.FlowCtx`, :class:`tabsdatak.api.FunctionCtx`,
:class:`tabsdatak.spi.PluginCtx`, :class:`tabsdatak.spi.SrcPluginCtx`,
:class:`tabsdatak.spi.DestContext`) so user-typed code never sees
them via autocomplete or static analysis. The runtime is the only
caller.

When a new field lands in the runner, fix it once here and every
concrete context inherits the upgrade.
"""

from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path
from typing import Self

from tabsdatak.api import BasicTypeSpec, ContextKeySpec, FlowCtx, TransactionCtx
from tabsdatak.server._tdrunner.function._commons.document.request import Info
from tabsdatak.server._tdrunner.function._handlers.state import validate_key


class FromFlowRequestCtx(FlowCtx):
    """Flow-only request-driven context: ``work_dir`` + flow-level
    identifiers + scratch state, without the trx-level fields.

    Subclasses :class:`tabsdatak.api.FlowCtx`, mirroring the SPI's
    :class:`PluginCtx` / :class:`StreamSrcPluginCtx`, which extend
    :class:`FlowCtx` but not :class:`TransactionCtx`. The streamer pod's
    context composes this with :class:`StreamSrcPluginCtx` because a
    streamer has no transaction or execution -- the per-batch
    publisher pods spawned downstream via ``fn_trigger`` are where
    real ``execution_id`` / ``transaction_id`` /
    ``logical_function_run_id`` / ``physical_function_run_id`` values
    live.
    """

    def __init__(
        self,
        work_folder: Path,
        project_id: str,
        project: str,
        collection_id: str,
        collection: str,
        function_id: str,
        function: str,
        function_version_id: str,
    ):
        self._work_folder = work_folder
        self._project_id = project_id
        self._project = project
        self._collection_id = collection_id
        self._collection = collection
        self._function_id = function_id
        self._function = function
        self._function_version_id = function_version_id
        self._scratch: dict[str, BasicTypeSpec] = {}

    @property
    def work_dir(self) -> Path:
        return self._work_folder

    @property
    def project_id(self) -> str:
        return self._project_id

    @property
    def project(self) -> str:
        return self._project

    @property
    def collection_id(self) -> str:
        return self._collection_id

    @property
    def collection(self) -> str:
        return self._collection

    @property
    def function_id(self) -> str:
        return self._function_id

    @property
    def function(self) -> str:
        return self._function

    @property
    def function_version_id(self) -> str:
        return self._function_version_id

    def get_attr(self, attr: ContextKeySpec) -> BasicTypeSpec | None:
        """Read a per-flow scratch value. Returns ``None`` for keys
        that have never been stored (no `KeyError` — matches the
        ``returns None if unset`` contract on
        :meth:`tabsdatak.api.TransactionCtx.get_attr`)."""
        return self._scratch.get(attr)

    def set_attr(self, attr: ContextKeySpec, val: BasicTypeSpec | None):
        """Write a per-flow scratch value. Write-through to the
        in-memory dict; the runtime persists the dict to Parquet at
        the end of the invocation via :meth:`drain`.

        The key is validated eagerly as a ``str`` so a non-string
        key surfaces at the user's call site rather than at drain
        time (where the stack trace would point at the runtime).
        Value-shape validation is deferred to the codec so the None
        sentinel — which the SPI allows but the Parquet codec
        doesn't take a position on — round-trips through the
        scratch store.
        """
        validate_key(attr)
        self._scratch[attr] = val

    def add_metadata(self, attr: ContextKeySpec, val: BasicTypeSpec | None):
        # TODO: wire the trx-level observability sink. No-op for now.
        pass

    # --- runtime-only lifecycle hooks ------------------------------------
    #
    # ``hydrate`` and ``drain`` are deliberately not declared on the
    # abstract bases -- user-typed code (which annotates as
    # ``TransactionCtx`` / ``SrcPluginCtx`` / ``DestContext`` / ``FunctionCtx``)
    # therefore never sees them. The runtime, which holds a concrete
    # ``FromRequestCtx`` reference, is the only caller.

    def hydrate(self, initial: dict[str, BasicTypeSpec]) -> None:
        """Pre-populate the scratch store with values loaded from the
        previous run's persisted dict. Called by the runtime once,
        before the plugin / user function is invoked.

        Repeated calls merge into the existing store (last value
        wins for any duplicate key).
        """
        self._scratch.update(initial)

    def drain(self) -> dict[str, BasicTypeSpec]:
        """Return a deep copy of the current scratch state for the
        runtime to persist, so the returned dict never aliases the
        ctx's own store."""
        return copy.deepcopy(self._scratch)


class FromRequestCtx(TransactionCtx, FromFlowRequestCtx):
    """Flow-level + trx-level request-driven context.

    Extends :class:`FromFlowRequestCtx` with the trx-level identifiers
    (``execution_id`` / ``execution`` / ``transaction_id`` /
    ``logical_function_run_id`` / ``physical_function_run_id`` /
    ``triggered_on``) populated from ``request.info``. Per-command
    classes compose this with the role-specific abstract base via
    multiple inheritance:

        class SourceContext(FromRequestCtx, SrcPluginCtx): ...
        class DestinationContext(FromRequestCtx, DestContext): ...
        class FunctionContext(FromRequestCtx, FunctionCtx): ...

    Subclassing :class:`tabsdatak.api.TransactionCtx` keeps the implementation
    type-honestly related to the API contract it satisfies. The base
    order is ``(TransactionCtx, FromFlowRequestCtx)`` so the MRO linearizes
    compatibly with the role-typed bases above, which all list
    :class:`TransactionCtx` before :class:`PluginCtx` (= ``FlowCtx``); listing
    ``FromFlowRequestCtx`` first here would put ``FlowCtx`` before
    ``TransactionCtx`` and break the MRO merge in ``DestinationContext`` /
    ``SourceContext`` / ``FunctionContext``. The streamer-pod context
    uses :class:`FromFlowRequestCtx` directly (no trx fields) and
    does not pass through this class.
    """

    def __init__(
        self,
        work_folder: Path,
        project_id: str,
        project: str,
        collection_id: str,
        collection: str,
        function_id: str,
        function: str,
        function_version_id: str,
        execution_id: str,
        execution: str | None,
        transaction_id: str,
        logical_function_run_id: str,
        physical_function_run_id: str,
        triggered_on: datetime,
    ):
        super().__init__(
            work_folder=work_folder,
            project_id=project_id,
            project=project,
            collection_id=collection_id,
            collection=collection,
            function_id=function_id,
            function=function,
            function_version_id=function_version_id,
        )
        self._execution_id = execution_id
        self._execution = execution
        self._transaction_id = transaction_id
        self._logical_function_run_id = logical_function_run_id
        self._physical_function_run_id = physical_function_run_id
        self._triggered_on = triggered_on

    @classmethod
    def from_info(cls, work_folder: Path, info: Info) -> Self:
        """Build the context of the run ``info`` describes."""
        return cls(
            work_folder=work_folder,
            project_id=str(info.project_id),
            project=info.project,
            collection_id=str(info.collection_id),
            collection=info.collection,
            function_id=str(info.function_id),
            function=info.function,
            function_version_id=str(info.function_version_id),
            execution_id=str(info.execution_id),
            execution=info.execution,
            transaction_id=str(info.transaction_id),
            logical_function_run_id=str(info.logical_function_run_id),
            physical_function_run_id=str(info.physical_function_run_id),
            triggered_on=info.triggered_on_datetime,
        )

    @property
    def execution_id(self) -> str:
        return self._execution_id

    @property
    def execution(self) -> str | None:
        return self._execution

    @property
    def transaction_id(self) -> str:
        return self._transaction_id

    @property
    def logical_function_run_id(self) -> str:
        return self._logical_function_run_id

    @property
    def physical_function_run_id(self) -> str:
        return self._physical_function_run_id

    @property
    def triggered_on(self) -> datetime:
        return self._triggered_on

    # Re-export the concrete impls from FromFlowRequestCtx so ABCMeta
    # credits them against TransactionCtx's abstract declarations of the same
    # names. With the MRO (TransactionCtx, FromFlowRequestCtx) TransactionCtx's
    # abstract get_attr / set_attr / add_metadata is visited first
    # at __abstractmethods__ computation, and the resolution machinery
    # does not always credit a non-TransactionCtx descendant's concrete impl
    # across the multi-inheritance gap. Explicit aliases short-circuit
    # that.
    get_attr = FromFlowRequestCtx.get_attr
    set_attr = FromFlowRequestCtx.set_attr
    add_metadata = FromFlowRequestCtx.add_metadata
