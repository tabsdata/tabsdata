#
# Copyright 2026 Tabs Data Inc.
#

"""Shared request-driven context implementation.

Two layers, matching the SPI:

* :class:`FromFlowRequestCtx` — flow-level surface only (``work_dir``
  + ``collection_id`` / ``collection_name`` / ``function_id`` /
  ``function_name``) plus the per-flow scratch store. Mirrors
  :class:`tabsdatak.spi.PluginCtx`, which extends :class:`FlowCtx`
  *without* :class:`TrxCtx`. The streamer-pod context uses this base —
  a streamer has no transaction or execution plan.
* :class:`FromRequestCtx` — extends :class:`FromFlowRequestCtx` with
  the trx-level identifiers (``exec_plan_id`` / ``trx_id`` /
  ``exec_plan_ts``). Mirrors :class:`SrcPluginCtx` / :class:`DestContext`
  / :class:`FunctionCtx`, which extend :class:`TrxCtx`. The per-command
  contexts driven by function-run requests use this base.

The split matches the SPI inheritance:

    FlowCtx                                ← collection / function ids
    PluginCtx(FlowCtx)                     ← + work_dir
    StreamSrcPluginCtx(PluginCtx)          ← stream sources: NO TrxCtx
    SrcPluginCtx(TrxCtx, PluginCtx)        ← batch sources: TrxCtx
    DestContext(TrxCtx, PluginCtx)         ← subscribers: TrxCtx
    FunctionCtx(TrxCtx, FlowCtx)           ← user fn: TrxCtx

Each per-command class subclasses
``(FromRequestCtx, <role-base>)`` (trx-ful) or
``(FromFlowRequestCtx, <role-base>)`` (flow-only). The name keeps
"Ctx" rather than "PluginCtx" because the user-function context is
*not* a plugin (see TD-2124) yet shares the same backing data.

State plumbed:

* ``work_dir``, ``collection_id``, ``collection_name``,
  ``function_id``, ``function_name`` — populated from
  ``request.info`` (or from the streamer-specific envelope).
  ``function_id`` borrows ``function_run_id`` on the trx-ful path
  and ``function_version_id`` on the streamer path; ``function_name``
  borrows ``function``.
* ``exec_plan_id``, ``trx_id``, ``exec_plan_ts`` — trx-ful only.
  ``exec_plan_ts`` is the ISO-8601 form of ``triggered_on``.
* ``get_attr`` / ``store_attr`` — per-flow scratch state backed by
  an in-memory dict. The runtime populates the dict at invocation
  start (:meth:`hydrate`) and reads it back at the end
  (:meth:`drain`) to round-trip through Parquet. Writes via
  ``store_attr`` are write-through to the in-memory dict; no I/O
  happens at the call site.

State *not yet* plumbed (falls back to a documented stub):

* ``add_metadata`` is a no-op — the trx-level observability sink
  isn't wired through tdrunner yet, but consumers that touch it
  shouldn't crash.

:meth:`hydrate` and :meth:`drain` are runtime-only — they're not
declared on the abstract bases (:class:`tabsdatak.api.TrxCtx`,
:class:`tabsdatak.spi.FlowCtx`, :class:`tabsdatak.spi.PluginCtx`,
:class:`tabsdatak.spi.SrcPluginCtx`, :class:`tabsdatak.spi.DestContext`,
:class:`tabsdatak.spi.FunctionCtx`) so user-typed code never sees
them via autocomplete or static analysis. The runtime is the only
caller.

When a new field lands in the runner, fix it once here and every
concrete context inherits the upgrade.
"""

from __future__ import annotations

import copy
from pathlib import Path

from tabsdatak.api import ContextKeySpec, ContextValueSpec, TrxCtx
from tabsdatak.server._tdrunner.function._lib.initial_values import _validate_key
from tabsdatak.spi import FlowCtx


class FromFlowRequestCtx(FlowCtx):
    """Flow-only request-driven context: ``work_dir`` + flow-level
    identifiers + scratch state, without the trx-level fields.

    Subclasses :class:`tabsdatak.spi.FlowCtx`, mirroring the SPI's
    :class:`PluginCtx` / :class:`StreamSrcPluginCtx`, which extend
    :class:`FlowCtx` but not :class:`TrxCtx`. The streamer pod's
    context composes this with :class:`StreamSrcPluginCtx` because a
    streamer has no transaction or execution plan -- the per-batch
    publisher pods spawned downstream via ``fn_trigger`` are where
    real ``execution_id`` / ``transaction_id`` / ``function_run_id``
    values live.
    """

    def __init__(
        self,
        work_dir: Path,
        collection_id: str,
        collection_name: str,
        function_id: str,
        function_name: str,
    ):
        self._work_dir = work_dir
        self._collection_id = collection_id
        self._collection_name = collection_name
        self._function_id = function_id
        self._function_name = function_name
        self._scratch: dict[str, ContextValueSpec] = {}

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def collection_id(self) -> str:
        return self._collection_id

    @property
    def collection_name(self) -> str:
        return self._collection_name

    @property
    def function_id(self) -> str:
        return self._function_id

    @property
    def function_name(self) -> str:
        return self._function_name

    def get_attr(self, attr: ContextKeySpec) -> ContextValueSpec | None:
        """Read a per-flow scratch value. Returns ``None`` for keys
        that have never been stored (no `KeyError` — matches the
        ``returns None if unset`` contract on
        :meth:`tabsdatak.api.TrxCtx.get_attr`)."""
        return self._scratch.get(attr)

    def store_attr(self, attr: ContextKeySpec, val: ContextValueSpec | None):
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
        _validate_key(attr)
        self._scratch[attr] = val

    def add_metadata(self, attr: ContextKeySpec, val: ContextValueSpec | None):
        # TODO: wire the trx-level observability sink. No-op for now.
        pass

    # --- runtime-only lifecycle hooks ------------------------------------
    #
    # ``hydrate`` and ``drain`` are deliberately not declared on the
    # abstract bases -- user-typed code (which annotates as
    # ``TrxCtx`` / ``SrcPluginCtx`` / ``DestContext`` / ``FunctionCtx``)
    # therefore never sees them. The runtime, which holds a concrete
    # ``FromRequestCtx`` reference, is the only caller.

    def hydrate(self, initial: dict[str, ContextValueSpec]) -> None:
        """Pre-populate the scratch store with values loaded from the
        previous run's persisted dict. Called by the runtime once,
        before the plugin / user function is invoked.

        Repeated calls merge into the existing store (last value
        wins for any duplicate key).
        """
        self._scratch.update(initial)

    def drain(self) -> dict[str, ContextValueSpec]:
        """Return a deep copy of the current scratch state for the
        runtime to persist. Deep so nested mutables (e.g. a stored
        ``dict[str, str]`` value) don't share references between the
        ctx and the runtime's persistence path."""
        return copy.deepcopy(self._scratch)


class FromRequestCtx(TrxCtx, FromFlowRequestCtx):
    """Flow-level + trx-level request-driven context.

    Extends :class:`FromFlowRequestCtx` with the trx-level identifiers
    (``exec_plan_id`` / ``trx_id`` / ``exec_plan_ts``) populated from
    ``request.info``. Per-command classes compose this with the
    role-specific abstract base via multiple inheritance:

        class _SrcContext(FromRequestCtx, SrcPluginCtx): ...
        class _DestContext(FromRequestCtx, DestContext): ...
        class _FunctionContext(FromRequestCtx, FunctionCtx): ...

    Subclassing :class:`tabsdatak.api.TrxCtx` keeps the implementation
    type-honestly related to the API contract it satisfies. The base
    order is ``(TrxCtx, FromFlowRequestCtx)`` so the MRO linearizes
    compatibly with the role-typed bases above, which all list
    :class:`TrxCtx` before :class:`PluginCtx` (= ``FlowCtx``); listing
    ``FromFlowRequestCtx`` first here would put ``FlowCtx`` before
    ``TrxCtx`` and break the MRO merge in ``_DestContext`` /
    ``_SrcContext`` / ``_FunctionContext``. The streamer-pod context
    uses :class:`FromFlowRequestCtx` directly (no trx fields) and
    does not pass through this class.
    """

    def __init__(
        self,
        work_dir: Path,
        exec_plan_id: str,
        trx_id: str,
        exec_plan_ts: str,
        collection_id: str,
        collection_name: str,
        function_id: str,
        function_name: str,
    ):
        super().__init__(
            work_dir=work_dir,
            collection_id=collection_id,
            collection_name=collection_name,
            function_id=function_id,
            function_name=function_name,
        )
        self._exec_plan_id = exec_plan_id
        self._trx_id = trx_id
        self._exec_plan_ts = exec_plan_ts

    @property
    def exec_plan_id(self) -> str:
        return self._exec_plan_id

    @property
    def trx_id(self) -> str:
        return self._trx_id

    @property
    def exec_plan_ts(self) -> str:
        return self._exec_plan_ts

    # Re-export the concrete impls from FromFlowRequestCtx so ABCMeta
    # credits them against TrxCtx's abstract declarations of the same
    # names. With the MRO (TrxCtx, FromFlowRequestCtx) TrxCtx's
    # abstract get_attr / store_attr / add_metadata is visited first
    # at __abstractmethods__ computation, and the resolution machinery
    # does not always credit a non-TrxCtx descendant's concrete impl
    # across the multi-inheritance gap. Explicit aliases short-circuit
    # that.
    get_attr = FromFlowRequestCtx.get_attr
    store_attr = FromFlowRequestCtx.store_attr
    add_metadata = FromFlowRequestCtx.add_metadata
