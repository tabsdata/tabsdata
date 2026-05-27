#
# Copyright 2026 Tabs Data Inc.
#

"""Shared plugin-context implementation.

Both ``run_source_plugin._SrcContext`` (subclass of
:class:`tabsdatak.spi.SrcPluginCtx`) and
``run_destination_plugin._DestContext`` (subclass of
:class:`tabsdatak.spi.DestContext`) had nearly identical 80-line
bodies — same constructor + 11 ``@property`` / method implementations
populated from ``request.yaml``. The only meaningful difference was
the abstract base they inherited from.

This module collects the shared bodies in :class:`FromRequestPluginCtx`,
a mixin that implements every property/method the two abstract bases
declare. Each command subclasses ``(_FromRequestPluginCtx, <base>)``
and the per-command class becomes a one-line declaration.

State plumbed today:
  * ``work_dir``, ``collection_id``, ``collection_name``,
    ``function_id``, ``exec_plan_id``, ``trx_id`` — populated from
    ``request.info`` (``function_id`` borrows ``function_run_id``,
    the closest identifier the schema carries today).

State *not yet* plumbed (each falls back to a documented stub):
  * ``function_name`` and ``exec_plan_ts`` raise
    ``NotImplementedError`` — they're missing from ``request.yaml``;
    raising surfaces the gap loudly rather than letting plugins
    silently consume a placeholder.
  * ``get_attr`` returns ``None`` (honours the documented
    "returns None if unset" contract). ``store_attr`` and
    ``add_metadata`` are no-ops — the per-flow scratch store and
    observability sink aren't wired through tdrunner yet, but
    plugins that touch them shouldn't crash. Values simply don't
    persist across trx invocations.

When the gaps above land in ``request.yaml`` / the runner, fix them
once here and every plugin context inherits the upgrade.
"""

from __future__ import annotations

from pathlib import Path

from tabsdatak.api import ContextKeySpec, ContextValueSpec


class FromRequestPluginCtx:
    """Mixin implementing the shared surface of
    :class:`tabsdatak.spi.SrcPluginCtx` and
    :class:`tabsdatak.spi.DestContext`.

    Both abstract bases declare the same 11 members; this class
    implements all of them in one place. Composes via multiple
    inheritance with the appropriate abstract base from
    ``tabsdatak.spi``.
    """

    def __init__(
        self,
        work_dir: Path,
        exec_plan_id: str,
        trx_id: str,
        collection_id: str,
        collection_name: str,
        function_id: str,
    ):
        self._work_dir = work_dir
        self._exec_plan_id = exec_plan_id
        self._trx_id = trx_id
        self._collection_id = collection_id
        self._collection_name = collection_name
        self._function_id = function_id

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
        # TODO: thread function_name through request.yaml's info block
        # and populate at construction. Raising rather than returning
        # a placeholder so consumers surface the gap.
        raise NotImplementedError(
            "function_name is not yet plumbed through request.yaml; "
            "see _lib.plugin_ctx.FromRequestPluginCtx"
        )

    @property
    def exec_plan_id(self) -> str:
        return self._exec_plan_id

    @property
    def trx_id(self) -> str:
        return self._trx_id

    @property
    def exec_plan_ts(self) -> str:
        # TODO: add `exec_plan_ts` to request.yaml's `info` block and
        # plumb it through here. Raising rather than returning a
        # placeholder so any consumer reading it gets pointed at the gap.
        raise NotImplementedError(
            "exec_plan_ts is not yet plumbed through request.yaml; "
            "see _lib.plugin_ctx.FromRequestPluginCtx"
        )

    def get_attr(self, attr: ContextKeySpec) -> ContextValueSpec | None:
        # TODO: wire the per-flow attribute store through the runner.
        # Until then we honour the documented "returns None if unset"
        # contract -- plugins that probe for scratch state see no
        # prior state, which is the same surface they'd see on the
        # first call of a brand-new flow anyway.
        return None

    def store_attr(self, attr: ContextKeySpec, val: ContextValueSpec | None):
        # TODO: wire the per-flow attribute store. No-op for now so
        # plugins that store progress markers don't crash; values
        # simply don't persist across trx invocations.
        pass

    def add_metadata(self, attr: ContextKeySpec, val: ContextValueSpec | None):
        # TODO: wire the trx-level observability sink. No-op for now.
        pass
