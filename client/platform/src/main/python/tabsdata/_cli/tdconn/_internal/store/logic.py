#
#  Copyright 2026. Tabs Data Inc.
#
"""Business logic for connection management.

Separated from the CLI so it can be reused by other entry points
(AI agents, tests, a future programmatic API) without argparse in
the mix.

The API is split in two layers:

* :class:`Logic` — the abstract surface AI callers target. Also hosts
  the concrete :meth:`Logic.enrich_all` since it is pure composition
  over :meth:`enrich` and :meth:`list`.
* :class:`BaseLogic` — the reference implementation backed by a
  :class:`ConnStore`. Subclasses override :meth:`prepare_conn` to plug
  in backend-specific post-validation behavior.

Every verb that produces records (``add``, ``update``, ``get``,
``enrich``, ``verify``, ``list``, ``enrich_all``) yields
:class:`_ConnDef` values rather than raw dicts or names: callers get
a typed envelope (``name`` / ``conn_type`` / ``conn_mode`` /
``conn_configs``) and can opt in to env-variable substitution via
``resolved_envs=True`` — mirroring what :class:`NamedConn._load` does
at function-run time. There is no separate ``refresh``: re-validating
the stored JSON is folded into :meth:`enrich`, so a single verb
drives "bring this connection up to date" — definition reload on the
base layer, plus metadata re-extraction on subclasses.

Every method also accepts a hidden ``debug: bool = False`` flag that
the CLI sets via its undocumented ``--debug`` option. When true,
each layer prints a ``[debug]`` trace line to stderr and forwards
the flag to whatever lower-level calls it makes — so ``tdconn
--debug <verb>`` shows exactly which store / metadata / logic
methods fire and in what order.
"""

from __future__ import annotations

import abc
import builtins
import json
from pathlib import Path
from typing import Any

from tabsdata import NamedConn
from tabsdata._cli.tdconn._internal._debug import log_call
from tabsdata._cli.tdconn._internal.extractor.registry import extractor_for
from tabsdata._cli.tdconn._internal.store.conn_store import ConnStore
from tabsdata._io.connections.named_connection.named_connection import _ConnDef

# Inside classes that define a ``list`` method, ``list[...]`` in
# annotations binds to the method and confuses static analysis. Alias
# through ``builtins`` so the type stays unambiguous.
_ConnDefList = builtins.list[_ConnDef]


class Logic(abc.ABC):
    """Abstract API for managing connections in a :class:`ConnStore`."""

    @abc.abstractmethod
    def add(
        self,
        conn_file: Path,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> tuple[str, _ConnDef]:
        """Validate *conn_file* and add it to the store.

        Returns ``(name, conn_def)`` where *name* is the connection
        name declared in the JSON and *conn_def* is the persisted
        record — with ``env:`` references resolved when
        *resolved_envs* is ``True``. Raises ``ValueError`` if a
        connection with the same name already exists or the JSON is
        invalid.
        """

    @abc.abstractmethod
    def update(
        self,
        name: str,
        conn_file: Path,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> _ConnDef:
        """Replace the stored connection *name* with the contents of *conn_file*.

        Returns the updated :class:`_ConnDef` — resolved when
        *resolved_envs* is ``True``. The JSON's ``name`` field must
        equal *name*. Raises ``ValueError`` if *name* is not in the
        store or the JSON disagrees on the name.
        """

    @abc.abstractmethod
    def remove(self, name: str, debug: bool = False) -> None:
        """Delete the connection named *name*. Raises ``ValueError`` if missing."""

    @abc.abstractmethod
    def get(
        self,
        name: str,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> _ConnDef:
        """Return the stored :class:`_ConnDef` for *name*.

        When *resolved_envs* is ``True``, the returned record carries
        ``conn_configs`` with ``env:`` references substituted for the
        named environment variable's value. Raises ``ValueError`` if
        *name* is not in the store.
        """

    @abc.abstractmethod
    def list(
        self,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> _ConnDefList:
        """Return every stored record as :class:`_ConnDef`, sorted by name.

        When *resolved_envs* is ``True``, each record's ``conn_configs``
        has ``env:`` references substituted so callers can hand the
        list straight to extractors without re-running
        :class:`NamedConn._load`. The name shadowing Python's built-in
        is deliberate — :class:`Logic` consumers always invoke it as
        ``logic.list(...)``, not bareword ``list``.
        """

    @abc.abstractmethod
    def enrich(
        self,
        name: str,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> tuple[_ConnDef, Any]:
        """Re-validate and (re)enrich the connection *name*.

        Runs the :class:`NamedConn` loader on the stored JSON (the
        old ``refresh`` contract) and then produces whatever enriched
        payload the subclass maintains. Returns ``(conn_def,
        enriched)`` where *conn_def* is the record that was
        re-validated — so UX layers can render identity without a
        follow-up fetch — and *enriched* is the payload: ``None`` on
        the base layer, a metadata dict on :class:`AiLogic`. Shape of
        *enriched* is subclass-defined; callers pass it straight to
        :meth:`get_enriched` to re-read later.
        """

    @abc.abstractmethod
    def get_enriched(self, name: str, debug: bool = False) -> Any:
        """Return the cached enriched information for *name*.

        Shape is subclass-defined. :class:`BaseLogic` returns
        ``None`` (nothing to enrich); :class:`AiLogic` returns
        ``{"metadata": [...]}`` when the connection has been enriched
        and ``{"metadata": None}`` otherwise — callers can tell
        "never enriched" apart from "enriched but empty" without
        having to poke the underlying store.
        """

    @abc.abstractmethod
    def verify(
        self,
        name: str,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> _ConnDef:
        """Round-trip a real connectivity probe against *name*.

        Unlike :meth:`enrich` — which only checks that the stored JSON
        still parses — ``verify`` looks up the matching
        :class:`ConnectionExtractor` for the record's ``conn_type``
        and calls its :meth:`check_connect`, which opens a real session
        against the target system. Returns the :class:`_ConnDef`
        that was probed — resolved when *resolved_envs* is ``True`` —
        so UX layers can render identity without a second fetch.
        Raises whatever the underlying client library raises on
        connectivity failure, and ``ValueError`` if *name* is not in
        the store or no extractor is registered for the ``conn_type``.
        """

    def enrich_all(
        self,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> builtins.list[tuple[_ConnDef, Any]]:
        """Enrich every stored connection.

        Returns one ``(conn_def, enriched)`` pair per record in the
        order :meth:`list` produces — each element matches what
        :meth:`enrich` would return for that name. Concrete on the
        abstract class because the implementation is a straightforward
        fold over :meth:`list` and :meth:`enrich` — subclasses never
        need to override it.
        """
        log_call(debug, "Logic.enrich_all()")
        return [
            self.enrich(conn_def.name, resolved_envs=resolved_envs, debug=debug)
            for conn_def in self.list(debug=debug)
        ]


class BaseLogic(Logic):
    """Reference implementation of :class:`Logic` backed by a :class:`ConnStore`."""

    def __init__(self, store: ConnStore):
        self.store = store

    def add(
        self,
        conn_file: Path,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> tuple[str, _ConnDef]:
        log_call(debug, f"BaseLogic.add(conn_file={str(conn_file)!r})")
        name = self._load_and_validate(conn_file, debug=debug)
        if self.store.exists(name, debug=debug):
            raise ValueError(f"Connection {name!r} already exists")
        self.store.put(name, conn_file, debug=debug)
        return name, self._load_def(name, resolved_envs, debug=debug)

    def update(
        self,
        name: str,
        conn_file: Path,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> _ConnDef:
        log_call(
            debug,
            f"BaseLogic.update(name={name!r}, conn_file={str(conn_file)!r})",
        )
        if not self.store.exists(name, debug=debug):
            raise ValueError(f"Connection {name!r} does not exist")
        loaded_name = self._load_and_validate(conn_file, debug=debug)
        if loaded_name != name:
            raise ValueError(
                f"Connection file declares name {loaded_name!r} but --name was {name!r}"
            )
        self.store.put(name, conn_file, debug=debug)
        return self._load_def(name, resolved_envs, debug=debug)

    def remove(self, name: str, debug: bool = False) -> None:
        log_call(debug, f"BaseLogic.remove(name={name!r})")
        if not self.store.exists(name, debug=debug):
            raise ValueError(f"Connection {name!r} does not exist")
        self.store.delete(name, debug=debug)

    def get(
        self,
        name: str,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> _ConnDef:
        log_call(
            debug,
            f"BaseLogic.get(name={name!r}, resolved_envs={resolved_envs})",
        )
        if not self.store.exists(name, debug=debug):
            raise ValueError(f"Connection {name!r} does not exist")
        return self._load_def(name, resolved_envs, debug=debug)

    def list(
        self,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> _ConnDefList:
        log_call(debug, f"BaseLogic.list(resolved_envs={resolved_envs})")
        return [
            self._load_def(name, resolved_envs, debug=debug)
            for name in self.store.names(debug=debug)
        ]

    def enrich(
        self,
        name: str,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> tuple[_ConnDef, Any]:
        # BaseLogic has nothing domain-specific to enrich, but it
        # still owns the "re-validate the stored JSON" pass that used
        # to live under ``refresh``: run the NamedConn loader (which
        # raises on malformed records) and invoke the ``prepare_conn``
        # hook so backend-specific registration stays wired up.
        log_call(
            debug,
            f"BaseLogic.enrich(name={name!r}, resolved_envs={resolved_envs})",
        )
        if not self.store.exists(name, debug=debug):
            raise ValueError(f"Connection {name!r} does not exist")
        cx_dict = self.store.get(name, debug=debug)
        conn = NamedConn(name=name)._load(cx_dict)
        self.prepare_conn(conn)
        return self._load_def(name, resolved_envs, debug=debug), None

    def get_enriched(self, name: str, debug: bool = False) -> Any:
        # Existence check mirrors the other verbs so the caller sees a
        # consistent "missing connection" error regardless of whether
        # enrichment is supported at this layer.
        log_call(debug, f"BaseLogic.get_enriched(name={name!r})")
        if not self.store.exists(name, debug=debug):
            raise ValueError(f"Connection {name!r} does not exist")
        return None

    def verify(
        self,
        name: str,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> _ConnDef:
        log_call(
            debug,
            f"BaseLogic.verify(name={name!r}, resolved_envs={resolved_envs})",
        )
        if not self.store.exists(name, debug=debug):
            raise ValueError(f"Connection {name!r} does not exist")
        # Always probe with resolved envs — the extractor opens a real
        # session and needs actual credential values. The returned
        # :class:`_ConnDef` follows the caller's ``resolved_envs``
        # preference so a UX that wants to render identity doesn't
        # accidentally leak resolved secrets into its output.
        probe_def = self._load_def(name, resolved_envs=True, debug=debug)
        if not probe_def.conn_type:
            raise ValueError(f"Connection {name!r} missing 'conn_type'")
        extractor_cls = extractor_for(probe_def.conn_type)
        extractor_cls().check_connect(probe_def.conn_configs)
        return probe_def if resolved_envs else self._load_def(name, False, debug=debug)

    def prepare_conn(self, conn) -> None:
        """Hook invoked after a connection loads successfully.

        Override in subclasses to register the resolved ``Conn`` with an
        external system (e.g. a running agent). No-op by default so that
        ``BaseLogic`` alone is a usable implementation.
        """
        return None

    def _load_def(
        self,
        name: str,
        resolved_envs: bool,
        debug: bool = False,
    ) -> _ConnDef:
        """Read the stored record and parse it into a :class:`_ConnDef`.

        ``resolved_envs`` flows straight to :meth:`ConnStore.get`, so
        env-variable substitution happens once — inside the store —
        regardless of which :class:`Logic` verb called us.
        """
        cx_dict = self.store.get(
            name,
            resolved_envs=resolved_envs,
            debug=debug,
        )
        return _ConnDef.from_dict(cx_dict)

    def _load_and_validate(self, conn_file: Path, debug: bool = False) -> str:
        """Validate a connection JSON file and return its declared name.

        Uses :meth:`NamedConn._load` as the authoritative validator so
        that the CLI rejects the exact same inputs the execution workers
        would reject at function-run time — no divergence between the
        two loaders.
        """
        log_call(
            debug,
            f"BaseLogic._load_and_validate(conn_file={str(conn_file)!r})",
        )
        if not conn_file.is_file():
            raise FileNotFoundError(f"Connection file not found: {conn_file}")
        with conn_file.open("r", encoding="utf-8") as f:
            cx_dict = json.load(f)
        if not isinstance(cx_dict, dict):
            raise ValueError(f"Expected JSON object in {conn_file}")
        name = cx_dict.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError(f"Connection file {conn_file} missing 'name'")
        conn = NamedConn(name=name)._load(cx_dict)
        self.prepare_conn(conn)
        return name
