#
# Copyright 2026 Tabsdata Inc.
#

"""The structural contract every system backend satisfies.

Command modules receive a backend as ``ctx.system`` and call these methods; a
new system is a class that implements this Protocol (plus its command files).
``meta`` dicts are the descriptor's connection settings (see ``descriptor``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from tabsdatax.itest.tdksystems2.datagen import Transaction
    from tabsdatax.itest.tdksystems2.emit import Table


class Backend(Protocol):
    slug: str
    label: str
    kind: str  # SYSTEM_KIND baked into the descriptor (v1: sql/file/service/…)
    domain_word: str
    max_entity_name_length: int
    weight: int  # 1..3 scheduling weight; batch runs the heaviest targets first

    # Managed venvs (see ``_venv``): the env a verb re-execs into. ``sys_env``
    # covers the verbs that only touch the system from the outside (create /
    # destroy / status / restart / fetch-bin); ``dom_env`` the ones that reach
    # inside it — every domain verb, plus ``purge``.
    sys_env: str
    dom_env: str

    # capability flags — the dispatcher gates verb availability on these.
    is_container: bool
    supports_purge: bool
    supports_models: bool
    supports_data: bool
    supports_changes: bool

    def docker_images(self) -> list[tuple[str, str]]: ...

    def configure(self, meta: dict[str, str]) -> None:
        """Bind a resolved descriptor's ``meta`` to the backend before its verb
        runs, so a backend that would otherwise read its config from the
        environment at use time (object stores) works from the descriptor alone
        — the only environment variable ever read at use time is the crypto key.
        Optional: backends that already build every op from ``meta`` don't
        implement it (the dispatcher binds only when present)."""

    def options(self) -> tuple:
        """Declarative create options (see ``options.Option``); ``()`` = none."""

    def synth_default(self, name: str, sys_name: str) -> str:
        """The SYNTH-source default for option `name` (backends with such options
        implement it)."""

    def use_ports(self, ports: list[int]) -> None:
        """Bind a pool of pre-found candidate host ports for this create (the
        dispatcher hands each parallel create a disjoint pool so they don't race
        for the same free port). Only port-using backends implement it."""

    def next_free_port(self) -> int:
        """A host port for the create: the next from the bound pool, else a freshly
        found free one. Only port-using backends implement it."""

    # system lifecycle. `overrides` carries the resolved create options (see
    # `options`) — every declared option is present, so the backend reads its
    # connection settings from here rather than the environment.
    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]: ...
    def provision_system(
        self, meta: dict[str, str], *, force: bool = False
    ) -> dict[str, str]: ...
    def destroy_system(self, meta: dict[str, str]) -> None: ...
    def system_status(self, meta: dict[str, str]) -> tuple[bool, str]: ...
    def restart_system(self, meta: dict[str, str]) -> str: ...

    # domain lifecycle
    def new_domain_password(self) -> str: ...
    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]: ...
    def provision_domain(
        self,
        sys_meta: dict[str, str],
        entity_name: str,
        domain_meta: dict[str, str],
        *,
        force: bool = False,
    ) -> None: ...
    def destroy_domain(self, domain_meta: dict[str, str]) -> None: ...
    def domain_status(self, domain_meta: dict[str, str]) -> tuple[bool, str]: ...
    def list_domain_entities(self, sys_meta: dict[str, str]) -> list[str]: ...
    def purge_domain_entity(self, sys_meta: dict[str, str], name: str) -> None: ...

    # data plane
    def data_plane_uri(self, meta: dict[str, str]) -> str:
        """The opaque handle the generic data-plane commands pass to
        ``install_schema`` / ``load_tables`` / ``apply_changes`` — the SQLAlchemy
        URL for SQL backends, the staging destination for file backends."""

    def has_model(self, model: str) -> bool: ...
    def available_models(self) -> list[str]: ...
    def install_schema(
        self, meta: dict[str, str], model: str, *, force: bool = False
    ) -> None: ...
    def load_tables(self, uri: str, tables: list[Table]) -> dict[str, int]:
        """Load `tables`; return the ``entities`` to record — ``{table: rows}`` for
        SQL, ``{file_path: rows}`` for file backends."""

    def apply_changes(
        self, uri: str, transactions: list[Transaction], before: dict[str, int]
    ) -> dict[str, int]:
        """Apply `transactions` and return the new ``entities`` map (SQL: `before`
        adjusted by the net row deltas; file: `before` plus the new files)."""

    def drop_all_tables(self, uri: str) -> None: ...
