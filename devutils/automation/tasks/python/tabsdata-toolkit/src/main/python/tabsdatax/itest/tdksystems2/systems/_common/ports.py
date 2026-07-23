#
# Copyright 2026 Tabsdata Inc.
#

"""A pool of candidate host ports a backend draws from when it needs any.

When many systems are created in parallel, the batch dispatcher pre-finds a
*disjoint* pool of free ports for each one (``--ports``) and passes it in, so two
creates running at once never pick the same free port. A backend takes ports
through :meth:`PortPoolMixin.next_free_port` — from the bound pool while it lasts,
then falling back to finding a fresh free port. Backends that need no ports
simply never call it and ignore the pool.
"""

from __future__ import annotations

from collections.abc import Iterable

from tabsdatax.itest.tdksystems2.shell import find_free_port


class PortPoolMixin:
    _port_pool: tuple[int, ...] = ()
    _port_taken: int = 0

    def use_ports(self, ports: Iterable[int]) -> None:
        """Bind a pool of pre-found candidate ports for this create."""
        self._port_pool = tuple(int(p) for p in ports)
        self._port_taken = 0

    def next_free_port(self) -> int:
        """The next candidate port from the bound pool, or a freshly found free
        port once the pool is exhausted (or none was bound)."""
        if self._port_taken < len(self._port_pool):
            port = self._port_pool[self._port_taken]
            self._port_taken += 1
            return port
        return find_free_port()
