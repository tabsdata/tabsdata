#
# Copyright 2026 Tabsdata Inc.
#

"""The hostname a container system advertises in its descriptor.

A container is published on a host port, and by default the descriptor says that
port lives on the loopback address — true only for the machine that ran docker. It
is ``127.0.0.1`` rather than the name ``localhost``: the SQL Server ODBC driver
connects to the machine's own hostname for ``localhost``, and a network's DNS can
resolve that hostname to another device, so every connection times out. ``create
--sys-hostname HOST`` binds another name here, and the backend plans its endpoints
around it, so the descriptors (and every domain URI derived from them) are usable
from other machines: a system store copied elsewhere, or a connector running off
the box. Backends that never publish an endpoint (object stores, warehouses,
credential-only services, the local filesystem) do not mix this in — ``create``
tells the user the flag has nothing to bind to.
"""

from __future__ import annotations

DEFAULT_HOSTNAME = "127.0.0.1"


class HostMixin:
    hostname: str = DEFAULT_HOSTNAME

    def use_hostname(self, host: str) -> None:
        """Bind the hostname this create advertises (see ``Backend.plan_system``)."""
        self.hostname = host
