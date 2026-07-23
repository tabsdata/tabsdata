#
# Copyright 2026 Tabsdata Inc.
#

# Redpanda system: docker broker; a domain is a Kafka topic. Only the backend is
# system-specific; every verb runs the shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.redpanda.backend import Redpanda

__all__ = ["Redpanda"]
