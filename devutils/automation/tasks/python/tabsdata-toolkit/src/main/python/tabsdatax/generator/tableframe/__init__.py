#
# Copyright 2026 Tabs Data Inc.
#

"""TableFrame wrapper-API generator (``tdx_gtableframe``).

Generates a delegating, whitelist-driven wrapper API over the Polars lazy API
(the ``tabsdatak.tableframe`` surface). Introspects whatever Polars is installed
in the active environment, so it runs in the target env (pinned polars), not the
toolkit's own deps. CLI subcommands: ``manifest`` / ``compare`` / ``generate``.
"""
