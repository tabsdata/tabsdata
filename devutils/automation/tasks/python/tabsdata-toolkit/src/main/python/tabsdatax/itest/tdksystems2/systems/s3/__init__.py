#
# Copyright 2026 Tabsdata Inc.
#

# Amazon S3 system: object-store domains as bucket prefixes. Only the S3 storage
# backend + create command are system-specific; every other verb runs the shared
# command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.s3.backend import S3

__all__ = ["S3"]
