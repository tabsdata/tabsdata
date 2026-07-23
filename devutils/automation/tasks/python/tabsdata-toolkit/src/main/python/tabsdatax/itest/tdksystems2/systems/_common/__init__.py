#
# Copyright 2026 Tabsdata Inc.
#

"""Shared system logic reused by concrete systems via subclassing (backends) and
delegation (the generic command modules).

- ``sql`` — :class:`SqlContainerBackend`, the docker-container SQL-database base
  (mysql/postgres/oracle/db2/… subclass it and supply only dialect hooks).
- ``commands`` — the generic verb ``add_arguments``/``run`` implementations every
  system's command modules re-export.
"""
