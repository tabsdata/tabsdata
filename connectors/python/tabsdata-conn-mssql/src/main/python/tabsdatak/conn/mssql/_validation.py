from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate

# --- URI / ODBC connection string -----------------------------------
#
# ``MSSQLConn.uri`` accepts two shapes, distinguished by the presence
# of ``://``:
#
# * URI mode  -- ``mssql[+driver]://host[:port][/db]`` (SQLAlchemy
#   URI form). ``+pyodbc`` is the implicit default driver.
# * ODBC mode -- a raw ODBC connection string
#   ``DRIVER={...};SERVER=...;DATABASE=...``. The adapter wraps it
#   as ``mssql+pyodbc:///?odbc_connect=<urlencoded>``.
#
# Pure-regex form used as ``value_pattern`` on Var / Secret subclasses
# and as ``StringConstraints`` on the bare-str branch of ``UriSpec``.
MSSQL_URI_PATTERN = r"(?i)(?:mssql(?:\+[a-z0-9]+)?://.*|[A-Za-z][A-Za-z0-9 ]*=.+)"


# --- Table FQN ------------------------------------------------------

# MSSQL tables can be 1-, 2-, or 3-part: ``table``, ``schema.table``
# or ``database.schema.table``. (Linked-server prefixes -- 4-part
# ``server.database.schema.table`` -- are not supported.) Quoted
# parts use the bracket convention ``[name]`` with a doubled-close-
# bracket escape (``]]`` -> literal ``]``).
_MSSQL_FQN_DEF = FqnDef(
    separator=".",
    quote="[",
    close_quote="]",
    escape="]",
)

# Validator for the ``MSSQLTableNameSpec`` field.
MSSQL_TABLE_FQN_VALIDATOR = name_validate(_MSSQL_FQN_DEF, max_parts=3)
