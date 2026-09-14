#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

from pydantic import AfterValidator

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate, unfqn
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode

# --- Identifier / Account patterns ----------------------------------

# Each pattern is paired with the ValueConstraints that binds it to an
# error code. The field spec annotates itself with the constraints, so
# one rule gates both arms of a str-or-secret union.
IDENTIFIER_PATTERN = r'^(?:[A-Za-z_][A-Za-z0-9_$]*|"[^"\x00]+(?:""[^"\x00]+)*")$'

IDENTIFIER_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(IDENTIFIER_PATTERN, SnowflakeErrorCode.SNOWFLAKE_10)
)
ACCOUNT_PATTERN = r"^(?:[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9])$"

ACCOUNT_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(ACCOUNT_PATTERN, SnowflakeErrorCode.SNOWFLAKE_9)
)

# --- Table FQN ------------------------------------------------------

# Snowflake tables can be referenced as ``table``, ``schema.table`` or
# ``database.schema.table``. Double-quoted identifiers may contain
# arbitrary chars and use the doubled-quote convention
# (``"weird ""name"``) to embed literal double-quotes.
_SNOWFLAKE_FQN_DEF = FqnDef(separator=".", quote='"', escape='"')

# Validator for the ``SnowflakeTableNameSpec`` field. Built once from
# ``_SNOWFLAKE_FQN_DEF`` so per-field annotations stay declarative.
SNOWFLAKE_TABLE_FQN_VALIDATION = name_validate(
    _SNOWFLAKE_FQN_DEF, max_parts=3, syntax="Snowflake"
)

# --- Merge ----------------------------------------------------------

# The two aliases the generated MERGE binds its sides to. The source is a
# session-scoped staging table whose name the connector picks, so nothing
# outside the statement can name either side.
MERGE_TARGET_ALIAS = "target"
MERGE_SOURCE_ALIAS = "source"

# A column is a single Snowflake identifier: the ``IDENTIFIER_PATTERN`` shape
# without the FQN separator, so a dotted value is rejected rather than silently
# read as a qualified reference.
_COLUMN_RE = re.compile(IDENTIFIER_PATTERN)


def _column_validate(value: str) -> str:
    if not _COLUMN_RE.match(value):
        raise ValueError(
            f"not a Snowflake column identifier (unquoted, or double-quoted"
            f" for a case-sensitive / special-char name): {value!r}"
        )
    return value


# Validator for the ``MergeColumnSpec`` field.
MERGE_COLUMN_VALIDATOR = AfterValidator(_column_validate)


def merge_column_key(name: str) -> str:
    """The stored form of a merge column name, unquoted.

    Snowflake upper-folds an unquoted identifier, and the tables the connector
    creates are templated from ``infer_schema(ignore_case => true)``, so their
    columns are stored upper-cased whatever case the parquet used. Folding an
    unquoted name the same way lets a user write ``on=["order_id"]`` and still
    hit ``ORDER_ID``; a double-quoted name is case-sensitive and kept verbatim.
    """
    (part,) = unfqn(name, _SNOWFLAKE_FQN_DEF)
    return part if name.startswith('"') else part.upper()
