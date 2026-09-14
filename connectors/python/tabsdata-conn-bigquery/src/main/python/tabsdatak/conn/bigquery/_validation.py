#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.bigquery.error import BigQueryErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate, unfqn

# --- Project / dataset patterns -------------------------------------

# Each pattern is paired with the ValueConstraints that binds it to an
# error code. The field spec annotates itself with the constraints, so
# one rule gates both arms of a str-or-secret union.

# Project id: 6-30 chars, lowercase letters / digits / hyphens, start
# with a letter, end alphanumeric.
PROJECT_PATTERN = r"^(?:[a-z][a-z0-9-]{4,28}[a-z0-9])$"

PROJECT_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(PROJECT_PATTERN, BigQueryErrorCode.BIGQUERY_9)
)

# Dataset id: 1-1024 chars, ASCII letters / digits / underscores.
DATASET_PATTERN = r"^(?:[A-Za-z0-9_]{1,1024})$"

DATASET_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(DATASET_PATTERN, BigQueryErrorCode.BIGQUERY_10)
)

# --- Table FQN ------------------------------------------------------

# BigQuery tables can be referenced as ``project.dataset.table``.
# Unlike most SQL systems, BigQuery wraps the *whole* FQN in a single
# pair of backticks (``outer_only=True``) rather than per-part, e.g.
# ```my-project.dataset.table```. This is what enables project ids
# (which legitimately contain hyphens) to round-trip through a SQL
# parser without per-part quoting. A literal backtick inside the FQN
# is written with a backslash escape (``\``` -> ``` ` ```), per the
# BigQuery lexer; doubled-backtick is *not* a valid escape here.
_BIGQUERY_FQN_DEF = FqnDef(separator=".", quote="`", escape="\\", outer_only=True)

# Validator for the ``BigQueryTableNameSpec`` field. Up to 3 parts:
# ``project.dataset.table``.
BIGQUERY_TABLE_FQN_VALIDATION = name_validate(
    _BIGQUERY_FQN_DEF, max_parts=3, syntax="BigQuery"
)

# --- Merge ----------------------------------------------------------

# A merge column is one column name, never a path. A standard BigQuery
# column name takes letters, digits and underscores, starts with a
# letter or underscore, and is at most 300 characters; a flexible column
# name (anything else BigQuery admits) is written backtick-quoted. A dot
# is out either way -- it would read as a qualified reference.
_UNQUOTED_COLUMN_PART = r"[A-Za-z_][A-Za-z0-9_]{0,299}"
_BACKTICK_COLUMN_PART = (
    r"`[^`.\\\s\x00-\x1f\x7f]"
    r"(?:[^`.\\\x00-\x1f\x7f]{0,298}[^`.\\\s\x00-\x1f\x7f])?`"
)

COLUMN_PATTERN = rf"^(?:{_UNQUOTED_COLUMN_PART}|{_BACKTICK_COLUMN_PART})$"

COLUMN_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(COLUMN_PATTERN, BigQueryErrorCode.BIGQUERY_25)
)

# A column name is a single part, so it quotes per part -- the
# ``outer_only`` wrapping ``_BIGQUERY_FQN_DEF`` uses for a table path
# would demand backticks around a bare column name.
_BIGQUERY_COLUMN_FQN_DEF = FqnDef(separator=".", quote="`", escape="\\")


def merge_column_key(name: str) -> str:
    """The canonical form of a merge column name: unquoted and lower-cased.

    BigQuery resolves column names case-insensitively and refuses a table two
    columns whose names differ only in case, and backticks quote a flexible
    column name without making it case-sensitive -- so ``order_id``,
    ``ORDER_ID`` and ```OrderId``` all name one column. Folding to a single case
    is therefore the only way to tell the marker column apart from a key
    whichever case either was written in.

    Note this is a canonical *comparison* key, not the name BigQuery stores: a
    column keeps the case it was defined with, which a config cannot know. That
    differs from Snowflake, where an unquoted name really is upper-folded on the
    way in and a quoted one really is case-sensitive.
    """
    (part,) = unfqn(name, _BIGQUERY_COLUMN_FQN_DEF)
    return str(part).lower()
