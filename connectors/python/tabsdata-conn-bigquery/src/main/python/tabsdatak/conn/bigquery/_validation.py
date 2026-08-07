#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.bigquery.error import BigQueryErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate

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
