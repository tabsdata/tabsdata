from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate

# --- Project / dataset patterns -------------------------------------

# Pure-regex forms used as ``value_pattern`` on the Secret
# subclasses for the BigQuery identifier-shaped fields (``project`` /
# ``dataset``). They are enforced via
# :class:`~tabsdatak._types.ValOrRef`'s ``re.fullmatch`` on the
# resolved literal / ref payload at ``value()`` time.

# Project id: 6-30 chars, lowercase letters / digits / hyphens, start
# with a letter, end alphanumeric.
PROJECT_PATTERN = r"[a-z][a-z0-9-]{4,28}[a-z0-9]"

# Dataset id: 1-1024 chars, ASCII letters / digits / underscores.
DATASET_PATTERN = r"[A-Za-z0-9_]{1,1024}"


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
BIGQUERY_TABLE_FQN_VALIDATOR = name_validate(_BIGQUERY_FQN_DEF, max_parts=3)
