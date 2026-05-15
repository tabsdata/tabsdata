from __future__ import annotations

import re

from pydantic import AfterValidator

from tabsdatak.conn.bigquery.error import BigqueryErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation

# --- Project id ------------------------------------------------------


_PROJECT_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")


def _validate_project_id(value: str) -> str:
    if not _PROJECT_RE.match(value):
        # noinspection PyProtectedMember
        raise BigqueryErrorCode.BIGQUERY_4.exception(value=value)
    return value


PROJECT_ID_VALIDATOR = AfterValidator(_validate_project_id)


# --- Dataset / table id ----------------------------------------------


_DATASET_RE = re.compile(r"^[A-Za-z0-9_]{1,1024}$")


def _validate_dataset_id(value: str) -> str:
    if not _DATASET_RE.match(value):
        # noinspection PyProtectedMember
        raise BigqueryErrorCode.BIGQUERY_5.exception(value=value)
    return value


DATASET_ID_VALIDATOR = AfterValidator(_validate_dataset_id)


def _validate_table_id(value: str) -> str:
    if not _DATASET_RE.match(value):
        # noinspection PyProtectedMember
        raise BigqueryErrorCode.BIGQUERY_6.exception(value=value)
    return value


TABLE_ID_VALIDATOR = AfterValidator(_validate_table_id)


# --- Table FQN ------------------------------------------------------

# BigQuery table references are 2-part (``dataset.table``) or 3-part
# (``project.dataset.table``). Backticks may quote each part.


def _validate_table_fqn(value: str) -> str:
    try:
        parts = _validation.split_fqn(value, quote="`")
    except ValueError as e:
        # noinspection PyProtectedMember
        raise BigqueryErrorCode.BIGQUERY_7.exception(cause=e, value=value)
    if len(parts) == 1:
        _validate_table_id(_unquote(parts[0]))
        return value
    if len(parts) == 2:
        dataset, table = parts
        _validate_dataset_id(_unquote(dataset))
        _validate_table_id(_unquote(table))
        return value
    if len(parts) == 3:
        project, dataset, table = parts
        _validate_project_id(_unquote(project))
        _validate_dataset_id(_unquote(dataset))
        _validate_table_id(_unquote(table))
        return value
    # noinspection PyProtectedMember
    raise BigqueryErrorCode.BIGQUERY_8.exception(value=value)


def _unquote(part: str) -> str:
    if len(part) >= 2 and part.startswith("`") and part.endswith("`"):
        return part[1:-1].replace("``", "`")
    return part


TABLE_FQN_VALIDATOR = AfterValidator(_validate_table_fqn)
