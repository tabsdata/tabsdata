#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake backend — a remote (non-container) warehouse.

A domain is a SCHEMA inside the account's ``DATABASE``. Connection comes from the
``SNOWFLAKE__`` environment; the data plane is inherited from
:class:`RemoteSqlBackend` over the ``snowflake`` SQLAlchemy dialect (PAT as the
URL password). Snowflake doesn't enforce foreign keys, so the employees model's
parent deletes apply cleanly (rows just orphan).
"""

from __future__ import annotations

from urllib.parse import quote_plus

from tabsdatax.itest.tdksystems2 import options
from tabsdatax.itest.tdksystems2.systems._common.sql import RemoteSqlBackend


class Snowflake(RemoteSqlBackend):
    slug = "snowflake"
    label = "Snowflake"
    domain_word = "schema"
    max_entity_name_length = 255
    driver = "snowflake"
    env_prefix = "SNOWFLAKE"
    sql_quote = '"'
    namespace_meta = "DATABASE"
    system_schemas = frozenset({"information_schema", "public"})

    def options(self) -> tuple[options.Option, ...]:
        return (
            options.Option(
                "ACCOUNT", "account", options.ENV, "env $SNOWFLAKE__ACCOUNT"
            ),
            options.Option(
                "USERNAME", "username", options.ENV, "env $SNOWFLAKE__USERNAME"
            ),
            options.Option(
                "PAT", "pat", options.ENV, "env $SNOWFLAKE__PAT", secret=True
            ),
            options.Option(
                "DATABASE", "database", options.ENV, "env $SNOWFLAKE__DATABASE"
            ),
            options.Option(
                "WAREHOUSE", "warehouse", options.ENV, "env $SNOWFLAKE__WAREHOUSE"
            ),
        )

    def _location(self, meta: dict[str, str]) -> str:
        return f"snowflake://{meta.get('ACCOUNT', '')}/{meta.get('DATABASE', '')}"

    def _schema(self, name: str) -> str:
        return name.upper()  # Snowflake folds unquoted identifiers to upper case

    def _url(self, meta: dict[str, str], schema: str = "") -> str:
        creds = f"{quote_plus(meta['USERNAME'])}:{quote_plus(meta['PAT'])}"
        tail = f"/{meta['DATABASE']}/{schema}" if schema else f"/{meta['DATABASE']}"
        return (
            f"snowflake://{creds}@{meta['ACCOUNT']}{tail}?warehouse={meta['WAREHOUSE']}"
        )
