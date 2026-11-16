<!--
Copyright 2026 Tabsdata Inc.
-->

# tabsdatak connectors

A set of Python packages under `connectors/python/tabsdata-*` in
tabsdata-os that share the `tabsdatak` namespace package. Each
connector ships as its own distribution so users can install only
the ones they need.

> **Future merge.** `tabsdatak` will eventually be folded into the
> `tabsdata` namespace (dropping the trailing `k`). The conventions
> below — Maven-style layout, `pkgutil.extend_path` `__init__.py` at
> every test level, per-connector `conftest.py` / `bootest.py` —
> are chosen to make that rename a mechanical search-and-replace
> rather than a structural overhaul.

## Packages

| Package                        | Import path                          | Depends on |
| ------------------------------ | ------------------------------------ | ---------- |
| `tabsdata-api`               | `tabsdatak.api`                     | — |
| `tabsdata-conn-common`       | `tabsdatak.conn.common`             | `tabsdata-api` |
| `tabsdata-conn-common-sql`   | `tabsdatak.conn.common.sql`         | `tabsdata-conn-common` |
| `tabsdata-conn-common-file`  | `tabsdatak.conn.common.file`        | `tabsdata-api`, `tabsdata-conn-common` |
| `tabsdata-conn-mysql`        | `tabsdatak.conn.mysql`              | `…-api`, `…-conn-common`, `…-conn-common-sql` |
| `tabsdata-conn-postgres`     | `tabsdatak.conn.postgres`           | `…-api`, `…-conn-common`, `…-conn-common-sql` |
| `tabsdata-conn-mssql`        | `tabsdatak.conn.mssql`              | `…-api`, `…-conn-common`, `…-conn-common-sql` |
| `tabsdata-conn-oracle`       | `tabsdatak.conn.oracle`             | `…-api`, `…-conn-common`, `…-conn-common-sql` |
| `tabsdata-conn-db2`          | `tabsdatak.conn.db2`                | `…-api`, `…-conn-common`, `…-conn-common-sql` |
| `tabsdata-conn-clickhouse`   | `tabsdatak.conn.clickhouse`         | `…-api`, `…-conn-common` |
| `tabsdata-conn-starrocks`    | `tabsdatak.conn.starrocks`          | `…-api`, `…-conn-common` |
| `tabsdata-conn-druid`        | `tabsdatak.conn.druid`              | `…-api`, `…-conn-common` |
| `tabsdata-conn-snowflake`    | `tabsdatak.conn.snowflake`          | `…-api`, `…-conn-common` |
| `tabsdata-conn-bigquery`     | `tabsdatak.conn.bigquery`           | `…-api`, `…-conn-common` |
| `tabsdata-conn-databricks`   | `tabsdatak.conn.databricks`         | `…-api`, `…-conn-common` |
| `tabsdata-conn-mongodb`      | `tabsdatak.conn.mongodb`            | `…-api`, `…-conn-common` |
| `tabsdata-conn-salesforce`   | `tabsdatak.conn.salesforce`         | `…-api`, `…-conn-common` |
| `tabsdata-conn-localfile`    | `tabsdatak.conn.localfile`          | `…-api`, `…-conn-common`, `…-conn-common-file` |
| `tabsdata-conn-s3`           | `tabsdatak.conn.s3`                 | `…-api`, `…-conn-common`, `…-conn-common-file` |
| `tabsdata-conn-gcs`          | `tabsdatak.conn.gcs`                | `…-api`, `…-conn-common`, `…-conn-common-file` |
| `tabsdata-conn-azureblob`    | `tabsdatak.conn.azureblob`          | `…-api`, `…-conn-common`, `…-conn-common-file` |

## Per-connector layout

Each connector follows the tabsdata-os connector shape (matches
`tabsdata-bigquery`, `tabsdata-cdc-db2`, etc.):

```
connectors/python/tabsdata-conn-<name>/
├── pyproject.toml          # setuptools, entry-points, [tool.tabsdata], ruff extends root
├── setup.py                # one-liner: tabsdatax.setup.builder.connector.build
├── setup.cfg               # egg_info
├── requirements.txt        # -r requirements/requirements-{first,third}-party.txt
├── requirements-dev.txt    # -r requirements/requirements-dev-{first,third}-party.txt
├── requirements/
│   ├── requirements-first-party.txt        # pinned sibling tabsdata-* deps
│   ├── requirements-third-party.txt        # external runtime deps
│   ├── requirements-dev-first-party.txt    # internal test deps (often empty)
│   └── requirements-dev-third-party.txt    # external test deps (docker, drivers, …)
└── src/
    ├── itest/{python,resources,scripts}/.gitkeep
    ├── main/
    │   ├── python/tabsdatak/conn/<name>/
    │   │   ├── __init__.py / _validation.py / _plugin.py / error.py
    │   │   └── assets/manifest/{VERSION, README-PyPi.md, RELEASE-NOTES, THIRD-PARTY}
    │   └── {resources,scripts}/.gitkeep
    └── utest/
        ├── python/test_tabsdatak/conn/<name>/
        │   ├── __init__.py                 # pkgutil.extend_path at every level
        │   ├── .coveragerc / pytest.ini / bootest.py / conftest.py
        │   └── test_*.py
        ├── resources/test_tabsdatak/conn/<name>/__init__.py   # pkgutil
        └── scripts/.gitkeep
```

The `tabsdatak/`, `tabsdatak/conn/`, `tabsdatak/conn/common/`,
`tabsdatak/conn/common/sql/`, and `tabsdatak/conn/common/file/`
directories contain no `__init__.py` in connector packages — they
are implicit (PEP 420) namespace packages, which lets every
connector distribution contribute subpackages to the same top-level
`tabsdatak` namespace. The single anchor
`tabsdatak/__init__.py` (with `pkgutil.extend_path`) lives in
tabsdata-os's `client/platform/src/main/python/tabsdatak/`.

The test tree IS sprinkled with `pkgutil.extend_path` `__init__.py`
files at every level — that's the forward-compat hook for the
eventual `tabsdatak` → `tabsdata` merge.

## Setting up

Set up the build environment as [Developing Tabsdata](../../DEVELOPING.md) describes. The
hydrate task, `cargo make hydrate`, installs every package of this tree, editable, into the
virtual environment. To run one package's tests, use `cargo make utest_py --packages=<name>`.

## Tree at a glance

```
connectors/python/
├── tabsdata-api/
├── tabsdata-conn-common/
├── tabsdata-conn-common-sql/
├── tabsdata-conn-common-file/
├── tabsdata-conn-mysql/
├── tabsdata-conn-postgres/
├── tabsdata-conn-mssql/
├── tabsdata-conn-oracle/
├── tabsdata-conn-db2/
├── tabsdata-conn-clickhouse/
├── tabsdata-conn-starrocks/
├── tabsdata-conn-druid/
├── tabsdata-conn-snowflake/
├── tabsdata-conn-bigquery/
├── tabsdata-conn-databricks/
├── tabsdata-conn-mongodb/
├── tabsdata-conn-salesforce/
├── tabsdata-conn-localfile/
├── tabsdata-conn-s3/
├── tabsdata-conn-gcs/
└── tabsdata-conn-azureblob/
```

Pytest markers are managed centrally — the source of truth is
`client/platform/src/utest/python/test_tabsdata/pytest.ini` in
tabsdata-os; `cargo make lock` syncs the marker list to every
root `pyproject.toml` (tabsdata-os, tabsdata-ee, tabsdata-ag) and
every per-connector `pytest.ini`.
