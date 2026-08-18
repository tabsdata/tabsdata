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

## Prerequisites

- Python 3.12+
- Working tabsdata-os checkout (`../tabsdata-os/` relative to this
  repo)

## Installing into a conda environment

Create the conda env, then install each package editable with
`--no-deps` so pip does not try to resolve sibling packages from
PyPI. Third-party deps (pydantic, pytest, …) are installed
separately. Order matters only for readability — `--no-deps` skips
resolution — but the layering is:
`api` → `conn-common` / `conn-common-sql` / `conn-common-file` → connectors.

```bash
conda create -n tabsdatak python=3.12.13 -y
conda activate tabsdatak
pip install --upgrade pip

pip install pytest pydantic

# From the tabsdata-os repo root:
pip install \
    -e connectors/python/tabsdata-api \
    -e connectors/python/tabsdata-conn-common \
    -e connectors/python/tabsdata-conn-common-sql \
    -e connectors/python/tabsdata-conn-common-file \
    -e connectors/python/tabsdata-conn-mysql \
    -e connectors/python/tabsdata-conn-postgres \
    -e connectors/python/tabsdata-conn-mssql \
    -e connectors/python/tabsdata-conn-oracle \
    -e connectors/python/tabsdata-conn-db2 \
    -e connectors/python/tabsdata-conn-clickhouse \
    -e connectors/python/tabsdata-conn-starrocks \
    -e connectors/python/tabsdata-conn-druid \
    -e connectors/python/tabsdata-conn-snowflake \
    -e connectors/python/tabsdata-conn-bigquery \
    -e connectors/python/tabsdata-conn-databricks \
    -e connectors/python/tabsdata-conn-mongodb \
    -e connectors/python/tabsdata-conn-salesforce \
    -e connectors/python/tabsdata-conn-localfile \
    -e connectors/python/tabsdata-conn-s3 \
    -e connectors/python/tabsdata-conn-gcs \
    -e connectors/python/tabsdata-conn-azureblob \
    -e connectors/python/tabsdata-scratchpad
```

Or use `cargo make` from the tabsdata-os repo root — that's the
build path CI takes. Per-connector requirements files
(`requirements.txt` / `requirements-dev.txt`) are wired into the
top-level tabsdata-os `requirements-dev.txt` aggregator.

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
├── tabsdata-conn-azureblob/
└── tabsdata-scratchpad/
```

Pytest markers are managed centrally — the source of truth is
`client/platform/src/utest/python/test_tabsdata/pytest.ini` in
tabsdata-os; `cargo make lock` syncs the marker list to every
root `pyproject.toml` (tabsdata-os, tabsdata-ee, tabsdata-ag) and
every per-connector `pytest.ini`.
