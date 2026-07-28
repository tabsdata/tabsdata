#
# Copyright 2026 Tabsdata Inc.
#

"""Databricks source-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.databricks import (
    DatabricksDomain,
    databricks_domain,
)
from tabsdatax.itest.fixture.functions.files import PluginSrcCtx
from testi_tabsdatak.conn.common.sql.employees_model import (
    DEPARTMENTS,
    EMPLOYEES,
    emp_nos,
)
from testi_tabsdatak.conn.common.sql.plugin_ctx import WatermarkSrcCtx
from testi_tabsdatak.conn.common.sql.plugin_io import entity_rows, slot_frame, work_dir
from testi_tabsdatak.conn.databricks.backend import src_conn

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import DatabricksSrc, _plugin as plugin
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksRuntimeException,
)

pytestmark = pytest.mark.databricks


@databricks_domain("databricks_employees_xs")
def test_src_plugin_read_in_returns_parquet_files(
    domain_databricks_employees_xs: DatabricksDomain, tmp_path: Path
):
    domain = domain_databricks_employees_xs
    src = DatabricksSrc(
        queries=["SELECT emp_no, first_name FROM employees ORDER BY emp_no"]
    )
    out = plugin._DatabricksSrcPlugin().read_in(
        PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
    )
    assert len(out) == 1
    assert slot_frame(out)["emp_no"].to_list() == emp_nos(
        entity_rows(domain, EMPLOYEES)
    )


@databricks_domain("databricks_employees_xs")
def test_src_plugin_read_in_multiple_queries(
    domain_databricks_employees_xs: DatabricksDomain, tmp_path: Path
):
    domain = domain_databricks_employees_xs
    all_emp_nos = emp_nos(entity_rows(domain, EMPLOYEES))
    cursor = all_emp_nos[len(all_emp_nos) // 2 - 1]
    # The one bind is substituted into every query naming it, and each query gets
    # its own output slot -- including one reading a different table.
    src = DatabricksSrc(
        queries=[
            "SELECT emp_no FROM employees WHERE emp_no > :cursor ORDER BY emp_no",
            "SELECT emp_no FROM employees WHERE emp_no <= :cursor ORDER BY emp_no",
            "SELECT dept_no FROM departments ORDER BY dept_no",
        ],
        initial_values={"cursor": cursor},
    )
    out = plugin._DatabricksSrcPlugin().read_in(
        PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
    )
    assert len(out) == 3
    assert slot_frame(out, 0)["emp_no"].to_list() == [
        n for n in all_emp_nos if n > cursor
    ]
    assert slot_frame(out, 1)["emp_no"].to_list() == [
        n for n in all_emp_nos if n <= cursor
    ]
    assert slot_frame(out, 2).height == entity_rows(domain, DEPARTMENTS)


@databricks_domain("databricks_employees_xs")
def test_src_plugin_read_in_binds_several_names_in_one_query(
    domain_databricks_employees_xs: DatabricksDomain, tmp_path: Path
):
    domain = domain_databricks_employees_xs
    all_emp_nos = emp_nos(entity_rows(domain, EMPLOYEES))
    low, high = all_emp_nos[1], all_emp_nos[-2]
    # Every name in the seed becomes its own statement parameter, so a query can
    # bind more than one; each is typed from its Python value independently.
    src = DatabricksSrc(
        queries=[
            "SELECT emp_no FROM employees WHERE emp_no > :low AND emp_no <= :high "
            "ORDER BY emp_no"
        ],
        initial_values={"low": low, "high": high},
    )
    out = plugin._DatabricksSrcPlugin().read_in(
        PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
    )
    assert slot_frame(out)["emp_no"].to_list() == [
        n for n in all_emp_nos if low < n <= high
    ]


@databricks_domain("databricks_employees_xs")
def test_src_plugin_read_in_resumes_from_advanced_initial_values(
    domain_databricks_employees_xs: DatabricksDomain, tmp_path: Path
):
    domain = domain_databricks_employees_xs
    all_emp_nos = emp_nos(entity_rows(domain, EMPLOYEES))
    cursor = all_emp_nos[len(all_emp_nos) // 2 - 1]
    src = DatabricksSrc(
        queries=["SELECT emp_no FROM employees WHERE emp_no > :cursor ORDER BY emp_no"],
        initial_values={"cursor": cursor},
    )
    ctx = WatermarkSrcCtx(work_dir(tmp_path))

    first = slot_frame(
        plugin._DatabricksSrcPlugin().read_in(ctx, src_conn(domain), src)
    )
    assert first["emp_no"].to_list() == [n for n in all_emp_nos if n > cursor]

    # What a publisher does with the watermark it just consumed; the next run must
    # read from it rather than from the seed, so it sees nothing new. The statement
    # manifest types a zero-row result, so the slot still holds a typed empty
    # parquet rather than nothing at all.
    ctx.set_attr("cursor", all_emp_nos[-1])
    empty = slot_frame(
        plugin._DatabricksSrcPlugin().read_in(ctx, src_conn(domain), src)
    )
    assert empty.height == 0
    assert empty.columns == ["emp_no"]

    # A run whose ctx never stored the bind falls back to the seed.
    again = slot_frame(
        plugin._DatabricksSrcPlugin().read_in(
            WatermarkSrcCtx(work_dir(tmp_path, "rerun")), src_conn(domain), src
        )
    )
    assert again["emp_no"].to_list() == first["emp_no"].to_list()


@databricks_domain("databricks_employees_xs")
def test_src_plugin_read_in_advances_cursor_to_max_row_read(
    domain_databricks_employees_xs: DatabricksDomain, tmp_path: Path
):
    # The watermark loop a publisher actually runs: read a window, store the
    # largest value the read observed, then read again. The two runs must partition
    # the table -- no row read twice, none skipped -- which only holds if the second
    # run binds the stored maximum and not the seed.
    domain = domain_databricks_employees_xs
    all_emp_nos = emp_nos(entity_rows(domain, EMPLOYEES))
    seed = all_emp_nos[0] - 1
    window_end = all_emp_nos[len(all_emp_nos) // 2 - 1]
    windowed = DatabricksSrc(
        queries=[
            "SELECT emp_no FROM employees WHERE emp_no > :cursor "
            f"AND emp_no <= {window_end} ORDER BY emp_no"
        ],
        initial_values={"cursor": seed},
    )
    rest_of_table = DatabricksSrc(
        queries=["SELECT emp_no FROM employees WHERE emp_no > :cursor ORDER BY emp_no"],
        initial_values={"cursor": seed},
    )
    ctx = WatermarkSrcCtx(work_dir(tmp_path))

    first = slot_frame(
        plugin._DatabricksSrcPlugin().read_in(ctx, src_conn(domain), windowed)
    )
    assert first["emp_no"].to_list() == [n for n in all_emp_nos if n <= window_end]

    ctx.set_attr("cursor", first["emp_no"].max())
    rest = slot_frame(
        plugin._DatabricksSrcPlugin().read_in(ctx, src_conn(domain), rest_of_table)
    )
    assert rest["emp_no"].to_list() == [n for n in all_emp_nos if n > window_end]


@pytest.mark.parametrize(
    "initial_values",
    [
        pytest.param({"other": 1}, id="seed-omits-bind"),
        pytest.param(None, id="no-seed"),
    ],
)
@databricks_domain("databricks_employees_xs")
def test_src_plugin_read_in_bind_absent_from_initial_values_fails(
    domain_databricks_employees_xs: DatabricksDomain,
    initial_values: dict | None,
    tmp_path: Path,
):
    # Only the names in `initial_values` are sent as statement parameters, so a
    # `:name` the seed does not carry stays unbound and the warehouse -- not the
    # adapter -- rejects the statement; the plugin recognises that failure and
    # names the bind.
    domain = domain_databricks_employees_xs
    src = DatabricksSrc(
        queries=["SELECT emp_no FROM employees WHERE emp_no > :cursor"],
        initial_values=initial_values,
    )
    with pytest.raises(DatabricksRuntimeException) as ei:
        plugin._DatabricksSrcPlugin().read_in(
            PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_12
    assert "'cursor'" in str(ei.value)
    assert "UNBOUND_SQL_PARAMETER" in str(ei.value.__cause__)


@databricks_domain("databricks_employees_xs")
def test_src_plugin_read_in_ignores_seed_no_query_binds(
    domain_databricks_employees_xs: DatabricksDomain, tmp_path: Path
):
    # The mirror case: a seeded name no query binds is sent as a parameter anyway
    # and the warehouse ignores it, so an over-declared seed is not an error.
    domain = domain_databricks_employees_xs
    src = DatabricksSrc(
        queries=["SELECT emp_no FROM employees ORDER BY emp_no"],
        initial_values={"never_bound": 42},
    )
    out = plugin._DatabricksSrcPlugin().read_in(
        PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
    )
    assert slot_frame(out)["emp_no"].to_list() == emp_nos(
        entity_rows(domain, EMPLOYEES)
    )
