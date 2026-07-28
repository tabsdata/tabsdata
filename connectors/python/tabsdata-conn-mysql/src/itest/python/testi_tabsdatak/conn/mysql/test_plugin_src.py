#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL source-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.mysql import MysqlDomain, mysql_domain
from tabsdatax.itest.fixture.functions.files import PluginSrcCtx
from testi_tabsdatak.conn.common.sql.employees_model import (
    DEPARTMENTS,
    EMPLOYEES,
    emp_nos,
)
from testi_tabsdatak.conn.common.sql.plugin_ctx import WatermarkSrcCtx
from testi_tabsdatak.conn.common.sql.plugin_io import entity_rows, slot_frame, work_dir
from testi_tabsdatak.conn.mysql.backend import src_conn

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import MySQLSrc, _plugin as plugin

pytestmark = pytest.mark.mysql


@mysql_domain("mysql_employees_xs")
def test_src_plugin_read_in_returns_parquet_files(
    domain_mysql_employees_xs: MysqlDomain, tmp_path: Path
):
    domain = domain_mysql_employees_xs
    src = MySQLSrc(queries=["SELECT emp_no, first_name FROM employees ORDER BY emp_no"])
    out = plugin.MySQLSrcPlugin().read_in(
        PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
    )
    assert len(out) == 1
    assert slot_frame(out)["emp_no"].to_list() == emp_nos(
        entity_rows(domain, EMPLOYEES)
    )


@mysql_domain("mysql_employees_xs")
def test_src_plugin_read_in_multiple_queries(
    domain_mysql_employees_xs: MysqlDomain, tmp_path: Path
):
    domain = domain_mysql_employees_xs
    all_emp_nos = emp_nos(entity_rows(domain, EMPLOYEES))
    cursor = all_emp_nos[len(all_emp_nos) // 2 - 1]
    # The one bind is substituted into every query naming it, and each query gets
    # its own output slot -- including one reading a different table.
    src = MySQLSrc(
        queries=[
            "SELECT emp_no FROM employees WHERE emp_no > :cursor ORDER BY emp_no",
            "SELECT emp_no FROM employees WHERE emp_no <= :cursor ORDER BY emp_no",
            "SELECT dept_no FROM departments ORDER BY dept_no",
        ],
        initial_values={"cursor": cursor},
    )
    out = plugin.MySQLSrcPlugin().read_in(
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


@mysql_domain("mysql_employees_xs")
def test_src_plugin_read_in_resumes_from_advanced_initial_values(
    domain_mysql_employees_xs: MysqlDomain, tmp_path: Path
):
    domain = domain_mysql_employees_xs
    all_emp_nos = emp_nos(entity_rows(domain, EMPLOYEES))
    cursor = all_emp_nos[len(all_emp_nos) // 2 - 1]
    src = MySQLSrc(
        queries=["SELECT emp_no FROM employees WHERE emp_no > :cursor ORDER BY emp_no"],
        initial_values={"cursor": cursor},
    )
    ctx = WatermarkSrcCtx(work_dir(tmp_path))

    first = slot_frame(plugin.MySQLSrcPlugin().read_in(ctx, src_conn(domain), src))
    assert first["emp_no"].to_list() == [n for n in all_emp_nos if n > cursor]

    # What a publisher does with the watermark it just consumed; the next run must
    # read from it rather than from the seed, so it sees nothing new. A query
    # matching no rows writes no chunk, so its slot comes back empty.
    ctx.set_attr("cursor", all_emp_nos[-1])
    assert plugin.MySQLSrcPlugin().read_in(ctx, src_conn(domain), src) == [[]]

    # A run whose ctx never stored the bind falls back to the seed.
    again = slot_frame(
        plugin.MySQLSrcPlugin().read_in(
            WatermarkSrcCtx(work_dir(tmp_path, "rerun")), src_conn(domain), src
        )
    )
    assert again["emp_no"].to_list() == first["emp_no"].to_list()
