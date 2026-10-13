//
// Copyright 2026 Tabsdata Inc.
//

//! The one door for SQL text built by code (sea-query, td-schema, tests),
//! never from input. Every other statement is `'static` with bound values.

use sqlx::AssertSqlSafe;

pub fn generated(sql: String) -> AssertSqlSafe<String> {
    AssertSqlSafe(sql)
}
