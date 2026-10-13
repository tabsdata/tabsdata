//
// Copyright 2026 Tabsdata Inc.
//

//! Expressions over PostgreSQL's own catalog and session, for the checks the
//! pools run on themselves.

use sea_query::{Alias, Expr, ExprTrait, Func, IntoIden, Keyword, Query};

pub(crate) fn current_user() -> Expr {
    Expr::Keyword(Keyword::Custom(Alias::new("current_user").into_iden()))
}

pub(crate) fn current_setting(name: &str) -> Expr {
    Func::cust("current_setting").arg(name).into()
}

pub(crate) fn catalog_has(catalog: &str, column: &str, name: &str) -> Expr {
    Expr::exists(
        Query::select()
            .expr(Expr::val(1))
            .from(Alias::new(catalog))
            .and_where(Expr::col(Alias::new(column)).eq(name))
            .take(),
    )
}

pub(crate) fn role_exists(role: &str) -> Expr {
    catalog_has("pg_roles", "rolname", role)
}

pub(crate) fn has_role(member: impl Into<Expr>, role: &str, privilege: &str) -> Expr {
    Func::cust("pg_has_role")
        .args([member.into(), Expr::val(role), Expr::val(privilege)])
        .into()
}

/// `then`, evaluated only when `when` holds; `FALSE` otherwise. Guards a
/// check that errors on a role that does not exist.
pub(crate) fn only_if(when: Expr, then: Expr) -> Expr {
    Expr::case(when, then).finally(Expr::val(false)).into()
}
