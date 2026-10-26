//
// Copyright 2026 Tabsdata Inc.
//

//! The SCD2 predicates the partial indexes and the referrer joins share.
//! `open_interval` is `valid_to = 'infinity'`, one row per partition by the
//! close-out trigger's invariant; a referrer joins its timeline's latest
//! version and labels it by name while open, by id once closed.

use sea_query::{Alias, Condition, DynIden, Expr, ExprTrait, Func, IntoColumnRef, IntoIden, Query, SimpleExpr};

pub fn open_interval(valid_to: impl IntoColumnRef) -> SimpleExpr {
    Expr::col(valid_to).eq(Expr::cust("'infinity'::timestamptz"))
}

/// Joins `versions` (an alias of `table`, an SCD2 table) to the last
/// version of the timeline `referrer` points at: the open one while the
/// timeline is open, the last one once it is closed. Versions order by
/// `(valid_from, id)`, the close-out trigger's own order for same-instant
/// versions, so exactly one row joins.
pub(crate) fn last_version_of(
    table: impl IntoIden,
    versions: impl IntoIden,
    partition: impl IntoIden,
    referrer: impl IntoColumnRef,
) -> Condition {
    let versions = versions.into_iden();
    let partition = partition.into_iden();
    let later: DynIden = Alias::new("later").into_iden();
    let order = |alias: &DynIden| {
        Expr::tuple([
            Expr::col((alias.clone(), Alias::new("valid_from"))),
            Expr::col((alias.clone(), Alias::new("id"))),
        ])
    };
    let no_later_version = Query::select()
        .expr(Expr::val(1))
        .from_as(table, later.clone())
        .and_where(Expr::col((later.clone(), partition.clone())).equals((versions.clone(), partition.clone())))
        .and_where(order(&later).gt(order(&versions)))
        .to_owned();
    Condition::all().add(Expr::col((versions, partition)).equals(referrer)).add(Expr::not_exists(no_later_version))
}

/// The name on the `versions` alias a [`last_version_of`] join brought,
/// while its timeline is open; NULL once it is closed.
pub(crate) fn open_name(versions: impl IntoIden, name: impl IntoIden) -> SimpleExpr {
    let versions = versions.into_iden();
    Expr::case(open_interval((versions.clone(), Alias::new("valid_to"))), Expr::col((versions, name.into_iden()))).into()
}

/// How a row labels the timeline `referrer` points at: its name while open,
/// `[<id>]` once closed.
pub(crate) fn label_of(versions: impl IntoIden, name: impl IntoIden, referrer: impl IntoColumnRef) -> SimpleExpr {
    Func::coalesce([
        open_name(versions, name),
        Expr::cust_with_exprs("'[' || $1 || ']'", [Expr::col(referrer)]),
    ])
    .into()
}

/// Whether the timeline a [`last_version_of`] join brought is closed:
/// its latest version has an end, or the referrer points at nothing.
pub(crate) fn closed_version(versions: impl IntoIden) -> SimpleExpr {
    let versions = versions.into_iden();
    Expr::col((versions.clone(), Alias::new("id"))).is_null().or(open_interval((versions, Alias::new("valid_to"))).not())
}
