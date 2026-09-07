//
// Copyright 2026 Tabsdata Inc.
//

//! The SCD2 open-interval predicate the partial indexes and pointer joins
//! share: `valid_to = 'infinity'`, one row per partition by the close-out
//! trigger's invariant.

use sea_query::{Expr, ExprTrait, IntoColumnRef, SimpleExpr};

pub fn open_interval(valid_to: impl IntoColumnRef) -> SimpleExpr {
    Expr::col(valid_to).eq(Expr::cust("'infinity'::timestamptz"))
}
