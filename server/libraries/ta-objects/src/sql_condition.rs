//
// Copyright 2026 Tabsdata Inc.
//

//! Type-safe, composable SQL WHERE conditions.
//!
//! Leaf values ([`SeaValue`] newtypes) resolve to a single `column = value`
//! expression through the [`HasColumn::COL`] associated const, so a condition
//! on a type the queried DAO doesn't contain is a compile error, not a
//! runtime one. `and!` / `or!` (value position) and `And!` / `Or!` (type
//! position) compose conditions into arbitrarily nested trees producing a
//! [`sea_query::Condition`]; DAOs expose named filters as inherent fns
//! returning `impl SqlCondition<Self>`.

use crate::dxo::{Dao, HasColumn};
use crate::sea_value::{NullableSeaValue, SeaValue};
use sea_query::{Expr, ExprTrait};

/// Trait for values that can appear in a SQL WHERE clause of a query on the
/// DAO `D`.
///
/// Leaf types produce a single `col = val` (the column is `D`'s
/// [`HasColumn::COL`] for the leaf's type, so a leaf only implements the
/// trait for DAOs that contain it); combinators ([`And`](And2) /
/// [`Or`](Or2)) compose children with AND / OR.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be used as a condition on `{D}`",
    note = "every leaf of the condition must be a field type of the queried DAO"
)]
pub trait SqlCondition<D: Dao>: Send + Sync {
    fn resolve(&self) -> sea_query::Condition;

    /// Whether this condition is the empty "match all rows" predicate
    /// (i.e. `()`). Destructive ops (`delete`, `update`) check this and
    /// refuse to run without a real WHERE clause.
    fn matches_all(&self) -> bool {
        false
    }

    /// Column name → display value pairs for error messages.
    fn display_values(&self) -> Vec<(String, String)> {
        vec![]
    }
}

pub fn leaf_display_values<D: HasColumn<T>, T: SeaValue>(sv: &T) -> Vec<(String, String)> {
    vec![(sea_query::Iden::to_string(&D::COL), sv.display_value())]
}

pub fn resolve_leaf_eq<D: HasColumn<T>, T: SeaValue>(sv: &T) -> sea_query::Condition {
    sea_query::Condition::all().add(Expr::col(D::COL).eq(sv.as_sea_expr()))
}

impl<D: Dao, T: SqlCondition<D> + ?Sized> SqlCondition<D> for &T {
    fn resolve(&self) -> sea_query::Condition {
        (**self).resolve()
    }

    fn matches_all(&self) -> bool {
        (**self).matches_all()
    }

    fn display_values(&self) -> Vec<(String, String)> {
        (**self).display_values()
    }
}

impl<D: Dao> SqlCondition<D> for () {
    fn resolve(&self) -> sea_query::Condition {
        sea_query::Condition::all()
    }

    fn matches_all(&self) -> bool {
        true
    }
}

/// Delegate through boxes — `Box<dyn SqlCondition<D>>` is the owned,
/// DAO-typed form for plumbing that must *store* a condition (e.g.
/// per-walk graph configs).
impl<D: Dao, T: SqlCondition<D> + ?Sized> SqlCondition<D> for Box<T> {
    fn resolve(&self) -> sea_query::Condition {
        (**self).resolve()
    }

    fn matches_all(&self) -> bool {
        (**self).matches_all()
    }

    fn display_values(&self) -> Vec<(String, String)> {
        (**self).display_values()
    }
}

/// `Option<T>`: `Some(v)` matches `col = v`; `None` matches `col IS NULL`
/// (a bound `= NULL` never matches in SQL, so equality would make a `None`
/// condition unsatisfiable). Keyed on the DAO's `Option<T>` column — the
/// nullable column's declared field type.
impl<D, T> SqlCondition<D> for Option<T>
where
    D: Dao + HasColumn<Option<T>>,
    T: NullableSeaValue + Send + Sync + 'static,
{
    fn resolve(&self) -> sea_query::Condition {
        match self {
            Some(_) => resolve_leaf_eq::<D, _>(self),
            None => sea_query::Condition::all()
                .add(Expr::col(<D as HasColumn<Option<T>>>::COL).is_null()),
        }
    }

    fn display_values(&self) -> Vec<(String, String)> {
        leaf_display_values::<D, _>(self)
    }
}

/// Generates a single combinator struct (e.g. `And2` or `Or3`) with its
/// `SqlCondition` impl.
macro_rules! define_combinator {
    ($Name:ident, $cond_fn:ident, $n:tt, $($T:ident),+) => {
        paste::paste! {
            #[derive(Debug, Clone)]
            pub struct [<$Name $n>]<$($T),+>($(pub $T),+);

            #[allow(non_snake_case)]
            impl<D: crate::dxo::Dao, $($T: SqlCondition<D>),+> SqlCondition<D> for [<$Name $n>]<$($T),+> {
                fn resolve(&self) -> sea_query::Condition {
                    let [<$Name $n>]($($T),+) = self;
                    let mut cond = sea_query::Condition::$cond_fn();
                    $(cond = cond.add($T.resolve());)+
                    cond
                }

                fn display_values(&self) -> Vec<(String, String)> {
                    let [<$Name $n>]($($T),+) = self;
                    let mut values = Vec::new();
                    $(values.extend($T.display_values());)+
                    values
                }
            }
        }
    };
}

macro_rules! define_and_or {
    ($n:tt, $($T:ident),+) => {
        define_combinator!(And, all, $n, $($T),+);
        define_combinator!(Or, any, $n, $($T),+);
    };
}

define_and_or!(1, T);
define_and_or!(2, T1, T2);
define_and_or!(3, T1, T2, T3);
define_and_or!(4, T1, T2, T3, T4);

/// Type-position macro: `And![A, B]` → `And2<A, B>`.
#[allow(non_snake_case)]
#[macro_export]
macro_rules! And {
    ($t:ty $(,)?) => { $crate::sql_condition::And1<$t> };
    ($t1:ty, $t2:ty $(,)?) => { $crate::sql_condition::And2<$t1, $t2> };
    ($t1:ty, $t2:ty, $t3:ty $(,)?) => { $crate::sql_condition::And3<$t1, $t2, $t3> };
    ($t1:ty, $t2:ty, $t3:ty, $t4:ty $(,)?) => { $crate::sql_condition::And4<$t1, $t2, $t3, $t4> };
}

/// Type-position macro: `Or![A, B]` → `Or2<A, B>`.
#[allow(non_snake_case)]
#[macro_export]
macro_rules! Or {
    ($t:ty $(,)?) => { $crate::sql_condition::Or1<$t> };
    ($t1:ty, $t2:ty $(,)?) => { $crate::sql_condition::Or2<$t1, $t2> };
    ($t1:ty, $t2:ty, $t3:ty $(,)?) => { $crate::sql_condition::Or3<$t1, $t2, $t3> };
    ($t1:ty, $t2:ty, $t3:ty, $t4:ty $(,)?) => { $crate::sql_condition::Or4<$t1, $t2, $t3, $t4> };
}

/// Value-position macro: `and!(a, b)` → `And2(a, b)`.
#[macro_export]
macro_rules! and {
    ($e:expr $(,)?) => {
        $crate::sql_condition::And1($e)
    };
    ($e1:expr, $e2:expr $(,)?) => {
        $crate::sql_condition::And2($e1, $e2)
    };
    ($e1:expr, $e2:expr, $e3:expr $(,)?) => {
        $crate::sql_condition::And3($e1, $e2, $e3)
    };
    ($e1:expr, $e2:expr, $e3:expr, $e4:expr $(,)?) => {
        $crate::sql_condition::And4($e1, $e2, $e3, $e4)
    };
}

/// Value-position macro: `or!(a, b)` → `Or2(a, b)`.
#[macro_export]
macro_rules! or {
    ($e:expr $(,)?) => {
        $crate::sql_condition::Or1($e)
    };
    ($e1:expr, $e2:expr $(,)?) => {
        $crate::sql_condition::Or2($e1, $e2)
    };
    ($e1:expr, $e2:expr, $e3:expr $(,)?) => {
        $crate::sql_condition::Or3($e1, $e2, $e3)
    };
    ($e1:expr, $e2:expr, $e3:expr, $e4:expr $(,)?) => {
        $crate::sql_condition::Or4($e1, $e2, $e3, $e4)
    };
}

/// Less-than-or-equal condition: produces `col <= val`.
///
/// Works like a leaf [`SqlCondition`] but uses `<=` instead of `=`:
/// ```text
/// and!(table_id, lte!(triggered_on))  →  table_id = ? AND triggered_on <= ?
/// ```
#[derive(Debug, Clone)]
pub struct Lte<T>(pub T);

impl<D: HasColumn<T>, T: SeaValue> SqlCondition<D> for Lte<T> {
    fn resolve(&self) -> sea_query::Condition {
        sea_query::Condition::all().add(Expr::col(D::COL).lte(self.0.as_sea_expr()))
    }

    fn display_values(&self) -> Vec<(String, String)> {
        leaf_display_values::<D, _>(&self.0)
    }
}

/// Value-position macro: `lte!(a)` → `Lte(a)`.
#[macro_export]
macro_rules! lte {
    ($e:expr) => {
        $crate::sql_condition::Lte($e)
    };
}

/// Negation condition: produces `NOT (inner_condition)`.
///
/// Wraps any [`SqlCondition`] and negates it:
/// ```text
/// and!(table_id, not!(status))  →  table_id = ? AND NOT (status = ?)
/// ```
#[derive(Debug, Clone)]
pub struct Not<T>(pub T);

impl<D: Dao, T: SqlCondition<D>> SqlCondition<D> for Not<T> {
    fn resolve(&self) -> sea_query::Condition {
        self.0.resolve().not()
    }

    fn display_values(&self) -> Vec<(String, String)> {
        self.0.display_values()
    }
}

/// Type-position macro: `Not![T]` → `Not<T>`.
#[allow(non_snake_case)]
#[macro_export]
macro_rules! Not {
      ($t:ty $(,)?) => { $crate::sql_condition::Not<$t> };
  }

/// Value-position macro: `not!(a)` → `Not(a)`.
#[macro_export]
macro_rules! not {
    ($e:expr) => {
        $crate::sql_condition::Not($e)
    };
}
