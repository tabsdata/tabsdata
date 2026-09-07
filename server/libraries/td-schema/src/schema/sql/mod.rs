//
// Copyright 2026 Tabsdata Inc.
//

//! Typed SQL-generator structs. Each file owns one declarative spec plus the
//! `render` method that turns it into SQL; the low-level rendering primitives
//! and expression helpers live in `render`.

pub(crate) mod rls;
pub(crate) mod scd2;
pub mod trigger;
pub(crate) mod view;

pub(crate) use trigger::{ImmutableColumnsTriggerDef, Scd2TriggerDef, StickyTerminalsTriggerDef};
pub(crate) use view::ViewDef;

use sea_query::{DynIden, Iden, IntoIden};
use strum::IntoEnumIterator;

/// Collect every non-`Table` variant of a column [`Iden`] enum as a list of
/// [`DynIden`], in declaration order. Convention: the first variant of every
/// column enum is `Table` (the table-name marker); all subsequent variants
/// are column names, so column lists derive from the enum instead of from a
/// hand-maintained `&[&str]`.
pub(crate) fn cols_of<T>() -> Vec<DynIden>
where
    T: Iden + IntoEnumIterator + IntoIden + 'static,
{
    T::iter().skip(1).map(IntoIden::into_iden).collect()
}

/// [`cols_of`] minus the given column — the immutable set of an SCD2 event
/// log, whose `valid_to` alone stays writable for the close-out trigger.
pub(crate) fn cols_of_except<T>(except: T) -> Vec<DynIden>
where
    T: Iden + IntoEnumIterator + IntoIden + 'static,
{
    let except = except.into_iden().to_string();
    cols_of::<T>().into_iter().filter(|c| c.to_string() != except).collect()
}
