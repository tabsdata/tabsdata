//
// Copyright 2026 Tabs Data Inc.
//

//! Typed SQL-generator structs. Each file owns one declarative spec plus the
//! dialect-aware `render`/`into_subquery` method that turns it into SQL.
//! [`Backend`](super::backend::Backend) is passed in as a dialect parameter;
//! it stays focused on low-level primitives (`build_schema`, `build_query`)
//! and small expression helpers (`name_or_id`, `json_object_agg`, etc.).

pub(crate) mod rls;
pub(crate) mod seed_current;
pub(crate) mod trigger;
pub(crate) mod view;

pub(crate) use seed_current::SeedCurrent;
pub(crate) use trigger::{ImmutableTriggerDef, StickyTerminalsTriggerDef, TriggerDef, TriggerSpec, VersionedTriggerDef};
pub(crate) use view::ViewDef;

use sea_query::{DynIden, Iden, IntoIden};
use strum::IntoEnumIterator;

/// Collect every non-`Table` variant of a column [`Iden`] enum as a list of
/// [`DynIden`], in declaration order. Convention: the first variant of every
/// column enum is `Table` (the table-name marker); all subsequent variants
/// are column names. This helper lets `TriggerDef::columns` /
/// `SeedCurrent::columns` derive directly from the enum instead of from a
/// hand-maintained `&[&str]`.
pub(crate) fn cols_of<T>() -> Vec<DynIden>
where
    T: Iden + IntoEnumIterator + IntoIden + 'static,
{
    T::iter().skip(1).map(IntoIden::into_iden).collect()
}
