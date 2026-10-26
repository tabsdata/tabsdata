//
// Copyright 2026 Tabsdata Inc.
//

//! The `tag.` filters of a listing, which [`Reader::list`](crate::Reader::list)
//! renders as semi-joins on the tags read at the listing's own grain.

use crate::Serves;
use crate::generic::grain::{Current, Grain};
use crate::primitives::tags::tag_selection;
use sea_query::{Condition, SelectStatement};
use ta_objects::TaggedDao;
use td_objects::dxo::tag::TagDB;
use td_objects::list::params::Untagged;
use td_objects::types::tag_selector::TagSelector;

/// The grains a listing's `tag.` filters read under: the tags are read at the
/// same grain as the rows they filter.
#[diagnostic::on_unimplemented(
    message = "tag filters cannot be read under `{Self}`",
    note = "tag filters read the tags at the listing's own grain; only `scd2()` reads have one"
)]
pub trait SeesTags: Grain<TagDB> {}

impl SeesTags for Current {}

#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot filter a listing of `{D}` read under `{G}`",
    note = "tag filters need a DAO declared `#[dao(tagged = ..)]`, a grain that sees tags, \
            and a handle whose scope sees the tags: under RLS a `Global` one sees none"
)]
pub trait TagFilter<D, G, E> {
    fn restrict(&self, grain: &G, query: &mut SelectStatement);
}

impl<D, G, E> TagFilter<D, G, E> for Untagged {
    fn restrict(&self, _: &G, _: &mut SelectStatement) {}
}

impl<D, G, E> TagFilter<D, G, E> for TagSelector
where
    D: TaggedDao,
    G: SeesTags,
    E: Serves<TagDB>,
{
    fn restrict(&self, grain: &G, query: &mut SelectStatement) {
        let visible = <G as Grain<TagDB>>::condition(grain).unwrap_or_else(Condition::all);
        query.cond_where(tag_selection::<D>(self, &visible));
    }
}
