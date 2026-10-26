//
// Copyright 2026 Tabsdata Inc.
//

//! The tag filters of a listing, which [`Reader::list`](crate::Reader::list)
//! renders as semi-joins on the tags of the entities the DTO marks, at the
//! listing grain's [`tags_visible`](Grain::tags_visible).

use crate::Serves;
use crate::generic::grain::{Grain, select_in};
use crate::primitives::tags::tag_selection;
use sea_query::{PostgresQueryBuilder, SelectStatement};
use ta_objects::{Dao, ListDto, NoTags, SelectDao, SomeTags};
use td_objects::dxo::tag::TagDB;
use td_objects::list::params::ListQueryParams;
use td_objects::types::tag_selector::TagSelector;

#[diagnostic::on_unimplemented(
    message = "a listing of `{D}` read under `{G}` cannot be tag-filtered",
    note = "a DTO with `tags` fields needs a handle whose scope sees the tags: under RLS a \
            `Global` one sees none"
)]
pub trait TagFilter<D: Dao, G, E> {
    fn restrict(grain: &G, selector: &TagSelector<D::Iden>, query: &mut SelectStatement);
}

impl<D: Dao, G, E> TagFilter<D, G, E> for NoTags {
    fn restrict(_: &G, _: &TagSelector<D::Iden>, _: &mut SelectStatement) {}
}

impl<D, G, E> TagFilter<D, G, E> for SomeTags
where
    D: SelectDao,
    G: Grain<D>,
    E: Serves<TagDB>,
{
    fn restrict(grain: &G, selector: &TagSelector<D::Iden>, query: &mut SelectStatement) {
        if !selector.is_empty() {
            query.cond_where(tag_selection::<D>(selector, &grain.tags_visible()));
        }
    }
}

/// The SQL a listing runs under `grain`, tag filters included, for plan
/// guards.
pub fn list_plan_sql<T, G>(grain: &G, params: &ListQueryParams<T>) -> String
where
    T: ListDto,
    G: Grain<T::Dao>,
{
    let mut base = select_in::<T::Dao, G>(grain, ());
    if !params.tags.is_empty() {
        base.cond_where(tag_selection::<T::Dao>(&params.tags, &grain.tags_visible()));
    }
    params.apply_to(base).to_string(PostgresQueryBuilder)
}
