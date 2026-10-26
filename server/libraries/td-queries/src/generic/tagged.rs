//
// Copyright 2026 Tabsdata Inc.
//

use crate::Serves;
use ta_objects::{NoTags, SomeTags};
use td_objects::dxo::tag::TagDB;

/// A listing whose DTO marks `tags` fields reads the tags table: only an
/// executor that serves it may run one.
#[diagnostic::on_unimplemented(
    message = "a tag-filtered listing cannot be read through `{E}`",
    note = "a DTO with `tags` fields needs a handle whose scope sees the tags: under RLS a \
            `Global` one sees none"
)]
pub trait TagFilter<E> {}

impl<E> TagFilter<E> for NoTags {}

impl<E: Serves<TagDB>> TagFilter<E> for SomeTags {}
