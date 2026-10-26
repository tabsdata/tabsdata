//
// Copyright 2026 Tabsdata Inc.
//

//! The entities that carry tags, and the list DTOs that can be filtered by
//! them: `<entity>.<key>:<op>:<value>` on a list whose DTO marks a field of a
//! taggable id `#[dto(list(tags))]`.

use crate::{SeaValue, Typed};
use td_common::id::Id;

/// The id of an entity that carries tags.
pub trait Taggable: Typed<Inner = Id> + Copy {
    type EntityType: SeaValue + Clone;

    /// The stored entity type of its tag rows.
    const ENTITY_TYPE: Self::EntityType;

    /// The entity's name in tag filters: `function` in `function.team:eq:a`.
    const ENTITY: &'static str;
}

/// A list DTO with no `tags` field: its listings take no tag filters.
pub struct NoTags;

/// A list DTO with `tags` fields: its listings read the tags table.
pub struct SomeTags;
