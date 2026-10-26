//
// Copyright 2026 Tabsdata Inc.
//

use crate::types::basic::{CollectionId, FunctionId, GroupId, ProjectId, TableId, TagEntityId};
use std::collections::BTreeMap;
use ta_objects::{Taggable, Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
use td_common::id::Id;
use td_error::{TdError, td_error};

#[td_error]
enum TagKeyError {
    #[error("Tag key '{0}' does not apply to entity type '{1}'")]
    NotApplicable(TopologyTagKey, TagEntityType) = 1,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub enum TagEntityType {
    #[typed_enum(rename = "P")]
    Project,
    #[typed_enum(rename = "G")]
    Group,
    #[typed_enum(rename = "C")]
    Collection,
    #[typed_enum(rename = "F")]
    Function,
    #[typed_enum(rename = "T")]
    Table,
}

impl Taggable for ProjectId {
    type EntityType = TagEntityType;
    const ENTITY_TYPE: TagEntityType = TagEntityType::Project;
    const ENTITY: &'static str = "project";
}

impl Taggable for GroupId {
    type EntityType = TagEntityType;
    const ENTITY_TYPE: TagEntityType = TagEntityType::Group;
    const ENTITY: &'static str = "group";
}

impl Taggable for CollectionId {
    type EntityType = TagEntityType;
    const ENTITY_TYPE: TagEntityType = TagEntityType::Collection;
    const ENTITY: &'static str = "collection";
}

impl Taggable for FunctionId {
    type EntityType = TagEntityType;
    const ENTITY_TYPE: TagEntityType = TagEntityType::Function;
    const ENTITY: &'static str = "function";
}

impl Taggable for TableId {
    type EntityType = TagEntityType;
    const ENTITY_TYPE: TagEntityType = TagEntityType::Table;
    const ENTITY: &'static str = "table";
}

impl TagEntityId {
    pub fn of<T: Typed<Inner = Id>>(id: T) -> Self {
        Self::try_new(id.into_inner()).expect("an id is a valid tag entity id")
    }

    pub fn id<T: Typed<Inner = Id>>(&self) -> Result<T, TdError> {
        T::try_new((*self).into_inner())
    }
}

/// One tagged entity: the pair every tag row addresses, built from a typed id
/// so the type and the id cannot disagree.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TagEntity {
    pub entity_type: TagEntityType,
    pub entity_id: TagEntityId,
}

impl<T: Taggable<EntityType = TagEntityType>> From<T> for TagEntity {
    fn from(id: T) -> Self {
        Self {
            entity_type: T::ENTITY_TYPE,
            entity_id: TagEntityId::of(id),
        }
    }
}

/// How a tag is written, which says when it may change.
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub enum TagKind {
    /// Set by the system at topology instants, so versioned with the topology.
    #[typed_enum(rename = "T")]
    Topology,
    /// Set by the system at lock instants, without bumping the topology.
    #[typed_enum(rename = "S")]
    System,
    #[typed_enum(rename = "U")]
    User,
}

/// The keys of [`TagKind::Topology`] tags. They start with `_`, which no
/// [`UserTagKey`] can, so system and user keys never collide.
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum TopologyTagKey {
    /// Value: the parent's stable id, of the tagged entity's type.
    #[typed_enum(rename = "_parent")]
    Parent,
}

impl TopologyTagKey {
    pub fn check_applies_to(&self, entity_type: &TagEntityType) -> Result<(), TdError> {
        let applies = match self {
            TopologyTagKey::Parent => {
                matches!(entity_type, TagEntityType::Function | TagEntityType::Table)
            }
        };
        if !applies {
            return Err(TagKeyError::NotApplicable(self.clone(), entity_type.clone()).into());
        }
        Ok(())
    }
}

/// The keys of [`TagKind::System`] tags; none yet. Its first key brings the
/// typed derives, which replace `iter`, and an insert DAO at the lock instant.
pub enum SystemTagKey {}

impl SystemTagKey {
    pub fn iter() -> std::iter::Empty<Self> {
        std::iter::empty()
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(
    regex = "^[a-z0-9][a-z0-9._/-]{0,62}$",
    expected = "a lowercase [a-z0-9._/-] key of up to 63 characters, starting with a letter or digit"
)]
pub struct UserTagKey(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(try_from = UserTagKey)]
pub struct TagKey(String);

impl From<TopologyTagKey> for TagKey {
    fn from(key: TopologyTagKey) -> Self {
        Self::try_new(key.to_string()).expect("a topology key is a valid tag key")
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(max_len = 256, default = "")]
pub struct TagValue(String);

/// The literal start of the tag keys or values a facet read returns.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(max_len = 256)]
pub struct TagPrefix(String);

pub type TopologyTags = BTreeMap<TopologyTagKey, TagValue>;

impl TagValue {
    pub fn of_id<T: Typed<Inner = Id>>(id: T) -> Self {
        Self::try_new(id.into_inner().to_string()).expect("an id is a valid tag value")
    }

    pub fn id<T: Typed<Inner = Id>>(&self) -> Result<T, TdError> {
        T::try_new(Id::try_from(self.as_str())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_topology_keys_are_no_user_keys() {
        for key in TopologyTagKey::iter() {
            assert!(key.to_string().starts_with('_'), "{key}");
            assert!(UserTagKey::try_new(key.to_string()).is_err(), "{key}");
            assert!(TagKey::try_new(key.to_string()).is_ok(), "{key}");
        }
    }

    /// Every system-written kind's keys share one `_` namespace and one open-row
    /// unique index, so no two kinds may spell a key alike: chain each new
    /// kind's keys here.
    #[test]
    fn test_system_keys_are_disjoint() {
        let keys: Vec<String> = TopologyTagKey::iter()
            .map(|key| key.to_string())
            .chain(SystemTagKey::iter().map(|key| match key {}))
            .collect();
        let distinct: HashSet<&String> = keys.iter().collect();
        assert_eq!(distinct.len(), keys.len(), "{keys:?}");
    }

    #[test]
    fn test_user_key_grammar() {
        assert!(UserTagKey::try_new("team").is_ok());
        assert!(UserTagKey::try_new("cost-center/eu.west_1").is_ok());
        assert!(UserTagKey::try_new("").is_err());
        assert!(UserTagKey::try_new("Team").is_err());
        assert!(UserTagKey::try_new(".team").is_err());
        assert!(UserTagKey::try_new("a".repeat(64)).is_err());
    }

    #[test]
    fn test_parent_applies_to_functions_and_tables_only() {
        let parent = TopologyTagKey::Parent;
        assert!(parent.check_applies_to(&TagEntityType::Function).is_ok());
        assert!(parent.check_applies_to(&TagEntityType::Table).is_ok());
        assert!(parent.check_applies_to(&TagEntityType::Project).is_err());
        assert!(parent.check_applies_to(&TagEntityType::Group).is_err());
        assert!(parent.check_applies_to(&TagEntityType::Collection).is_err());
    }
}
