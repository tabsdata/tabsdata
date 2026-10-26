//
// Copyright 2026 Tabsdata Inc.
//

use std::str::FromStr;
use ta_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
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
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
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

impl TagKey {
    pub fn topology(&self) -> Option<TopologyTagKey> {
        TopologyTagKey::from_str(self).ok()
    }
}

impl From<TopologyTagKey> for TagKey {
    fn from(key: TopologyTagKey) -> Self {
        Self::try_new(key.to_string()).expect("a topology key is a valid tag key")
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(max_len = 256, default = "")]
pub struct TagValue(String);

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
    fn test_stored_keys_name_their_topology_key() {
        assert_eq!(
            TagKey::from(TopologyTagKey::Parent).topology(),
            Some(TopologyTagKey::Parent)
        );
        let user = TagKey::try_from(UserTagKey::try_new("team").unwrap()).unwrap();
        assert_eq!(user.topology(), None);
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
