//
// Copyright 2026 Tabsdata Inc.
//

use std::fmt::Display;
use ta_objects::ComposedString;
use td_error::{ApiError, TdError, api_error};

/// How a row names an entity it points at: the entity's name while it
/// exists, its bracketed id (`[<id>]`) once it is deleted.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Label<N, I> {
    Named(N),
    Deleted(I),
}

impl<N: Clone, I: Display> Label<N, I> {
    /// The name it labels; an error once the entity is deleted.
    pub fn name(&self) -> Result<N, TdError> {
        match self {
            Label::Named(name) => Ok(name.clone()),
            Label::Deleted(id) => Err(api_error!(ApiError::NotFound, "'{id}' was deleted")),
        }
    }
}

impl<N, I> From<N> for Label<N, I> {
    fn from(name: N) -> Self {
        Label::Named(name)
    }
}

impl<N, I> ComposedString for Label<N, I>
where
    N: Display + for<'a> TryFrom<&'a str, Error = TdError>,
    I: Display + for<'a> TryFrom<&'a str, Error = TdError>,
{
    fn parse(s: impl Into<String>) -> Result<Self, TdError> {
        let s = s.into();
        match s.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            Some(id) => Ok(Label::Deleted(I::try_from(id)?)),
            None => Ok(Label::Named(N::try_from(s.as_str())?)),
        }
    }

    fn compose(&self) -> String {
        match self {
            Label::Named(name) => name.to_string(),
            Label::Deleted(id) => format!("[{id}]"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Label;
    use crate::types::basic::{CollectionId, CollectionName};
    use ta_objects::ComposedString;

    type CollectionLabel = Label<CollectionName, CollectionId>;

    #[test]
    fn a_label_round_trips_a_name_and_a_deleted_id() {
        let named = CollectionLabel::parse("sales").unwrap();
        assert_eq!(
            named,
            Label::Named(CollectionName::try_from("sales").unwrap())
        );
        assert_eq!(named.compose(), "sales");

        let id = CollectionId::default();
        let deleted = CollectionLabel::parse(format!("[{id}]")).unwrap();
        assert_eq!(deleted, Label::Deleted(id));
        assert_eq!(deleted.compose(), format!("[{id}]"));
        assert!(deleted.name().is_err());
    }
}
