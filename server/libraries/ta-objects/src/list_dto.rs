//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::{Dao, Dto, FromDao};
use crate::sea_value::SeaValue;
use td_error::TdError;

/// What a listing does with a field, as its DTO marks it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListRole {
    /// `eq`/`ne`/`gt`/`ge`/`lt`/`le`/`btw`/`is`, marked `filter`.
    Filter,
    /// `lk`, marked `filter_like`.
    Like,
    /// `order_by`, marked `order_by` or `pagination_by`.
    Order,
}

/// The direction a listing sorts in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

impl SortDir {
    pub fn reversed(self) -> Self {
        match self {
            SortDir::Asc => SortDir::Desc,
            SortDir::Desc => SortDir::Asc,
        }
    }
}

/// A DTO field a listing uses, as `#[dto(list(...))]` marks it.
pub struct ListField<D: FromDao> {
    pub name: &'static str,
    pub column: <D::Dao as Dao>::Iden,
    pub roles: &'static [ListRole],
    /// A request's value of the field: a filter operand or a cursor.
    pub parse: fn(&str) -> Result<Box<dyn SeaValue>, TdError>,
    /// The field's value in a row, as a cursor.
    pub value: fn(&D) -> String,
    /// The entity whose id the field holds, when marked `tags`.
    pub tags: Option<ListTags>,
}

/// A taggable entity a list filters by, `<entity>.<key>`.
pub struct ListTags {
    pub entity: &'static str,
    /// The entity type its tag rows store.
    pub entity_type: fn() -> sea_query::Expr,
}

/// List and filter endpoint support.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot back a list endpoint",
    note = "declare `#[dto(list(on = SomeDao))]` on the DTO"
)]
pub trait ListDto: Dto + Send + FromDao + 'static {
    /// [`SomeTags`](crate::SomeTags) when a field is marked `tags`, else
    /// [`NoTags`](crate::NoTags).
    type Tags;

    const LIST_FIELDS: &'static [ListField<Self>];

    /// The field the listing pages by, unique within it, and its default
    /// direction.
    const PAGINATION_BY: (&'static ListField<Self>, SortDir);

    /// The column of `field`, when the DTO marks it for `role`.
    fn column(role: ListRole, field: &str) -> Option<<Self::Dao as Dao>::Iden> {
        Self::LIST_FIELDS
            .iter()
            .find(|f| f.name == field && f.roles.contains(&role))
            .map(|f| f.column)
    }

    /// The fields the DTO marks for `role`.
    fn fields(role: ListRole) -> Vec<&'static str> {
        Self::LIST_FIELDS
            .iter()
            .filter(|f| f.roles.contains(&role))
            .map(|f| f.name)
            .collect()
    }

    /// The column holding the id of `entity`, a `<entity>.<key>` tag filter's,
    /// and the entity type its tag rows store.
    fn tag_col(entity: &str) -> Option<(<Self::Dao as Dao>::Iden, sea_query::Expr)> {
        Self::LIST_FIELDS.iter().find_map(|f| match &f.tags {
            Some(tags) if tags.entity == entity => Some((f.column, (tags.entity_type)())),
            _ => None,
        })
    }

    /// The entities the DTO filters by the tags of.
    fn tag_entities() -> Vec<&'static str> {
        Self::LIST_FIELDS
            .iter()
            .filter_map(|f| f.tags.as_ref().map(|tags| tags.entity))
            .collect()
    }

    fn pagination_col() -> (<Self::Dao as Dao>::Iden, SortDir) {
        (Self::PAGINATION_BY.0.column, Self::PAGINATION_BY.1)
    }

    fn pagination_value(&self) -> String {
        (Self::PAGINATION_BY.0.value)(self)
    }

    /// A request's value of `column`, a field the DTO marks.
    fn parse_value(
        column: &<Self::Dao as Dao>::Iden,
        value: &str,
    ) -> Option<Result<Box<dyn SeaValue>, TdError>> {
        Self::LIST_FIELDS
            .iter()
            .find(|f| f.column == *column)
            .map(|f| (f.parse)(value))
    }

    /// The row's cursor value of `column`, an order-by field's.
    fn order_by_value(&self, column: &<Self::Dao as Dao>::Iden) -> Option<String> {
        Self::LIST_FIELDS
            .iter()
            .find(|f| f.column == *column && f.roles.contains(&ListRole::Order))
            .map(|f| (f.value)(self))
    }
}
