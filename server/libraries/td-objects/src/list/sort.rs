//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::crudl::ListParams;
use crate::list::params::{ListError, ListIden, accepted, parse_value};
use crate::parse::IDENTIFIER_PATTERN;
use regex::Regex;
use std::sync::LazyLock;
use ta_objects::{ListDto, ListField, ListRole, SeaValue, SortDir};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Toward {
    Next,
    Previous,
}

/// A request's value of a sort key, where a keyset page starts.
pub type KeyValue = Box<dyn SeaValue>;

/// How a listing orders its rows, every key in `dir`: by `field` first.
#[non_exhaustive]
pub struct Sort<D: ListDto> {
    pub field: &'static ListField<D>,
    pub dir: SortDir,
    pub keys: SortKeys<ListIden<D>>,
}

/// The ORDER BY columns, and the cursor's value of each when the request
/// pages: the rows past them toward the next or the previous page. The
/// tiebreak is the DTO's pagination column, unique within the listing.
#[derive(Debug)]
pub enum SortKeys<I> {
    Tiebreak {
        tiebreak: I,
        after: Option<(Toward, KeyValue)>,
    },
    /// A requested column, then the tiebreak.
    By {
        by: I,
        tiebreak: I,
        after: Option<(Toward, KeyValue, KeyValue)>,
    },
}

impl<D: ListDto> Sort<D> {
    pub fn toward(&self) -> Option<Toward> {
        match &self.keys {
            SortKeys::Tiebreak { after, .. } => after.as_ref().map(|(toward, _)| *toward),
            SortKeys::By { after, .. } => after.as_ref().map(|(toward, _, _)| *toward),
        }
    }

    /// `order_by` is `<field>[+|-]`, ascending by default; `previous` or
    /// `next` carries the cursor's value of the field, `pagination_id` the
    /// tiebreak's.
    pub fn parse(params: &ListParams) -> Result<Self, ListError> {
        static ORDER_BY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(&format!("^(?<field>{IDENTIFIER_PATTERN})(?<dir>[+-])?$")).unwrap()
        });
        let (tiebreak, natural) = D::PAGINATION_BY;
        let (field, dir) = match params.order_by.as_deref() {
            None => (tiebreak, natural),
            Some(order_by) => {
                let caps = ORDER_BY_REGEX
                    .captures(order_by)
                    .ok_or_else(|| ListError::InvalidOrderBy(order_by.to_string()))?;
                let name = caps.name("field").unwrap().as_str();
                let field = D::field(ListRole::Order, name).ok_or_else(|| {
                    ListError::UndefinedOrderBy(name.to_string(), accepted::<D>(ListRole::Order))
                })?;
                let dir = match caps.name("dir").map(|m| m.as_str()) {
                    Some("-") => SortDir::Desc,
                    _ => SortDir::Asc,
                };
                (field, dir)
            }
        };
        let cursor = match (&params.previous, &params.next, &params.pagination_id) {
            (Some(_), Some(_), _) => Err(ListError::PreviousAndNext)?,
            (None, None, None) => None,
            (Some(value), None, Some(id)) => Some((Toward::Previous, value, id)),
            (None, Some(value), Some(id)) => Some((Toward::Next, value, id)),
            _ => Err(ListError::MissingPaginationParams)?,
        };
        let keys = if field.column == tiebreak.column {
            SortKeys::Tiebreak {
                tiebreak: tiebreak.column,
                after: cursor
                    .map(|(toward, _, id)| Ok((toward, parse_value(tiebreak, id)?)))
                    .transpose()?,
            }
        } else {
            SortKeys::By {
                by: field.column,
                tiebreak: tiebreak.column,
                after: cursor
                    .map(|(toward, value, id)| {
                        Ok::<_, ListError>((
                            toward,
                            parse_value(field, value)?,
                            parse_value(tiebreak, id)?,
                        ))
                    })
                    .transpose()?,
            }
        };
        Ok(Self { field, dir, keys })
    }
}
