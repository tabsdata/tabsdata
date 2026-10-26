//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::crudl::ListParams;
use crate::list::params::{ListError, accepted, parse_value};
use crate::parse::IDENTIFIER_PATTERN;
use regex::Regex;
use std::sync::LazyLock;
use ta_objects::{Dao, ListDto, ListRole, SeaValue, SortDir};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Toward {
    Next,
    Previous,
}

/// A request's value of a sort key, where a keyset page starts.
pub type KeyValue = Box<dyn SeaValue>;

/// How a listing orders its rows, every key in `dir`.
#[derive(Debug)]
#[non_exhaustive]
pub struct Sort<I> {
    pub dir: SortDir,
    pub keys: SortKeys<I>,
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

impl<I: Copy> Sort<I> {
    /// The column the cursor values page through first.
    pub fn primary(&self) -> I {
        match self.keys {
            SortKeys::Tiebreak { tiebreak, .. } => tiebreak,
            SortKeys::By { by, .. } => by,
        }
    }

    pub fn toward(&self) -> Option<Toward> {
        match &self.keys {
            SortKeys::Tiebreak { after, .. } => after.as_ref().map(|(toward, _)| *toward),
            SortKeys::By { after, .. } => after.as_ref().map(|(toward, _, _)| *toward),
        }
    }
}

impl<I: Copy + Eq> Sort<I> {
    /// `order_by` is `<field>[+|-]`, ascending by default; `previous` or
    /// `next` carries the cursor's primary value, `pagination_id` the
    /// tiebreak's.
    pub fn parse<D>(params: &ListParams) -> Result<Self, ListError>
    where
        D: ListDto<Dao: Dao<Iden = I>>,
    {
        static ORDER_BY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(&format!("^(?<field>{IDENTIFIER_PATTERN})(?<dir>[+-])?$")).unwrap()
        });
        let (tiebreak, natural) = D::pagination_col();
        let (by, dir) = match params.order_by.as_deref() {
            None => (None, natural),
            Some(order_by) => {
                let caps = ORDER_BY_REGEX
                    .captures(order_by)
                    .ok_or_else(|| ListError::InvalidOrderBy(order_by.to_string()))?;
                let field = caps.name("field").unwrap().as_str();
                let by = D::column(ListRole::Order, field).ok_or_else(|| {
                    ListError::UndefinedOrderBy(field.to_string(), accepted::<D>(ListRole::Order))
                })?;
                let dir = match caps.name("dir").map(|m| m.as_str()) {
                    Some("-") => SortDir::Desc,
                    _ => SortDir::Asc,
                };
                ((by != tiebreak).then_some(by), dir)
            }
        };
        let cursor = match (&params.previous, &params.next, &params.pagination_id) {
            (Some(_), Some(_), _) => Err(ListError::PreviousAndNext)?,
            (None, None, None) => None,
            (Some(value), None, Some(id)) => Some((Toward::Previous, value, id)),
            (None, Some(value), Some(id)) => Some((Toward::Next, value, id)),
            _ => Err(ListError::MissingPaginationParams)?,
        };
        let keys = match by {
            None => SortKeys::Tiebreak {
                tiebreak,
                after: cursor
                    .map(|(toward, _, id)| Ok((toward, parse_value::<D>(&tiebreak, id)?)))
                    .transpose()?,
            },
            Some(by) => SortKeys::By {
                by,
                tiebreak,
                after: cursor
                    .map(|(toward, value, id)| {
                        Ok::<_, ListError>((
                            toward,
                            parse_value::<D>(&by, value)?,
                            parse_value::<D>(&tiebreak, id)?,
                        ))
                    })
                    .transpose()?,
            },
        };
        Ok(Self { dir, keys })
    }
}
