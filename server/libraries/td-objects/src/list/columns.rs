//
// Copyright 2026 Tabsdata Inc.
//

//! The column filters of a listing. Same column → OR, otherwise AND.

use crate::list::filter::{FilterOp, IsFilter, convert_to_like_pattern};
use crate::list::params::{ListError, accepted, parse_value};
use crate::types::basic::LikeFilter;
use std::str::FromStr;
use ta_objects::{FromDao, ListDto, ListField, ListRole, SeaValue};
use td_error::TdError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug)]
pub enum ColumnTest {
    Compare(Comparison, Box<dyn SeaValue>),
    Between(Box<dyn SeaValue>, Box<dyn SeaValue>),
    Like(LikeFilter),
    Null,
    NotNull,
}

/// One column and its tests, any one of which holds.
#[derive(Debug)]
#[non_exhaustive]
pub struct ColumnTerm<I> {
    pub column: I,
    pub tests: Vec<ColumnTest>,
}

/// Every term holds.
#[derive(Debug)]
#[non_exhaustive]
pub struct ColumnSelector<I>(pub Vec<ColumnTerm<I>>);

impl<I> Default for ColumnSelector<I> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<I: Copy + Eq> ColumnSelector<I> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn add<D>(
        &mut self,
        filter: &str,
        field: &str,
        op: FilterOp,
        value: &str,
    ) -> Result<(), TdError>
    where
        D: ListDto<Dao: ta_objects::Dao<Iden = I>>,
    {
        let role = match op {
            FilterOp::Like => ListRole::Like,
            _ => ListRole::Filter,
        };
        let listed = D::field(role, field).ok_or_else(|| match role {
            ListRole::Like => {
                ListError::UndefinedLikeFilter(field.to_string(), accepted::<D>(role))
            }
            _ => ListError::UndefinedField(field.to_string(), accepted::<D>(role)),
        })?;
        let compare = |comparison| -> Result<_, TdError> {
            Ok(ColumnTest::Compare(
                comparison,
                condition_value(listed, value)?,
            ))
        };
        let test = match op {
            FilterOp::Eq => compare(Comparison::Eq)?,
            FilterOp::Ne => compare(Comparison::Ne)?,
            FilterOp::Gt => compare(Comparison::Gt)?,
            FilterOp::Ge => compare(Comparison::Ge)?,
            FilterOp::Lt => compare(Comparison::Lt)?,
            FilterOp::Le => compare(Comparison::Le)?,
            FilterOp::Like => ColumnTest::Like(convert_to_like_pattern(value)?),
            FilterOp::Between => {
                let [low, high] = value.split("::").collect::<Vec<_>>()[..] else {
                    Err(ListError::InvalidBetweenCondition(filter.to_string()))?
                };
                ColumnTest::Between(
                    condition_value(listed, low)?,
                    condition_value(listed, high)?,
                )
            }
            FilterOp::Is => match IsFilter::from_str(value)
                .map_err(|_| ListError::UndefinedIsFilter(value.to_string()))?
            {
                IsFilter::Null => ColumnTest::Null,
                IsFilter::NotNull => ColumnTest::NotNull,
            },
        };
        let column = listed.column;
        match self.0.iter_mut().find(|term| term.column == column) {
            Some(term) => term.tests.push(test),
            None => self.0.push(ColumnTerm {
                column,
                tests: vec![test],
            }),
        }
        Ok(())
    }
}

/// A compared value: the empty NULL marker would build a never-true
/// `col = NULL`.
fn condition_value<D: FromDao>(
    field: &ListField<D>,
    value: &str,
) -> Result<Box<dyn SeaValue>, ListError> {
    let value = parse_value(field, value)?;
    if value.is_sql_null() {
        return Err(ListError::EmptyFilterValue(field.name.to_string()));
    }
    Ok(value)
}
