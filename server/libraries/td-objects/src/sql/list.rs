//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::ListParams;
use crate::types::parse::IDENTIFIER_PATTERN;
use crate::types::ListQuery;
use getset::Getters;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashSet;
use std::marker::PhantomData;
use td_error::td_error;

#[td_error]
pub enum ListError {
    #[error("Invalid order-by value, it must be <NAME>+/-: {0}")]
    InvalidOrderBy(String) = 0,
    #[error(
        "Invalid condition value, it must be <NAME><OPERATOR><VALUE> (operators are {0}): {1}"
    )]
    InvalidCondition(String, String) = 1,
    #[error("Undefined filter: {0}")]
    UndefinedFilter(String) = 2,
    #[error("Undefined like filter: {0}")]
    UndefinedLikeFilter(String) = 3,
    #[error("Undefined order by: {0}")]
    UndefinedOrderBy(String) = 4,
    #[error("Previous and Next parameters cannot be used together")]
    PreviousAndNext = 5,
    #[error("Natural Id must be use in pagination with Previous or Next parameters")]
    MissingPaginationParams = 6,
    #[error("Invalid between condition '{0}', it must be <NAME>:btw:<min>::<max>")]
    InvalidBetweenCondition(String) = 7,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Order {
    Asc(String),
    Desc(String),
}

impl Order {
    pub fn invert(&self) -> Self {
        match self {
            Order::Asc(field) => Order::Desc(field.clone()),
            Order::Desc(field) => Order::Asc(field.clone()),
        }
    }

    pub fn field(&self) -> &str {
        match self {
            Order::Asc(field) => field,
            Order::Desc(field) => field,
        }
    }

    pub fn direction(&self) -> &str {
        match self {
            Order::Asc(_) => "ASC",
            Order::Desc(_) => "DESC",
        }
    }

    pub fn asc(field: impl Into<String>) -> Self {
        Self::Asc(field.into())
    }

    pub fn desc(field: impl Into<String>) -> Self {
        Self::Desc(field.into())
    }

    fn parse(s: &str) -> Result<Order, ListError> {
        const ORDER_BY_PATTERN: &str = constcat::concat!(
            "^(?<field>",
            IDENTIFIER_PATTERN,
            ")(?<direction>(\\+|\\-))?$"
        );

        lazy_static! {
            static ref ORDER_BY_REGEX: Regex = Regex::new(ORDER_BY_PATTERN).unwrap();
        }

        if let Some(captures) = ORDER_BY_REGEX.captures(s) {
            let field = captures.name("field").unwrap().as_str().to_string();
            let direction = captures
                .name("direction")
                .map(|m| m.as_str())
                .unwrap_or("+")
                == "+";
            if direction {
                Ok(Order::Asc(field))
            } else {
                Ok(Order::Desc(field))
            }
        } else {
            Err(ListError::InvalidOrderBy(s.to_string()))
        }
    }
}

impl TryFrom<&str> for Order {
    type Error = ListError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Condition {
    Eq(String, String),
    Ne(String, String),
    Lk(String, String),
    Gt(String, String),
    Ge(String, String),
    Lt(String, String),
    Le(String, String),
    Btw(String, String, String),
}

impl Condition {
    pub fn eq(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Eq(field.into(), value.into())
    }

    pub fn ne(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Ne(field.into(), value.into())
    }

    pub fn gt(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Gt(field.into(), value.into())
    }

    pub fn ge(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Ge(field.into(), value.into())
    }

    pub fn lt(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Lt(field.into(), value.into())
    }

    pub fn le(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Le(field.into(), value.into())
    }

    pub fn lk(field: impl Into<String>, value: impl Into<String>) -> Self {
        // Replace '*' with '%' for SQL LIKE wildcard operator. URL cannot contain '%'.
        let value = value.into().replace("*", "%");
        Self::Lk(field.into(), value)
    }

    pub fn btw(field: impl Into<String>, min: impl Into<String>, max: impl Into<String>) -> Self {
        Self::Btw(field.into(), min.into(), max.into())
    }

    fn parse(s: &str) -> Result<Self, ListError> {
        const EQ: &str = ":eq:";
        const NE: &str = ":ne:";
        const GT: &str = ":gt:";
        const GE: &str = ":ge:";
        const LT: &str = ":lt:";
        const LE: &str = ":le:";
        const LK: &str = ":lk:";
        const BTW: &str = ":btw:";

        const OPERATORS: &str =
            constcat::concat!(EQ, "|", NE, "|", GT, "|", GE, "|", LT, "|", LE, "|", LK, "|", BTW);
        const CONDITION_PATTERN: &str = constcat::concat!(
            "^(?<field>",
            IDENTIFIER_PATTERN,
            ")(?<operator>(",
            OPERATORS,
            "))(?<value>(.*))$"
        );

        lazy_static! {
            static ref CONDITION_REGEX: Regex = Regex::new(CONDITION_PATTERN).unwrap();
        }

        if let Some(captures) = CONDITION_REGEX.captures(s) {
            let field = captures.name("field").unwrap().as_str().to_string();
            let operator = captures.name("operator").unwrap().as_str().to_string();
            let value = captures.name("value").unwrap().as_str().to_string();
            let condition = match operator.as_str() {
                EQ => Self::eq(field, value),
                NE => Self::ne(field, value),
                GT => Self::gt(field, value),
                GE => Self::ge(field, value),
                LT => Self::lt(field, value),
                LE => Self::le(field, value),
                LK => Self::lk(field, value),
                BTW => {
                    let min_max = value.split("::").collect::<Vec<_>>();
                    if min_max.len() != 2 {
                        return Err(ListError::InvalidBetweenCondition(s.to_string()));
                    }
                    Self::btw(field, min_max[0], min_max[1])
                }
                _ => {
                    return Err(ListError::InvalidCondition(
                        OPERATORS.to_string(),
                        s.to_string(),
                    ))
                }
            };
            Ok(condition)
        } else {
            Err(ListError::InvalidCondition(
                OPERATORS.to_string(),
                s.to_string(),
            ))
        }
    }

    pub fn field(&self) -> &str {
        match self {
            Condition::Eq(field, _) => field,
            Condition::Ne(field, _) => field,
            Condition::Lk(field, _) => field,
            Condition::Gt(field, _) => field,
            Condition::Ge(field, _) => field,
            Condition::Lt(field, _) => field,
            Condition::Le(field, _) => field,
            Condition::Btw(field, _, _) => field,
        }
    }

    pub fn values(&self) -> Vec<&str> {
        match self {
            Condition::Eq(_, value) => vec![value],
            Condition::Ne(_, value) => vec![value],
            Condition::Lk(_, value) => vec![value],
            Condition::Gt(_, value) => vec![value],
            Condition::Ge(_, value) => vec![value],
            Condition::Lt(_, value) => vec![value],
            Condition::Le(_, value) => vec![value],
            Condition::Btw(_, min, max) => vec![min, max],
        }
    }

    pub fn operator(&self) -> &str {
        match self {
            Condition::Eq(_, _) => "=",
            Condition::Ne(_, _) => "!=",
            Condition::Lk(_, _) => "LIKE",
            Condition::Gt(_, _) => ">",
            Condition::Ge(_, _) => ">=",
            Condition::Lt(_, _) => "<",
            Condition::Le(_, _) => "<=",
            Condition::Btw(_, _, _) => "BETWEEN",
        }
    }

    pub fn connector(&self) -> &str {
        match self {
            Condition::Eq(_, _)
            | Condition::Ne(_, _)
            | Condition::Lk(_, _)
            | Condition::Gt(_, _)
            | Condition::Ge(_, _)
            | Condition::Lt(_, _)
            | Condition::Le(_, _) => "",
            Condition::Btw(_, _, _) => "AND",
        }
    }

    pub fn cardinality(&self) -> usize {
        match self {
            Condition::Eq(_, _)
            | Condition::Ne(_, _)
            | Condition::Lk(_, _)
            | Condition::Gt(_, _)
            | Condition::Ge(_, _)
            | Condition::Lt(_, _)
            | Condition::Le(_, _) => 1,
            Condition::Btw(_, _, _) => 2,
        }
    }
}

impl TryFrom<&str> for Condition {
    type Error = ListError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Debug, Clone, Eq, Hash)]
pub struct OrConditions(pub Vec<Condition>);

impl OrConditions {
    pub fn conditions(&self) -> &[Condition] {
        &self.0
    }
}

impl PartialEq for OrConditions {
    fn eq(&self, other: &Self) -> bool {
        let this = self.0.iter().collect::<HashSet<&Condition>>();
        let other = other.0.iter().collect::<HashSet<&Condition>>();
        this == other
    }
}

impl<T> From<T> for OrConditions
where
    T: IntoIterator<Item = Condition>,
{
    fn from(or_conditions: T) -> Self {
        OrConditions(or_conditions.into_iter().collect())
    }
}

#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Debug, Clone, Eq, Hash)]
pub struct AndConditions(pub Vec<OrConditions>);

impl AndConditions {
    pub fn conditions(&self) -> &[OrConditions] {
        &self.0
    }
}

impl PartialEq for AndConditions {
    fn eq(&self, other: &Self) -> bool {
        let this = self.0.iter().collect::<HashSet<&OrConditions>>();
        let other = other.0.iter().collect::<HashSet<&OrConditions>>();
        this == other
    }
}

impl<T> From<T> for AndConditions
where
    T: Into<Vec<OrConditions>>,
{
    fn from(or_conditions: T) -> Self {
        AndConditions(or_conditions.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Pagination {
    Previous(String, String),
    Next(String, String),
}

impl Pagination {
    pub fn column_value(&self) -> &str {
        match self {
            Pagination::Previous(column_value, _) => column_value,
            Pagination::Next(column_value, _) => column_value,
        }
    }

    pub fn pagination_id(&self) -> &str {
        match self {
            Pagination::Previous(_, pagination_id) => pagination_id,
            Pagination::Next(_, pagination_id) => pagination_id,
        }
    }
}

#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct ListQueryParams<D: ListQuery> {
    len: usize,
    conditions: AndConditions,
    natural_order: Order,
    order: Option<Order>,
    pagination: Option<Pagination>,
    phantom: PhantomData<D>,
}

impl<D: ListQuery> TryFrom<&ListParams> for ListQueryParams<D> {
    type Error = ListError;
    fn try_from(value: &ListParams) -> Result<Self, Self::Error> {
        let conditions = value
            .filter()
            .iter()
            .map(String::as_str)
            .map(TryInto::try_into)
            .collect::<Result<Vec<Condition>, _>>()?;
        conditions
            .iter()
            .map(|c| match c {
                Condition::Lk(field, _) => {
                    if !D::filter_by_like_fields().contains(&field.as_str()) {
                        Err(ListError::UndefinedLikeFilter(field.to_string()))
                    } else {
                        Ok(())
                    }
                }
                c => {
                    if !D::filter_by_fields().contains(&c.field()) {
                        Err(ListError::UndefinedFilter(c.field().to_string()))
                    } else {
                        Ok(())
                    }
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        let or_condition_groups = conditions
            .into_iter()
            .map(|c| (c.field().to_string(), c))
            .into_group_map();
        let conditions: AndConditions = or_condition_groups
            .values()
            .cloned()
            .map(OrConditions::from)
            .collect::<Vec<_>>()
            .into();

        let order = match value.order_by() {
            Some(o) => {
                let o = o.as_str().try_into()?;
                match o {
                    Order::Asc(field) | Order::Desc(field)
                        if !D::order_by_fields().contains(&field.as_str()) =>
                    {
                        Err(ListError::UndefinedOrderBy(field.to_string()))
                    }
                    _ => Ok(Some(o)),
                }
            }
            None => Ok(None),
        }?;

        let default_pagination_order = Order::parse(D::pagination_by())?;
        let natural_order = match &order {
            Some(o) => match o {
                Order::Asc(o) if o != D::pagination_by() => {
                    Order::asc(default_pagination_order.field())
                }
                Order::Desc(o) if o != D::pagination_by() => {
                    Order::desc(default_pagination_order.field())
                }
                _ => default_pagination_order,
            },
            None => default_pagination_order,
        };

        // Column value applies to order-by column, or natural-order-by column if order-by is empty.
        let pagination = match (value.previous(), value.next(), value.pagination_id()) {
            (Some(_), Some(_), _) => Err(ListError::PreviousAndNext),
            (Some(_), _, None) => Err(ListError::MissingPaginationParams),
            (_, Some(_), None) => Err(ListError::MissingPaginationParams),
            (None, None, Some(_)) => Err(ListError::MissingPaginationParams),
            (Some(column_value), None, Some(pagination_id)) => Ok(Some(Pagination::Previous(
                column_value.to_string(),
                pagination_id.to_string(),
            ))),
            (None, Some(column_value), Some(pagination_id)) => Ok(Some(Pagination::Next(
                column_value.to_string(),
                pagination_id.to_string(),
            ))),
            _ => Ok(None),
        }?;

        Ok(ListQueryParams {
            len: *value.len(),
            conditions,
            natural_order,
            order,
            pagination,
            phantom: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crudl::ListParamsBuilder;

    #[test]
    fn test_order_parse() {
        assert!(Order::parse("").is_err());
        assert!(Order::parse("-").is_err());
        assert!(Order::parse("a=").is_err());
        assert_eq!(Order::parse("a").unwrap(), Order::asc("a"));
        assert_eq!(Order::parse("a+").unwrap(), Order::asc("a"));
        assert_eq!(Order::parse("a-").unwrap(), Order::desc("a"));
    }

    #[test]
    fn test_condition_parse() {
        assert!(Condition::parse("").is_err());
        assert!(Condition::parse("a").is_err());
        assert!(Condition::parse(":eq:").is_err());
        assert!(Condition::parse("a:ff:b").is_err());
        assert_eq!(Condition::parse("a:eq:").unwrap(), Condition::eq("a", ""));
        assert_eq!(Condition::parse("a:eq:A").unwrap(), Condition::eq("a", "A"));
        assert_eq!(Condition::parse("a:ne:A").unwrap(), Condition::ne("a", "A"));
        assert_eq!(Condition::parse("a:gt:A").unwrap(), Condition::gt("a", "A"));
        assert_eq!(Condition::parse("a:ge:A").unwrap(), Condition::ge("a", "A"));
        assert_eq!(Condition::parse("a:lt:A").unwrap(), Condition::lt("a", "A"));
        assert_eq!(Condition::parse("a:le:A").unwrap(), Condition::le("a", "A"));
        assert_eq!(Condition::parse("a:lk:A").unwrap(), Condition::lk("a", "A"));
    }

    #[test]
    fn test_or_conditions() {
        let _: OrConditions = vec![Condition::eq("a", "A")].into();
        let or_c1: OrConditions = vec![Condition::eq("a", "A"), Condition::eq("b", "B")].into();
        let or_c2: OrConditions = vec![Condition::eq("b", "B"), Condition::eq("a", "A")].into();
        assert_eq!(or_c1, or_c2);
    }

    #[test]
    fn test_and_conditions() {
        let or_c1: OrConditions = vec![Condition::eq("a", "A"), Condition::eq("b", "B")].into();
        let or_c2: OrConditions = vec![Condition::eq("b", "B"), Condition::eq("a", "A")].into();
        let _: AndConditions = Vec::<OrConditions>::new().into();
        let _: AndConditions = vec![or_c1.clone()].into();
        let and_c1: AndConditions = vec![or_c1.clone(), or_c2.clone()].into();
        let and_c2: AndConditions = vec![or_c2.clone(), or_c1.clone()].into();
        assert_eq!(and_c1, and_c2);
    }

    #[test]
    fn test_list_query() {
        #[td_type::Dao]
        struct DaoDef {
            id: i64,
        }

        #[td_type::Dto]
        #[dto(list(on = DaoDef))]
        #[td_type(builder(try_from = DaoDef))]
        struct Def {
            #[dto(list(pagination_by = "+"))]
            #[td_type(builder(field = "id"))]
            order: i64,
            #[dto(list(filter))]
            #[td_type(builder(field = "id"))]
            filter: i64,
            #[dto(list(filter_like))]
            #[td_type(builder(field = "id"))]
            like: i64,

            id: i64, //TODO remove
        }

        let list_params = ListParamsBuilder::default()
            .len(0usize)
            .filter(vec![
                "filter:eq:FILTER".to_string(),
                "like:lk:LIKE".to_string(),
            ])
            .order_by("order".to_string())
            .build()
            .unwrap();
        let list_query: ListQueryParams<Def> = (&list_params).try_into().unwrap();
        let expect: AndConditions = vec![
            Into::<OrConditions>::into(vec![Condition::eq("filter", "FILTER")]),
            Into::<OrConditions>::into(vec![Condition::lk("like", "LIKE")]),
        ]
        .into();
        assert_eq!(list_query.conditions(), &expect);

        let list_params = ListParamsBuilder::default()
            .len(0usize)
            .filter(vec![
                "filterx:eq:FILTER".to_string(),
                "like:lk:LIKE".to_string(),
            ])
            .order_by("order".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<Def>, ListError> = (&list_params).try_into();
        assert!(matches!(res, Err(ListError::UndefinedFilter(_))));
        let list_params = ListParamsBuilder::default()
            .len(0usize)
            .filter(vec!["likex:lk:LIKE".to_string()])
            .order_by("order".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<Def>, ListError> = (&list_params).try_into();
        assert!(matches!(res, Err(ListError::UndefinedLikeFilter(_))));
        let list_params = ListParamsBuilder::default()
            .len(0usize)
            .order_by("orderx".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<Def>, ListError> = (&list_params).try_into();
        assert!(matches!(res, Err(ListError::UndefinedOrderBy(_))));
    }
}
