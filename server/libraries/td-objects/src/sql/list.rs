//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::ListQuery;
use crate::dxo::crudl::ListParams;
use crate::parse::IDENTIFIER_PATTERN;
use crate::types::SqlEntity;
use crate::types::basic::LikeFilter;
use itertools::Itertools;
use regex::Regex;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::LazyLock;
use td_error::{TdError, td_error};

#[td_error]
pub enum ListError {
    #[error("Invalid order-by value, it must be <NAME>+/-: {0}")]
    InvalidOrderBy(String) = 0,
    #[error("Invalid condition value, it must be <NAME><OPERATOR><VALUE> (operators are {0}): {1}")]
    InvalidCondition(String, String) = 1,
    #[error("Undefined field: {0}")]
    UndefinedField(String) = 2,
    #[error("Undefined filter: {0}")]
    UndefinedFilter(String) = 3,
    #[error("Undefined like filter: {0}")]
    UndefinedLikeFilter(String) = 4,
    #[error("Undefined order by: {0}")]
    UndefinedOrderBy(String) = 5,
    #[error("Previous and Next parameters cannot be used together")]
    PreviousAndNext = 6,
    #[error("Natural Id must be use in pagination with Previous or Next parameters")]
    MissingPaginationParams = 7,
    #[error("Invalid between condition '{0}', it must be <NAME>:btw:<min>::<max>")]
    InvalidBetweenCondition(String) = 8,
    #[error("Undefined is filter: {0}")]
    UndefinedIsFilter(String) = 9,

    #[error("Error computing SQL entity value: {0}")]
    InvalidSqlEntity(#[source] TdError) = 5000,
}

#[derive(Debug, Clone, Eq, PartialEq)]
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

    pub fn value_sql_entity<D: ListQuery>(
        &self,
        value: &str,
    ) -> Result<Box<dyn SqlEntity>, ListError> {
        D::map_sql_entity_value(self.field(), value)
            .map_err(ListError::InvalidSqlEntity)?
            .ok_or(ListError::UndefinedField(self.field().to_string()))
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

        static ORDER_BY_REGEX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(ORDER_BY_PATTERN).unwrap());

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

impl FromStr for Order {
    type Err = ListError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[td_type::typed_enum]
pub enum IsFilter {
    #[typed_enum(rename = "null")]
    Null,
    #[typed_enum(rename = "not_null")]
    NotNull,
}

#[derive(Debug)]
pub enum Condition<D> {
    Eq(String, Box<dyn SqlEntity>),
    Ne(String, Box<dyn SqlEntity>),
    Lk(String, LikeFilter),
    Gt(String, Box<dyn SqlEntity>),
    Ge(String, Box<dyn SqlEntity>),
    Lt(String, Box<dyn SqlEntity>),
    Le(String, Box<dyn SqlEntity>),
    Btw(String, Box<dyn SqlEntity>, Box<dyn SqlEntity>),
    // Similar to string like comparisons but for fixed SQL comparisons.
    Is(String, IsFilter),
    Phantom(PhantomData<D>),
}

impl<D: ListQuery + Eq> PartialEq for Condition<D> {
    fn eq(&self, other: &Self) -> bool {
        use Condition::*;
        match (self, other) {
            (Eq(f1, v1), Eq(f2, v2))
            | (Ne(f1, v1), Ne(f2, v2))
            | (Gt(f1, v1), Gt(f2, v2))
            | (Ge(f1, v1), Ge(f2, v2))
            | (Lt(f1, v1), Lt(f2, v2))
            | (Le(f1, v1), Le(f2, v2)) => f1 == f2 && v1.as_display() == v2.as_display(),
            (Btw(f1, min1, max1), Btw(f2, min2, max2)) => {
                f1 == f2
                    && min1.as_display() == min2.as_display()
                    && max1.as_display() == max2.as_display()
            }
            (Lk(f1, v1), Lk(f2, v2)) => f1 == f2 && v1.as_display() == v2.as_display(),
            (Is(c1, v1), Is(c2, v2)) => c1 == c2 && v1 == v2,
            (Phantom(_), Phantom(_)) => true,
            _ => false,
        }
    }
}
impl<D: ListQuery + Eq> Eq for Condition<D> {}

impl<D: ListQuery> Condition<D> {
    fn parse(s: &str) -> Result<Self, TdError> {
        const EQ: &str = ":eq:";
        const NE: &str = ":ne:";
        const GT: &str = ":gt:";
        const GE: &str = ":ge:";
        const LT: &str = ":lt:";
        const LE: &str = ":le:";
        const LK: &str = ":lk:";
        const BTW: &str = ":btw:";
        const IS: &str = ":is:";

        const OPERATORS: &str = constcat::concat!(
            EQ, "|", NE, "|", GT, "|", GE, "|", LT, "|", LE, "|", LK, "|", BTW, "|", IS
        );
        const CONDITION_PATTERN: &str = constcat::concat!(
            "^(?<field>",
            IDENTIFIER_PATTERN,
            ")(?<operator>(",
            OPERATORS,
            "))(?<value>(.*))$"
        );

        static CONDITION_REGEX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(CONDITION_PATTERN).unwrap());

        if let Some(captures) = CONDITION_REGEX.captures(s) {
            let field = captures.name("field").unwrap().as_str().to_string();
            let operator = captures.name("operator").unwrap().as_str().to_string();
            let value = captures.name("value").unwrap().as_str().to_string();
            let condition = match operator.as_str() {
                EQ => {
                    let sql_value = D::map_sql_entity_value(&field, &value)?
                        .ok_or(ListError::UndefinedField(field.clone()))?;
                    Self::Eq(field, sql_value)
                }
                NE => {
                    let sql_value = D::map_sql_entity_value(&field, &value)?
                        .ok_or(ListError::UndefinedField(field.clone()))?;
                    Self::Ne(field, sql_value)
                }
                GT => {
                    let sql_value = D::map_sql_entity_value(&field, &value)?
                        .ok_or(ListError::UndefinedField(field.clone()))?;
                    Self::Gt(field, sql_value)
                }
                GE => {
                    let sql_value = D::map_sql_entity_value(&field, &value)?
                        .ok_or(ListError::UndefinedField(field.clone()))?;
                    Self::Ge(field, sql_value)
                }
                LT => {
                    let sql_value = D::map_sql_entity_value(&field, &value)?
                        .ok_or(ListError::UndefinedField(field.clone()))?;
                    Self::Lt(field, sql_value)
                }
                LE => {
                    let sql_value = D::map_sql_entity_value(&field, &value)?
                        .ok_or(ListError::UndefinedField(field.clone()))?;
                    Self::Le(field, sql_value)
                }
                LK => {
                    // check that it exists, but do not convert to type, as it is a LIKE filter
                    if !D::filter_by_like_fields().contains(&field.as_str()) {
                        Err(ListError::UndefinedLikeFilter(field.clone()))?
                    }
                    let converted = Self::convert_to_like_pattern(&value)?;
                    Self::Lk(field, converted)
                }
                BTW => {
                    let min_max = value.split("::").collect::<Vec<_>>();
                    if min_max.len() != 2 {
                        Err(ListError::InvalidBetweenCondition(s.to_string()))?
                    }
                    let sql_min = D::map_sql_entity_value(&field, min_max[0])?
                        .ok_or(ListError::UndefinedField(field.clone()))?;
                    let sql_max = D::map_sql_entity_value(&field, min_max[1])?
                        .ok_or(ListError::UndefinedField(field.clone()))?;
                    Self::Btw(field, sql_min, sql_max)
                }
                IS => {
                    let sql_value = IsFilter::from_str(&value)
                        .map_err(|_| ListError::UndefinedIsFilter(value.clone()))?;
                    Self::Is(field, sql_value)
                }
                _ => Err(ListError::InvalidCondition(
                    OPERATORS.to_string(),
                    s.to_string(),
                ))?,
            };
            Ok(condition)
        } else {
            Err(ListError::InvalidCondition(
                OPERATORS.to_string(),
                s.to_string(),
            ))?
        }
    }

    fn convert_to_like_pattern(value: &str) -> Result<LikeFilter, TdError> {
        let mut converted_value = String::with_capacity(value.len() * 2);
        let mut previous_char_is_escape = false;
        for c in value.chars() {
            let mut _buffer = String::with_capacity(4);
            let (converted_char, is_escape) = match (c, previous_char_is_escape) {
                ('\\', false) => ("", true),
                ('\\', true) => ("\\\\", false),
                ('%', _) => ("\\%", false),
                ('_', _) => ("\\_", false),
                ('*', true) => ("*", false),
                ('.', true) => (".", false),
                ('*', false) => ("%", false),
                ('.', false) => ("_", false),
                (other, _) => {
                    // non special chars can be safely de-escaped
                    _buffer.clear();
                    _buffer.push(other);
                    (_buffer.as_str(), false)
                }
            };
            converted_value += converted_char;
            previous_char_is_escape = is_escape;
        }
        converted_value.try_into()
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
            Condition::Is(field, _) => field,
            Condition::Phantom(_) => unreachable!(),
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
            Condition::Is(_, _) => "IS",
            Condition::Phantom(_) => unreachable!(),
        }
    }
    pub fn bind_value<'a>(
        &'a self,
        query_builder: &mut sqlx::query_builder::Separated<'_, 'a, sqlx::Sqlite, &str>,
    ) {
        // no SQL injection here, as the values are bound to the fields of the struct
        query_builder.push(format!("{} {} ", self.field(), self.operator()));

        match self {
            Condition::Eq(_, value)
            | Condition::Ne(_, value)
            | Condition::Gt(_, value)
            | Condition::Ge(_, value)
            | Condition::Lt(_, value)
            | Condition::Le(_, value) => {
                value.push_bind_unseparated(query_builder);
            }
            Condition::Lk(_, value) => {
                value.push_bind_unseparated(query_builder);
                query_builder.push_unseparated(r#" ESCAPE '\' "#);
            }
            Condition::Btw(_, max, min) => {
                max.push_bind_unseparated(query_builder);
                query_builder.push_unseparated(" AND ");
                min.push_bind_unseparated(query_builder);
            }
            Condition::Is(_, value) => match value {
                IsFilter::Null => {
                    query_builder.push_unseparated("NULL");
                }
                IsFilter::NotNull => {
                    query_builder.push_unseparated("NOT NULL");
                }
            },
            Condition::Phantom(_) => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub struct OrConditions<D>(pub Vec<Condition<D>>);

impl<D: ListQuery + Eq> PartialEq for OrConditions<D> {
    fn eq(&self, other: &Self) -> bool {
        for condition in &self.0 {
            if !other.0.contains(condition) {
                return false;
            }
        }
        for condition in &other.0 {
            if !self.0.contains(condition) {
                return false;
            }
        }
        true
    }
}

impl<D> OrConditions<D> {
    pub fn conditions(&self) -> &[Condition<D>] {
        &self.0
    }
}

#[derive(Debug)]
pub struct AndConditions<D>(pub Vec<OrConditions<D>>);

impl<D: ListQuery + Eq> PartialEq for AndConditions<D> {
    fn eq(&self, other: &Self) -> bool {
        for condition in &self.0 {
            if !other.0.contains(condition) {
                return false;
            }
        }
        for condition in &other.0 {
            if !self.0.contains(condition) {
                return false;
            }
        }
        true
    }
}

impl<D: ListQuery> AndConditions<D> {
    pub fn conditions(&self) -> &[OrConditions<D>] {
        &self.0
    }
}

impl<T, D: ListQuery> From<T> for AndConditions<D>
where
    T: Into<Vec<OrConditions<D>>>,
{
    fn from(or_conditions: T) -> Self {
        AndConditions(or_conditions.into())
    }
}

#[derive(Debug)]
pub enum Pagination {
    Previous(Box<dyn SqlEntity>, Box<dyn SqlEntity>),
    Next(Box<dyn SqlEntity>, Box<dyn SqlEntity>),
}

impl Pagination {
    pub fn column_value(&self) -> &dyn SqlEntity {
        match self {
            Pagination::Previous(column_value, _) => &**column_value,
            Pagination::Next(column_value, _) => &**column_value,
        }
    }

    pub fn pagination_id(&self) -> &dyn SqlEntity {
        match self {
            Pagination::Previous(_, pagination_id) => &**pagination_id,
            Pagination::Next(_, pagination_id) => &**pagination_id,
        }
    }
}

#[derive(Debug)]
pub struct ListQueryParams<D: ListQuery> {
    pub len: usize,
    pub conditions: AndConditions<D>,
    pub natural_order: Order,
    pub order: Option<Order>,
    pub pagination: Option<Pagination>,
}

impl<D: ListQuery> TryFrom<&ListParams> for ListQueryParams<D> {
    type Error = TdError;
    fn try_from(value: &ListParams) -> Result<Self, Self::Error> {
        let conditions = value
            .filter
            .iter()
            .map(String::as_str)
            .map(Condition::parse)
            .collect::<Result<Vec<Condition<D>>, _>>()?;
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
        let conditions: AndConditions<D> = or_condition_groups
            .into_values()
            .map(|c| OrConditions(c))
            .collect::<Vec<_>>()
            .into();

        let order = match &value.order_by {
            Some(o) => {
                let o = o.parse()?;
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
        let pagination = match (&value.previous, &value.next, &value.pagination_id) {
            (Some(_), Some(_), _) => Err(ListError::PreviousAndNext),
            (Some(_), _, None) => Err(ListError::MissingPaginationParams),
            (_, Some(_), None) => Err(ListError::MissingPaginationParams),
            (None, None, Some(_)) => Err(ListError::MissingPaginationParams),
            (Some(column_value), None, Some(pagination_id)) => {
                let column_value = order
                    .as_ref()
                    .unwrap_or(&natural_order)
                    .value_sql_entity::<D>(column_value)?;
                let pagination_id = natural_order.value_sql_entity::<D>(pagination_id)?;
                Ok(Some(Pagination::Previous(column_value, pagination_id)))
            }
            (None, Some(column_value), Some(pagination_id)) => {
                let column_value = order
                    .as_ref()
                    .unwrap_or(&natural_order)
                    .value_sql_entity::<D>(column_value)?;
                let pagination_id = natural_order.value_sql_entity::<D>(pagination_id)?;
                Ok(Some(Pagination::Next(column_value, pagination_id)))
            }
            _ => Ok(None),
        }?;

        Ok(ListQueryParams {
            len: value.len,
            conditions,
            natural_order,
            order,
            pagination,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::crudl::ListParamsBuilder;
    use std::any::Any;

    #[td_type::Dao]
    struct TestDao {
        a: String,
    }

    #[td_type::Dto]
    #[derive(Eq, PartialEq)]
    #[dto(list(on = TestDao))]
    #[td_type(builder(try_from = TestDao))]
    struct TestDto {
        #[dto(list(pagination_by = "+", filter_like))]
        a: String,
    }

    impl SqlEntity for String {
        fn push_bind<'a>(&'a self, _builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>) {
            unreachable!()
        }

        fn push_bind_unseparated<'a>(
            &'a self,
            _builder: &mut sqlx::query_builder::Separated<'_, 'a, sqlx::Sqlite, &str>,
        ) {
            unreachable!()
        }

        fn as_display(&self) -> String {
            self.clone()
        }

        fn from_display(s: impl ToString) -> Result<Self, TdError> {
            Ok(s.to_string())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

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
        assert!(Condition::<TestDto>::parse("").is_err());
        assert!(Condition::<TestDto>::parse("a").is_err());
        assert!(Condition::<TestDto>::parse(":eq:").is_err());
        assert!(Condition::<TestDto>::parse("a:ff:b").is_err());
        assert_eq!(
            Condition::<TestDto>::parse("a:eq:").unwrap(),
            Condition::Eq("a".to_string(), Box::new("".to_string()))
        );
        assert_eq!(
            Condition::<TestDto>::parse("a:eq:A").unwrap(),
            Condition::Eq("a".to_string(), Box::new("A".to_string()))
        );
        assert_eq!(
            Condition::<TestDto>::parse("a:ne:A").unwrap(),
            Condition::Ne("a".to_string(), Box::new("A".to_string()))
        );
        assert_eq!(
            Condition::<TestDto>::parse("a:gt:A").unwrap(),
            Condition::Gt("a".to_string(), Box::new("A".to_string()))
        );
        assert_eq!(
            Condition::<TestDto>::parse("a:ge:A").unwrap(),
            Condition::Ge("a".to_string(), Box::new("A".to_string()))
        );
        assert_eq!(
            Condition::<TestDto>::parse("a:lt:A").unwrap(),
            Condition::Lt("a".to_string(), Box::new("A".to_string()))
        );
        assert_eq!(
            Condition::<TestDto>::parse("a:le:A").unwrap(),
            Condition::Le("a".to_string(), Box::new("A".to_string()))
        );
        assert_eq!(
            Condition::<TestDto>::parse("a:lk:A").unwrap(),
            Condition::Lk("a".to_string(), "A".try_into().unwrap())
        );
        assert_eq!(
            Condition::<TestDto>::parse("a:is:null").unwrap(),
            Condition::Is("a".to_string(), IsFilter::Null)
        );
        assert_eq!(
            Condition::<TestDto>::parse("a:is:not_null").unwrap(),
            Condition::Is("a".to_string(), IsFilter::NotNull)
        );
    }

    #[test]
    fn test_list_query() {
        #[td_type::Dao]
        struct DaoDef {
            id: String,
        }

        #[td_type::Dto]
        #[dto(list(on = DaoDef))]
        #[td_type(builder(try_from = DaoDef))]
        #[derive(Eq, PartialEq, Hash)]
        struct Def {
            #[dto(list(pagination_by = "+"))]
            #[td_type(builder(field = "id"))]
            order: String,
            #[dto(list(filter))]
            #[td_type(builder(field = "id"))]
            filter: String,
            #[dto(list(filter_like))]
            #[td_type(builder(field = "id"))]
            like: String,

            id: String, //TODO remove
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
        let expect = AndConditions::<Def>(vec![
            OrConditions(vec![Condition::Eq(
                "filter".to_string(),
                Box::new("FILTER".to_string()),
            )]),
            OrConditions(vec![Condition::Lk(
                "like".to_string(),
                "LIKE".try_into().unwrap(),
            )]),
        ]);
        assert_eq!(list_query.conditions, expect);

        let list_params = ListParamsBuilder::default()
            .len(0usize)
            .filter(vec![
                "filterx:eq:FILTER".to_string(),
                "like:lk:LIKE".to_string(),
            ])
            .order_by("order".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<Def>, TdError> = (&list_params).try_into();
        let err = res.err().unwrap();
        let err = err.domain_err::<ListError>();
        assert!(matches!(err, ListError::UndefinedField(_)));
        let list_params = ListParamsBuilder::default()
            .len(0usize)
            .filter(vec!["likex:lk:LIKE".to_string()])
            .order_by("order".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<Def>, TdError> = (&list_params).try_into();
        let err = res.err().unwrap();
        let err = err.domain_err::<ListError>();
        assert!(matches!(err, ListError::UndefinedLikeFilter(_)));
        let list_params = ListParamsBuilder::default()
            .len(0usize)
            .order_by("orderx".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<Def>, TdError> = (&list_params).try_into();
        let err = res.err().unwrap();
        let err = err.domain_err::<ListError>();
        assert!(matches!(err, ListError::UndefinedOrderBy(_)));
    }

    #[test]
    fn test_convert_to_like_pattern() {
        let test_cases = vec![
            ("abc", "abc"),
            ("\\abc", "abc"),
            ("a\\bc", "abc"),
            ("abc\\\\", "abc\\\\"),
            ("*abc", "%abc"),
            ("a*bc", "a%bc"),
            ("abc*", "abc%"),
            (".abc", "_abc"),
            ("a.bc", "a_bc"),
            ("abc.", "abc_"),
            ("\\*abc", "*abc"),
            ("a\\*bc", "a*bc"),
            ("abc\\*", "abc*"),
            ("\\.abc", ".abc"),
            ("a\\.bc", "a.bc"),
            ("abc\\.", "abc."),
            ("%abc", "\\%abc"),
            ("a%bc", "a\\%bc"),
            ("abc%", "abc\\%"),
            ("_abc", "\\_abc"),
            ("a_bc", "a\\_bc"),
            ("abc_", "abc\\_"),
            ("\\\\", "\\\\"),
            ("\\*", "*"),
            ("\\%", "\\%"),
            ("\\.", "."),
            ("\\_", "\\_"),
            ("_a%b*\\%c\\*d.e\\.\\_", "\\_a\\%b%\\%c*d_e.\\_"),
        ];

        for (input, expected) in test_cases {
            let result = Condition::<TestDto>::convert_to_like_pattern(input).unwrap();
            assert_eq!(*result, expected);
        }
    }
}
