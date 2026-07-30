//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::crudl::ListParams;
use crate::parse::IDENTIFIER_PATTERN;
use crate::types::basic::LikeFilter;
use itertools::Itertools;
use regex::Regex;
use sea_query::{
    Condition, Expr, ExprTrait, Iden, LikeExpr, NullOrdering, Order as SeaOrder, SelectStatement,
};
use std::str::FromStr;
use std::sync::LazyLock;
use ta_objects::{ListDto, SeaValue, TypedColumns};
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
    #[error("Error parsing value: {0}")]
    InvalidFieldValue(#[source] TdError) = 10,
    #[error("Empty value for filter '{0}'; use '{0}:is:null' to match NULLs")]
    EmptyFilterValue(String) = 11,
    #[error(
        "Pagination column of {0} is not unique within the listing (cursor value '{1}' repeats); paging would silently skip rows"
    )]
    NonUniquePaginationKey(String, String) = 5000,
}

/// Parse a typed column value string for this order's field.
fn parse_val<D: ListDto>(
    field: &<<D as ListDto>::Dao as TypedColumns>::Iden,
    value: &str,
) -> Result<Box<dyn SeaValue>, ListError> {
    D::parse_filter_value(field, value)
        .map_err(ListError::InvalidFieldValue)?
        .ok_or(ListError::UndefinedField(field.to_string()))
}

/// Parse a filter-condition value, rejecting the empty NULL marker — in a
/// comparison it would build a never-true `col = NULL` predicate.
fn parse_cond_val<D: ListDto>(
    field: &<<D as ListDto>::Dao as TypedColumns>::Iden,
    value: &str,
) -> Result<Box<dyn SeaValue>, ListError> {
    let val = parse_val::<D>(field, value)?;
    if val.is_sql_null() {
        return Err(ListError::EmptyFilterValue(field.to_string()));
    }
    Ok(val)
}

// ---------------------------------------------------------------------------
// Order
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Order<D>
where
    D: ListDto,
{
    Asc(<<D as ListDto>::Dao as TypedColumns>::Iden),
    Desc(<<D as ListDto>::Dao as TypedColumns>::Iden),
}

impl<D> Order<D>
where
    D: ListDto,
{
    pub fn invert(&self) -> Self {
        match self {
            Order::Asc(f) => Order::Desc(*f),
            Order::Desc(f) => Order::Asc(*f),
        }
    }

    pub fn field(&self) -> &<<D as ListDto>::Dao as TypedColumns>::Iden {
        match self {
            Order::Asc(f) | Order::Desc(f) => f,
        }
    }

    pub fn sea_order(&self) -> SeaOrder {
        match self {
            Order::Asc(_) => SeaOrder::Asc,
            Order::Desc(_) => SeaOrder::Desc,
        }
    }

    pub(crate) fn parse(s: &str) -> Result<Order<D>, ListError> {
        const ORDER_BY_PATTERN: &str = constcat::concat!(
            "^(?<field>",
            IDENTIFIER_PATTERN,
            ")(?<direction>(\\+|\\-))?$"
        );
        static ORDER_BY_REGEX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(ORDER_BY_PATTERN).unwrap());

        if let Some(caps) = ORDER_BY_REGEX.captures(s) {
            let field = caps.name("field").unwrap().as_str();
            let field = D::order_by_col(field)
                .ok_or_else(|| ListError::UndefinedOrderBy(field.to_string()))?;
            let asc = caps.name("direction").map_or("+", |m| m.as_str()) == "+";
            Ok(if asc {
                Order::Asc(field)
            } else {
                Order::Desc(field)
            })
        } else {
            Err(ListError::InvalidOrderBy(s.to_string()))
        }
    }
}

impl<D> FromStr for Order<D>
where
    D: ListDto,
{
    type Err = ListError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

// ---------------------------------------------------------------------------
// IsFilter
// ---------------------------------------------------------------------------

#[ta_objects::typed_enum]
pub enum IsFilter {
    #[typed_enum(rename = "null")]
    Null,
    #[typed_enum(rename = "not_null")]
    NotNull,
}

// ---------------------------------------------------------------------------
// Keyset Pagination
// ---------------------------------------------------------------------------

/// Keyset cursor expressed as typed Sea Query values.
///
/// For a query ordered by `(order_col ASC, id_col ASC)` the predicates are:
/// - **Next**:     `(order_col > val) OR (order_col = val AND id_col > id)`
/// - **Previous**: `(order_col < val) OR (order_col = val AND id_col < id)`
///   (the caller must also invert ORDER BY and reverse the result set)
///
/// `id_col` is the DTO's `pagination_by` field: it MUST be unique within the
/// listing, or page boundaries inside a tie group silently skip rows.
#[derive(Debug)]
pub enum Pagination<D>
where
    D: ListDto,
{
    Previous {
        order_col: <<D as ListDto>::Dao as TypedColumns>::Iden,
        order_val: Box<dyn SeaValue>,
        id_col: <<D as ListDto>::Dao as TypedColumns>::Iden,
        id_val: Box<dyn SeaValue>,
    },
    Next {
        order_col: <<D as ListDto>::Dao as TypedColumns>::Iden,
        order_val: Box<dyn SeaValue>,
        id_col: <<D as ListDto>::Dao as TypedColumns>::Iden,
        id_val: Box<dyn SeaValue>,
    },
}

impl<D> Pagination<D>
where
    D: ListDto,
{
    /// Emit the keyset predicate as a Sea Query `Expr`.
    pub fn to_expr(&self, natural_order: &Order<D>) -> Expr {
        let (order_col, order_val, id_col, id_val, forward) = match self {
            Pagination::Next {
                order_col,
                order_val,
                id_col,
                id_val,
            } => (order_col, order_val, id_col, id_val, true),
            Pagination::Previous {
                order_col,
                order_val,
                id_col,
                id_val,
            } => (order_col, order_val, id_col, id_val, false),
        };

        // Cursor-based pagination: select rows "past" the current cursor (order_val, id_val).
        // "Past" means greater-than when sorting ascending, less-than when descending.
        let towards_end = matches!(
            (forward, natural_order.sea_order()),
            (true, SeaOrder::Asc) | (false, SeaOrder::Desc)
        );
        let past = if towards_end { Expr::gt } else { Expr::lt };
        let past_id = past(Expr::col(*id_col), Expr::val(id_val.as_sea_value()));

        // Same column: the composite degenerates to the single comparison.
        if order_col.to_string() == id_col.to_string() {
            return past_id;
        }

        // NULLs sort as the largest values (see `apply_to`), a run plain
        // comparisons never reach — walk it with IS NULL predicates.
        if order_val.is_sql_null() {
            let in_null_run = Expr::col(*order_col).is_null().and(past_id);
            if towards_end {
                in_null_run
            } else {
                in_null_run.or(Expr::col(*order_col).is_not_null())
            }
        } else {
            // WHERE order_col > order_val
            //    OR (order_col = order_val AND id_col > id_val)
            let composite = past(Expr::col(*order_col), Expr::val(order_val.as_sea_value())).or(
                Expr::col(*order_col)
                    .eq(Expr::val(order_val.as_sea_value()))
                    .and(past_id),
            );
            if order_val.is_nullable() && towards_end {
                composite.or(Expr::col(*order_col).is_null())
            } else {
                composite
            }
        }
    }

    /// Emit the ORDER BY columns for this pagination direction.
    pub fn order_exprs(
        &self,
        natural_order: &Order<D>,
    ) -> Vec<(<<D as ListDto>::Dao as TypedColumns>::Iden, SeaOrder)> {
        let (order_col, id_col, forward) = match self {
            Pagination::Next {
                order_col, id_col, ..
            } => (order_col, id_col, true),
            Pagination::Previous {
                order_col, id_col, ..
            } => (order_col, id_col, false),
        };
        let dir = if forward {
            natural_order.sea_order()
        } else {
            natural_order.invert().sea_order()
        };
        if order_col.to_string() == id_col.to_string() {
            return vec![(*id_col, dir)];
        }
        vec![(*order_col, dir.clone()), (*id_col, dir)]
    }
}

// ---------------------------------------------------------------------------
// ListQueryParams
// ---------------------------------------------------------------------------

pub struct ListQueryParams<D>
where
    D: ListDto,
{
    pub len: usize,
    pub filter: Condition,
    pub natural_order: Order<D>,
    pub order: Option<Order<D>>,
    pub pagination: Option<Pagination<D>>,
}

impl<D> ListQueryParams<D>
where
    D: ListDto,
{
    /// Build a complete `SelectStatement` from the parsed parameters.
    ///
    /// `base` should be a `SelectStatement` that already has the correct
    /// columns and FROM clause — this method appends WHERE, ORDER BY and LIMIT.
    pub fn apply_to(&self, mut base: SelectStatement) -> SelectStatement {
        // --- WHERE from filter conditions ---
        base.cond_where(self.filter.clone());

        // --- WHERE from keyset pagination ---
        let effective_order = self.order.as_ref().unwrap_or(&self.natural_order);
        if let Some(pag) = &self.pagination {
            base.and_where(pag.to_expr(effective_order));
        }

        // --- ORDER BY ---
        // Client-chosen order columns may be nullable: pin NULLs to the large
        // end (the Postgres default) — `Pagination::to_expr` relies on it.
        let pin_nulls = |dir: &SeaOrder| match dir {
            SeaOrder::Asc => NullOrdering::Last,
            _ => NullOrdering::First,
        };
        match &self.pagination {
            Some(pag) => {
                let mut exprs = pag.order_exprs(effective_order).into_iter();
                if let Some((col, dir)) = exprs.next() {
                    if self.order.is_some() {
                        let nulls = pin_nulls(&dir);
                        base.order_by_with_nulls(col, dir, nulls);
                    } else {
                        base.order_by(col, dir);
                    }
                }
                for (col, dir) in exprs {
                    base.order_by(col, dir);
                }
            }
            None => {
                if let Some(o) = &self.order {
                    let (id_col, _) = D::pagination_col();
                    let dir = o.sea_order();
                    let nulls = pin_nulls(&dir);
                    base.order_by_with_nulls(*o.field(), dir, nulls);
                    base.order_by(id_col, self.natural_order.sea_order());
                } else {
                    let (id_col, id_dir) = D::pagination_col();
                    base.order_by(id_col, id_dir);
                }
            }
        }

        // --- LIMIT --- fetch one extra row to detect "has more pages"
        base.limit(self.len as u64 + 1);

        base
    }
}

// ---------------------------------------------------------------------------
// Condition parsing
// ---------------------------------------------------------------------------

/// Parse a single filter string into a `(column_key, Expr)` pair.
///
/// The `column_key` is the string representation of the resolved column and is
/// used to group multiple filters on the same column with OR.
pub(crate) fn parse_condition<D: ListDto>(
    s: &str,
) -> Result<(<<D as ListDto>::Dao as TypedColumns>::Iden, Expr), TdError> {
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

    let caps = CONDITION_REGEX
        .captures(s)
        .ok_or_else(|| ListError::InvalidCondition(OPERATORS.to_string(), s.to_string()))?;

    let field = caps.name("field").unwrap().as_str();
    let operator = caps.name("operator").unwrap().as_str();
    let value = caps.name("value").unwrap().as_str();

    let resolve_col = |f: &str| -> Result<_, TdError> {
        D::filter_col(f).ok_or_else(|| ListError::UndefinedField(f.to_string()).into())
    };

    let (col, expr) = match operator {
        EQ => {
            let col = resolve_col(field)?;
            (
                col,
                Expr::col(col).eq(parse_cond_val::<D>(&col, value)?.as_sea_value()),
            )
        }
        NE => {
            let col = resolve_col(field)?;
            (
                col,
                Expr::col(col).ne(parse_cond_val::<D>(&col, value)?.as_sea_value()),
            )
        }
        GT => {
            let col = resolve_col(field)?;
            (
                col,
                Expr::col(col).gt(parse_cond_val::<D>(&col, value)?.as_sea_value()),
            )
        }
        GE => {
            let col = resolve_col(field)?;
            (
                col,
                Expr::col(col).gte(parse_cond_val::<D>(&col, value)?.as_sea_value()),
            )
        }
        LT => {
            let col = resolve_col(field)?;
            (
                col,
                Expr::col(col).lt(parse_cond_val::<D>(&col, value)?.as_sea_value()),
            )
        }
        LE => {
            let col = resolve_col(field)?;
            (
                col,
                Expr::col(col).lte(parse_cond_val::<D>(&col, value)?.as_sea_value()),
            )
        }
        LK => {
            let col = D::filter_like_col(field)
                .ok_or_else(|| ListError::UndefinedLikeFilter(field.to_string()))?;
            let pat = convert_to_like_pattern(value)?;
            (
                col,
                Expr::col(col).like(LikeExpr::new(pat.as_str()).escape('\\')),
            )
        }
        BTW => {
            let parts = value.split("::").collect::<Vec<_>>();
            if parts.len() != 2 {
                Err(ListError::InvalidBetweenCondition(s.to_string()))?
            }
            let col = resolve_col(field)?;
            (
                col,
                Expr::col(col).between(
                    parse_cond_val::<D>(&col, parts[0])?.as_sea_value(),
                    parse_cond_val::<D>(&col, parts[1])?.as_sea_value(),
                ),
            )
        }
        IS => {
            let col = resolve_col(field)?;
            let f = IsFilter::from_str(value)
                .map_err(|_| ListError::UndefinedIsFilter(value.to_string()))?;
            (
                col,
                match f {
                    IsFilter::Null => Expr::col(col).is_null(),
                    IsFilter::NotNull => Expr::col(col).is_not_null(),
                },
            )
        }
        _ => Err(ListError::InvalidCondition(
            OPERATORS.to_string(),
            s.to_string(),
        ))?,
    };

    Ok((col, expr))
}

pub(crate) fn convert_to_like_pattern(value: &str) -> Result<LikeFilter, TdError> {
    let mut out = String::with_capacity(value.len() * 2);
    let mut escaped = false;
    for c in value.chars() {
        let mut buf = String::with_capacity(4);
        let (fragment, next_escaped) = match (c, escaped) {
            ('\\', false) => ("", true),
            ('\\', true) => ("\\\\", false),
            ('%', _) => ("\\%", false),
            ('_', _) => ("\\_", false),
            ('*', true) => ("*", false),
            ('.', true) => (".", false),
            ('*', false) => ("%", false),
            ('.', false) => ("_", false),
            (other, _) => {
                buf.push(other);
                (buf.as_str(), false)
            }
        };
        out += fragment;
        escaped = next_escaped;
    }
    out.try_into()
}

// ---------------------------------------------------------------------------
// TryFrom<&ListParams>
// ---------------------------------------------------------------------------

impl<D: ListDto> TryFrom<&ListParams> for ListQueryParams<D> {
    type Error = TdError;

    fn try_from(value: &ListParams) -> Result<Self, Self::Error> {
        // Parse filter strings into (column_key, Expr) pairs.
        let parsed: Vec<_> = value
            .filter
            .iter()
            .map(String::as_str)
            .map(parse_condition::<D>)
            .collect::<Result<_, _>>()?;

        // Group by column key: same column → OR, different columns → AND.
        let filter = parsed
            .into_iter()
            .map(|(col, expr)| (col.to_string(), expr))
            .into_group_map()
            .into_values()
            .fold(Condition::all(), |and_cond, exprs| {
                let or_cond = exprs
                    .into_iter()
                    .fold(Condition::any(), |c, expr| c.add(expr));
                and_cond.add(or_cond)
            });

        // Parse and validate order-by.
        let order = value.order_by.as_ref().map(|o| o.parse()).transpose()?;

        // Natural (pagination) order — always the pagination column.
        let (id_col, id_dir) = D::pagination_col();
        let natural_order = match &order {
            Some(Order::Asc(_)) => Order::Asc(id_col),
            Some(Order::Desc(_)) => Order::Desc(id_col),
            None => match id_dir {
                SeaOrder::Asc => Order::Asc(id_col),
                _ => Order::Desc(id_col),
            },
        };

        // Resolve keyset pagination cursors.
        let effective_order = order.as_ref().unwrap_or(&natural_order);
        let pagination = match (&value.previous, &value.next, &value.pagination_id) {
            (Some(_), Some(_), _) => Err(ListError::PreviousAndNext),
            (Some(_), _, None) => Err(ListError::MissingPaginationParams),
            (_, Some(_), None) => Err(ListError::MissingPaginationParams),
            (None, None, Some(_)) => Err(ListError::MissingPaginationParams),
            (Some(col_val), None, Some(id_val)) => {
                let (id_col, _) = D::pagination_col();
                Ok(Some(Pagination::Previous {
                    order_col: *effective_order.field(),
                    order_val: parse_val::<D>(effective_order.field(), col_val)?,
                    id_col,
                    id_val: parse_val::<D>(natural_order.field(), id_val)?,
                }))
            }
            (None, Some(col_val), Some(id_val)) => {
                let (id_col, _) = D::pagination_col();
                Ok(Some(Pagination::Next {
                    order_col: *effective_order.field(),
                    order_val: parse_val::<D>(effective_order.field(), col_val)?,
                    id_col,
                    id_val: parse_val::<D>(natural_order.field(), id_val)?,
                }))
            }
            _ => Ok(None),
        }?;

        Ok(ListQueryParams {
            len: value.len,
            filter,
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
    use ta_objects::TypedColumns;
    use td_error::TdError;

    #[ta_objects::typed(string)]
    pub struct StringType;

    #[ta_objects::Dao]
    struct TestDao {
        a: StringType,
    }

    #[ta_objects::Dto]
    #[dto(list(on = TestDao))]
    struct TestDto {
        #[dto(list(pagination_by = "+", filter_like))]
        a: StringType,
    }

    #[test]
    fn test_order_parse() {
        assert!(Order::<TestDto>::parse("").is_err());
        assert!(Order::<TestDto>::parse("-").is_err());
        assert!(Order::<TestDto>::parse("a=").is_err());
        assert!(matches!(
            Order::<TestDto>::parse("a").unwrap(),
            Order::<TestDto>::Asc(<TestDao as TypedColumns>::Iden::A)
        ));
        assert!(matches!(
            Order::<TestDto>::parse("a+").unwrap(),
            Order::<TestDto>::Asc(<TestDao as TypedColumns>::Iden::A)
        ));
        assert!(matches!(
            Order::<TestDto>::parse("a-").unwrap(),
            Order::<TestDto>::Desc(<TestDao as TypedColumns>::Iden::A)
        ));
    }

    #[test]
    fn test_condition_parse_errors() {
        assert!(parse_condition::<TestDto>("").is_err());
        assert!(parse_condition::<TestDto>("a").is_err());
        assert!(parse_condition::<TestDto>(":eq:").is_err());
        assert!(parse_condition::<TestDto>("a:ff:b").is_err());
        // `a` is only a like-filter field, not a regular filter field
        assert!(parse_condition::<TestDto>("a:eq:A").is_err());
    }

    #[test]
    fn test_condition_parse_ok() {
        assert!(parse_condition::<TestDto>("a:lk:A").is_ok());
        assert!(parse_condition::<TestDto>("a:lk:").is_ok());
    }

    #[test]
    fn test_list_query() {
        #[ta_objects::Dao]
        struct DaoDef {
            id: StringType,
        }

        #[ta_objects::Dto]
        #[dto(list(on = DaoDef))]
        struct Def {
            #[dto(list(pagination_by = "+"))]
            #[dxo(builder(field = "id"))]
            order: StringType,
            #[dto(list(filter))]
            #[dxo(builder(field = "id"))]
            filter: StringType,
            #[dto(list(filter_like))]
            #[dxo(builder(field = "id"))]
            like: StringType,
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
        // two distinct columns → two OR-groups AND'd together
        assert!(!list_query.filter.is_empty());

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
    fn test_pagination_previous_and_next_error() {
        let list_params = ListParamsBuilder::default()
            .len(10usize)
            .previous("prev".to_string())
            .next("next".to_string())
            .pagination_id("id".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<TestDto>, TdError> = (&list_params).try_into();
        let err = res.err().unwrap();
        let err = err.domain_err::<ListError>();
        assert!(matches!(err, ListError::PreviousAndNext));
    }

    #[test]
    fn test_pagination_previous_missing_id() {
        let list_params = ListParamsBuilder::default()
            .len(10usize)
            .previous("prev".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<TestDto>, TdError> = (&list_params).try_into();
        let err = res.err().unwrap();
        let err = err.domain_err::<ListError>();
        assert!(matches!(err, ListError::MissingPaginationParams));
    }

    #[test]
    fn test_pagination_next_missing_id() {
        let list_params = ListParamsBuilder::default()
            .len(10usize)
            .next("next".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<TestDto>, TdError> = (&list_params).try_into();
        let err = res.err().unwrap();
        let err = err.domain_err::<ListError>();
        assert!(matches!(err, ListError::MissingPaginationParams));
    }

    #[test]
    fn test_pagination_id_without_cursor() {
        let list_params = ListParamsBuilder::default()
            .len(10usize)
            .pagination_id("id".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<TestDto>, TdError> = (&list_params).try_into();
        let err = res.err().unwrap();
        let err = err.domain_err::<ListError>();
        assert!(matches!(err, ListError::MissingPaginationParams));
    }

    #[test]
    fn test_pagination_next_ok() {
        let list_params = ListParamsBuilder::default()
            .len(10usize)
            .next("cursor_val".to_string())
            .pagination_id("id_val".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<TestDto>, TdError> = (&list_params).try_into();
        assert!(res.is_ok());
        let qp = res.unwrap();
        assert!(qp.pagination.is_some());
    }

    #[test]
    fn test_pagination_previous_ok() {
        let list_params = ListParamsBuilder::default()
            .len(10usize)
            .previous("cursor_val".to_string())
            .pagination_id("id_val".to_string())
            .build()
            .unwrap();
        let res: Result<ListQueryParams<TestDto>, TdError> = (&list_params).try_into();
        assert!(res.is_ok());
        let qp = res.unwrap();
        assert!(qp.pagination.is_some());
    }

    #[ta_objects::Dao]
    struct RunDao {
        id: StringType,
        name: StringType,
    }

    #[ta_objects::Dto]
    #[dto(list(on = RunDao))]
    struct RunDto {
        #[dto(list(pagination_by = "+", filter))]
        id: StringType,
        #[dto(list(filter, order_by))]
        name: StringType,
    }

    fn run_sql(list_params: &ListParams) -> String {
        use sea_query::{PostgresQueryBuilder, Query};

        let qp: ListQueryParams<RunDto> = list_params.try_into().unwrap();
        qp.apply_to(
            Query::select()
                .column(<RunDao as TypedColumns>::Iden::Id)
                .from(<RunDao as TypedColumns>::Iden::Table)
                .to_owned(),
        )
        .to_string(PostgresQueryBuilder)
    }

    #[test]
    fn test_cursor_sql_default_order_next() {
        let list_params = ListParamsBuilder::default()
            .len(2usize)
            .next("i2".to_string())
            .pagination_id("i2".to_string())
            .build()
            .unwrap();
        assert_eq!(
            run_sql(&list_params),
            r#"SELECT "id" FROM "rundao" WHERE "id" > 'i2' ORDER BY "id" ASC LIMIT 3"#
        );
    }

    #[test]
    fn test_cursor_sql_desc_order_next() {
        // The id tiebreak follows the primary direction.
        let list_params = ListParamsBuilder::default()
            .len(2usize)
            .order_by("name-".to_string())
            .next("n2".to_string())
            .pagination_id("i2".to_string())
            .build()
            .unwrap();
        assert_eq!(
            run_sql(&list_params),
            r#"SELECT "id" FROM "rundao" WHERE "name" < 'n2' OR ("name" = 'n2' AND "id" < 'i2') ORDER BY "name" DESC NULLS FIRST, "id" DESC LIMIT 3"#
        );
    }

    #[test]
    fn test_cursor_sql_desc_order_previous() {
        // Previous inverts both columns.
        let list_params = ListParamsBuilder::default()
            .len(2usize)
            .order_by("name-".to_string())
            .previous("n2".to_string())
            .pagination_id("i2".to_string())
            .build()
            .unwrap();
        assert_eq!(
            run_sql(&list_params),
            r#"SELECT "id" FROM "rundao" WHERE "name" > 'n2' OR ("name" = 'n2' AND "id" > 'i2') ORDER BY "name" ASC NULLS LAST, "id" ASC LIMIT 3"#
        );
    }

    #[ta_objects::Dao]
    struct NullableDao {
        id: StringType,
        ended: Option<StringType>,
    }

    #[ta_objects::Dto]
    #[dto(list(on = NullableDao))]
    struct NullableDto {
        #[dto(list(pagination_by = "+", filter))]
        id: StringType,
        #[dto(list(filter, order_by))]
        ended: Option<StringType>,
    }

    fn nullable_sql(list_params: &ListParams) -> String {
        use sea_query::{PostgresQueryBuilder, Query};

        let qp: ListQueryParams<NullableDto> = list_params.try_into().unwrap();
        qp.apply_to(
            Query::select()
                .column(<NullableDao as TypedColumns>::Iden::Id)
                .from(<NullableDao as TypedColumns>::Iden::Table)
                .to_owned(),
        )
        .to_string(PostgresQueryBuilder)
    }

    #[test]
    fn test_empty_filter_value_on_nullable_column_rejected() {
        let err = parse_condition::<NullableDto>("ended:eq:").unwrap_err();
        let err = err.domain_err::<ListError>();
        assert!(matches!(err, ListError::EmptyFilterValue(_)));
    }

    #[test]
    fn test_nullable_order_next_reaches_null_run() {
        // A non-NULL forward cursor must still reach the NULL run at the tail.
        let list_params = ListParamsBuilder::default()
            .len(2usize)
            .order_by("ended".to_string())
            .next("e2".to_string())
            .pagination_id("i2".to_string())
            .build()
            .unwrap();
        assert_eq!(
            nullable_sql(&list_params),
            r#"SELECT "id" FROM "nullabledao" WHERE "ended" > 'e2' OR ("ended" = 'e2' AND "id" > 'i2') OR "ended" IS NULL ORDER BY "ended" ASC NULLS LAST, "id" ASC LIMIT 3"#
        );
    }

    #[test]
    fn test_nullable_order_next_inside_null_run() {
        // An empty order value is the NULL cursor: walk the run by id alone.
        let list_params = ListParamsBuilder::default()
            .len(2usize)
            .order_by("ended".to_string())
            .next("".to_string())
            .pagination_id("i2".to_string())
            .build()
            .unwrap();
        assert_eq!(
            nullable_sql(&list_params),
            r#"SELECT "id" FROM "nullabledao" WHERE "ended" IS NULL AND "id" > 'i2' ORDER BY "ended" ASC NULLS LAST, "id" ASC LIMIT 3"#
        );
    }

    #[test]
    fn test_nullable_order_previous_from_null_run() {
        // Backing out of the NULL run re-enters the non-NULL rows.
        let list_params = ListParamsBuilder::default()
            .len(2usize)
            .order_by("ended".to_string())
            .previous("".to_string())
            .pagination_id("i2".to_string())
            .build()
            .unwrap();
        assert_eq!(
            nullable_sql(&list_params),
            r#"SELECT "id" FROM "nullabledao" WHERE ("ended" IS NULL AND "id" < 'i2') OR "ended" IS NOT NULL ORDER BY "ended" DESC NULLS FIRST, "id" DESC LIMIT 3"#
        );
    }

    #[test]
    fn test_nullable_order_desc_next_leaves_null_run() {
        // Descending puts NULLs first; a non-NULL cursor never re-enters them.
        let list_params = ListParamsBuilder::default()
            .len(2usize)
            .order_by("ended-".to_string())
            .next("e2".to_string())
            .pagination_id("i2".to_string())
            .build()
            .unwrap();
        assert_eq!(
            nullable_sql(&list_params),
            r#"SELECT "id" FROM "nullabledao" WHERE "ended" < 'e2' OR ("ended" = 'e2' AND "id" < 'i2') ORDER BY "ended" DESC NULLS FIRST, "id" DESC LIMIT 3"#
        );
    }

    #[test]
    fn test_like_filter_generates_escape_clause() {
        use sea_query::{Query, SqliteQueryBuilder};

        let list_params = ListParamsBuilder::default()
            .len(10usize)
            .filter(vec!["a:lk:test*".to_string()])
            .build()
            .unwrap();
        let qp: ListQueryParams<TestDto> = (&list_params).try_into().unwrap();

        let sql = qp
            .apply_to(
                Query::select()
                    .column(<TestDao as TypedColumns>::Iden::A)
                    .from(<TestDao as TypedColumns>::Iden::Table)
                    .to_owned(),
            )
            .to_string(SqliteQueryBuilder);

        assert!(
            sql.contains(r"ESCAPE '\'"),
            "LIKE filter must include ESCAPE clause: {sql}"
        );
        assert!(
            sql.contains("LIKE 'test%'"),
            "* should be converted to SQL %: {sql}"
        );
    }

    #[test]
    fn test_like_filter_escapes_sql_wildcards() {
        use sea_query::{Query, SqliteQueryBuilder};

        // SQL % in user input should be escaped
        let list_params = ListParamsBuilder::default()
            .len(10usize)
            .filter(vec!["a:lk:100%".to_string()])
            .build()
            .unwrap();
        let qp: ListQueryParams<TestDto> = (&list_params).try_into().unwrap();

        let sql = qp
            .apply_to(
                Query::select()
                    .column(<TestDao as TypedColumns>::Iden::A)
                    .from(<TestDao as TypedColumns>::Iden::Table)
                    .to_owned(),
            )
            .to_string(SqliteQueryBuilder);

        assert!(
            sql.contains(r"LIKE '100\%'"),
            "SQL % must be escaped in LIKE pattern: {sql}"
        );
        assert!(
            sql.contains(r"ESCAPE '\'"),
            "ESCAPE clause required for escaped wildcards: {sql}"
        );
    }

    #[test]
    fn test_like_filter_wildcard_and_escape_combined() {
        use sea_query::{Query, SqliteQueryBuilder};

        // Mix: * wildcard + escaped \* literal + SQL _ escaped
        let list_params = ListParamsBuilder::default()
            .len(10usize)
            .filter(vec![r"a:lk:*foo\_bar\*".to_string()])
            .build()
            .unwrap();
        let qp: ListQueryParams<TestDto> = (&list_params).try_into().unwrap();

        let sql = qp
            .apply_to(
                Query::select()
                    .column(<TestDao as TypedColumns>::Iden::A)
                    .from(<TestDao as TypedColumns>::Iden::Table)
                    .to_owned(),
            )
            .to_string(SqliteQueryBuilder);

        // *     → %
        // foo   → foo
        // \_    → \_  (SQL _ always escaped)
        // bar   → bar
        // \*    → *   (escaped user wildcard → literal *)
        assert!(
            sql.contains(r"LIKE '%foo\_bar*'"),
            "Combined wildcards and escapes must be correct: {sql}"
        );
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
            let result = convert_to_like_pattern(input).unwrap();
            assert_eq!(*result, expected);
        }
    }
}
