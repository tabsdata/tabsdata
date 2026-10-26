//
// Copyright 2026 Tabsdata Inc.
//

//! The SQL of a parsed listing over its base select: tag filters at the
//! grain, column filters, the keyset cursor, the order and the page.

use crate::generic::grain::{Grain, select_in};
use crate::primitives::tags::tag_selection;
use sea_query::{
    Condition, Expr, ExprTrait, LikeExpr, NullOrdering, Order, PostgresQueryBuilder,
    SelectStatement,
};
use ta_objects::{ListDto, SortDir};
use td_error::TdError;
use td_objects::dxo::crudl::ListParams;
use td_objects::list::columns::{ColumnSelector, ColumnTest, Comparison};
use td_objects::list::params::ListQueryParams;
use td_objects::list::sort::{KeyValue, SortKeys, Toward};

/// `base` restricted, ordered and limited as `params` asks, one row past the
/// page so the response can tell there is more.
pub(crate) fn listing<T, G>(
    grain: &G,
    mut base: SelectStatement,
    params: &ListQueryParams<T>,
) -> SelectStatement
where
    T: ListDto,
    G: Grain<T::Dao>,
{
    if !params.tags.is_empty() {
        base.cond_where(tag_selection::<T::Dao>(&params.tags, &grain.tags_visible()));
    }
    if !params.columns.is_empty() {
        base.cond_where(column_selection(&params.columns));
    }
    let sort = &params.sort;
    let dir = match sort.toward() {
        Some(Toward::Previous) => sort.dir.reversed(),
        _ => sort.dir,
    };
    match &sort.keys {
        SortKeys::Tiebreak { tiebreak, after } => {
            if let Some((toward, value)) = after {
                base.and_where(beyond(toward_end(*toward, sort.dir))(
                    Expr::col(*tiebreak),
                    value.as_sea_expr(),
                ));
            }
            base.order_by(*tiebreak, order(dir));
        }
        SortKeys::By {
            by,
            tiebreak,
            after,
        } => {
            if let Some((toward, value, id)) = after {
                base.and_where(past_by(*by, *tiebreak, *toward, sort.dir, value, id));
            }
            // A requested column may be nullable: NULLs sort as the largest
            // values, which `past_by` relies on.
            let nulls = match dir {
                SortDir::Asc => NullOrdering::Last,
                SortDir::Desc => NullOrdering::First,
            };
            base.order_by_with_nulls(*by, order(dir), nulls);
            base.order_by(*tiebreak, order(dir));
        }
    }
    base.limit(*params.len as u64 + 1);
    base
}

/// The SQL a listing of `T` runs under `grain`, for plan guards.
pub fn list_plan_sql<T, G>(grain: &G, request: ListParams) -> Result<String, TdError>
where
    T: ListDto,
    G: Grain<T::Dao>,
{
    let params = ListQueryParams::<T>::try_from(request)?;
    let base = select_in::<T::Dao, G>(grain, ());
    Ok(listing(grain, base, &params).to_string(PostgresQueryBuilder))
}

fn order(dir: SortDir) -> Order {
    match dir {
        SortDir::Asc => Order::Asc,
        SortDir::Desc => Order::Desc,
    }
}

fn column_selection<I: sea_query::Iden + Copy + 'static>(columns: &ColumnSelector<I>) -> Condition {
    columns.0.iter().fold(Condition::all(), |all, term| {
        let any = term.tests.iter().fold(Condition::any(), |any, test| {
            any.add(test_holds(term.column, test))
        });
        all.add(any)
    })
}

fn test_holds<I: sea_query::Iden + 'static>(column: I, test: &ColumnTest) -> Expr {
    let column = Expr::col(column);
    match test {
        ColumnTest::Compare(comparison, value) => {
            let value = value.as_sea_expr();
            match comparison {
                Comparison::Eq => column.eq(value),
                Comparison::Ne => column.ne(value),
                Comparison::Gt => column.gt(value),
                Comparison::Ge => column.gte(value),
                Comparison::Lt => column.lt(value),
                Comparison::Le => column.lte(value),
            }
        }
        ColumnTest::Between(low, high) => column.between(low.as_sea_expr(), high.as_sea_expr()),
        ColumnTest::Like(pattern) => column.like(LikeExpr::new(pattern.as_str()).escape('\\')),
        ColumnTest::Null => column.is_null(),
        ColumnTest::NotNull => column.is_not_null(),
    }
}

/// Whether a page past a cursor lies toward the larger values.
fn toward_end(toward: Toward, dir: SortDir) -> bool {
    matches!(
        (toward, dir),
        (Toward::Next, SortDir::Asc) | (Toward::Previous, SortDir::Desc)
    )
}

fn beyond(toward_end: bool) -> fn(Expr, Expr) -> Expr {
    if toward_end { Expr::gt } else { Expr::lt }
}

/// The rows past `(value, id)` in `ORDER BY by, tiebreak`: toward the next
/// page of an ascending sort, `by > value OR (by = value AND tiebreak > id)`.
fn past_by<I: sea_query::Iden + Copy + 'static>(
    by: I,
    tiebreak: I,
    toward: Toward,
    dir: SortDir,
    value: &KeyValue,
    id: &KeyValue,
) -> Expr {
    let toward_end = toward_end(toward, dir);
    let beyond = beyond(toward_end);
    let past_tiebreak = beyond(Expr::col(tiebreak), id.as_sea_expr());
    // NULLs sort as the largest values, a run plain comparisons never
    // reach: walk it with IS NULL predicates.
    if value.is_sql_null() {
        let in_null_run = Expr::col(by).is_null().and(past_tiebreak);
        return if toward_end {
            in_null_run
        } else {
            in_null_run.or(Expr::col(by).is_not_null())
        };
    }
    let composite = beyond(Expr::col(by), value.as_sea_expr())
        .or(Expr::col(by).eq(value.as_sea_expr()).and(past_tiebreak));
    if value.is_nullable() && toward_end {
        composite.or(Expr::col(by).is_null())
    } else {
        composite
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Unversioned;
    use ta_objects::{Dao, FromDao, Typed, TypedOpenApi, TypedSerde, TypedSqlx};
    use td_error::TdError;
    use td_objects::dxo::crudl::ListParams;
    use td_objects::types::basic::PageLen;

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct StringType(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct RunName(String);

    #[ta_objects::Dao]
    #[dao(table = "rundao", ops(select, insert, delete))]
    struct RunDao {
        #[dao(pk)]
        id: StringType,
        name: RunName,
        ended: Option<RunEnded>,
    }

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct RunEnded(String);

    #[ta_objects::Dto]
    #[dto(list(on = RunDao))]
    struct RunDto {
        #[dto(list(pagination_by = "+", filter))]
        id: StringType,
        #[dto(list(filter, filter_like, order_by))]
        name: RunName,
        #[dto(list(filter, order_by))]
        ended: Option<RunEnded>,
    }

    impl FromDao for RunDto {
        type Dao = RunDao;

        fn from_dao(dao: RunDao) -> Result<Self, TdError> {
            Ok(Self {
                id: dao.id,
                name: dao.name,
                ended: dao.ended,
            })
        }
    }

    fn sql(params: ListParams) -> String {
        let params = ListQueryParams::<RunDto>::try_from(ListParams {
            len: PageLen::try_from(2).unwrap(),
            ..params
        })
        .unwrap();
        listing(
            &Unversioned,
            sea_query::Query::select()
                .column(<RunDao as Dao>::Iden::Id)
                .from(<RunDao as Dao>::TABLE)
                .take(),
            &params,
        )
        .to_string(PostgresQueryBuilder)
    }

    fn paged(order_by: Option<&str>, previous: Option<&str>, next: Option<&str>) -> ListParams {
        ListParams {
            order_by: order_by.map(str::to_string),
            previous: previous.map(str::to_string),
            next: next.map(str::to_string),
            pagination_id: Some("i2".to_string()),
            ..ListParams::default()
        }
    }

    fn filtered(filters: &[&str]) -> ListParams {
        ListParams {
            filter: filters.iter().map(|f| f.to_string()).collect(),
            ..ListParams::default()
        }
    }

    #[test]
    fn test_a_first_page_orders_by_the_pagination_column() {
        assert_eq!(
            sql(ListParams::default()),
            r#"SELECT "id" FROM "rundao" ORDER BY "id" ASC LIMIT 3"#
        );
    }

    #[test]
    fn test_one_column_any_test_and_every_column() {
        assert_eq!(
            sql(filtered(&["name:eq:a", "id:ne:x", "name:lk:b*"])),
            r#"SELECT "id" FROM "rundao" WHERE ("name" = 'a' OR "name" LIKE 'b%' ESCAPE E'\\') AND "id" <> 'x' ORDER BY "id" ASC LIMIT 3"#
        );
        assert_eq!(
            sql(filtered(&["name:btw:a::c", "ended:is:null"])),
            r#"SELECT "id" FROM "rundao" WHERE ("name" BETWEEN 'a' AND 'c') AND "ended" IS NULL ORDER BY "id" ASC LIMIT 3"#
        );
    }

    #[test]
    fn test_like_filter_escapes_sql_wildcards() {
        let sql = sql(filtered(&[r"name:lk:*100%foo\_bar\*"]));
        assert!(
            sql.contains(r"LIKE E'%100\\%foo\\_bar*' ESCAPE E'\\'"),
            "{sql}"
        );
    }

    #[test]
    fn test_cursor_sql_default_order_next() {
        assert_eq!(
            sql(ListParams {
                next: Some("i2".to_string()),
                ..paged(None, None, None)
            }),
            r#"SELECT "id" FROM "rundao" WHERE "id" > 'i2' ORDER BY "id" ASC LIMIT 3"#
        );
    }

    #[test]
    fn test_cursor_sql_desc_order_next() {
        // The tiebreak follows the primary direction.
        assert_eq!(
            sql(paged(Some("name-"), None, Some("n2"))),
            r#"SELECT "id" FROM "rundao" WHERE "name" < 'n2' OR ("name" = 'n2' AND "id" < 'i2') ORDER BY "name" DESC NULLS FIRST, "id" DESC LIMIT 3"#
        );
    }

    #[test]
    fn test_cursor_sql_desc_order_previous() {
        // Previous inverts both columns.
        assert_eq!(
            sql(paged(Some("name-"), Some("n2"), None)),
            r#"SELECT "id" FROM "rundao" WHERE "name" > 'n2' OR ("name" = 'n2' AND "id" > 'i2') ORDER BY "name" ASC NULLS LAST, "id" ASC LIMIT 3"#
        );
    }

    #[test]
    fn test_nullable_order_next_reaches_null_run() {
        // A non-NULL forward cursor must still reach the NULL run at the tail.
        assert_eq!(
            sql(paged(Some("ended"), None, Some("e2"))),
            r#"SELECT "id" FROM "rundao" WHERE "ended" > 'e2' OR ("ended" = 'e2' AND "id" > 'i2') OR "ended" IS NULL ORDER BY "ended" ASC NULLS LAST, "id" ASC LIMIT 3"#
        );
    }

    #[test]
    fn test_nullable_order_next_inside_null_run() {
        // The `~` marker is the NULL cursor: walk the run by id alone.
        assert_eq!(
            sql(paged(Some("ended"), None, Some("~"))),
            r#"SELECT "id" FROM "rundao" WHERE "ended" IS NULL AND "id" > 'i2' ORDER BY "ended" ASC NULLS LAST, "id" ASC LIMIT 3"#
        );
    }

    #[test]
    fn test_nullable_order_previous_from_null_run() {
        // Backing out of the NULL run re-enters the non-NULL rows.
        assert_eq!(
            sql(paged(Some("ended"), Some("~"), None)),
            r#"SELECT "id" FROM "rundao" WHERE ("ended" IS NULL AND "id" < 'i2') OR "ended" IS NOT NULL ORDER BY "ended" DESC NULLS FIRST, "id" DESC LIMIT 3"#
        );
    }

    #[test]
    fn test_nullable_order_desc_next_leaves_null_run() {
        // Descending puts NULLs first; a non-NULL cursor never re-enters them.
        assert_eq!(
            sql(paged(Some("ended-"), None, Some("e2"))),
            r#"SELECT "id" FROM "rundao" WHERE "ended" < 'e2' OR ("ended" = 'e2' AND "id" < 'i2') ORDER BY "ended" DESC NULLS FIRST, "id" DESC LIMIT 3"#
        );
    }
}
