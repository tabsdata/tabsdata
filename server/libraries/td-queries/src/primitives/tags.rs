//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{Condition, Expr, ExprTrait, LikeExpr, Query, SelectStatement};
use ta_objects::{Dao, HasColumn, SeaValue};
use td_objects::dxo::tag::TagDB;
use td_objects::types::basic::{TagEntityId, TagEntityType, TagKey, TagValue};
use td_objects::types::tag_selector::{TagSelector, TagTerm, TagTest};

/// The rows of `D` meeting the selector, over the tag versions `visible`
/// admits.
pub(crate) fn tag_selection<D: Dao>(
    selector: &TagSelector<D::Iden>,
    visible: &Condition,
) -> Condition {
    selector.0.iter().fold(Condition::all(), |all, term| {
        let any = term.tests.iter().fold(Condition::any(), |any, test| {
            any.add(test_holds::<D>(term, test, visible))
        });
        all.add(any)
    })
}

fn test_holds<D: Dao>(term: &TagTerm<D::Iden>, test: &TagTest, visible: &Condition) -> Expr {
    let column = |col| Expr::col((TagDB::TABLE, col));
    let value = || column(<TagDB as HasColumn<TagValue>>::COL);
    let mut versions: SelectStatement = Query::select()
        .expr(Expr::val(1))
        .from(TagDB::TABLE)
        .and_where(column(<TagDB as HasColumn<TagEntityType>>::COL).eq(term.entity_type.clone()))
        .and_where(column(<TagDB as HasColumn<TagEntityId>>::COL).equals((D::TABLE, term.column)))
        .and_where(column(<TagDB as HasColumn<TagKey>>::COL).eq(term.key.as_sea_expr()))
        .cond_where(visible.clone())
        .take();
    match test {
        TagTest::Equals(v) => Expr::exists(versions.and_where(value().eq(v.as_sea_expr())).take()),
        TagTest::NotEquals(v) => {
            Expr::exists(versions.and_where(value().ne(v.as_sea_expr())).take())
        }
        TagTest::Like(pattern) => Expr::exists(
            versions
                .and_where(value().like(LikeExpr::new(pattern.as_str()).escape('\\')))
                .take(),
        ),
        TagTest::Present => Expr::exists(versions),
        TagTest::Absent => Expr::exists(versions).not(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;
    use td_objects::dxo::table::{Table, TableDBWithNames};

    fn tables_where(filters: &[&str]) -> String {
        let selector = TagSelector::parse::<Table, _>(filters).unwrap();
        Query::select()
            .expr(Expr::val(1))
            .from(TableDBWithNames::TABLE)
            .cond_where(tag_selection::<TableDBWithNames>(
                &selector,
                &Condition::all().add(Expr::cust("seen")),
            ))
            .to_string(PostgresQueryBuilder)
    }

    #[test]
    fn test_a_tag_filter_tests_the_entity_tags_the_read_sees() {
        assert_eq!(
            tables_where(&["table._parent:eq:abc"]),
            r#"SELECT 1 FROM "tables__with_names" WHERE EXISTS(SELECT 1 FROM "tags" WHERE "tags"."entity_type" = 'T' AND "tags"."entity_id" = "tables__with_names"."table_id" AND "tags"."key" = '_parent' AND (seen) AND "tags"."value" = 'abc')"#
        );
    }

    #[test]
    fn test_a_tag_filter_on_a_referenced_entity_joins_its_column() {
        assert_eq!(
            tables_where(&["function.team:eq:a"]),
            r#"SELECT 1 FROM "tables__with_names" WHERE EXISTS(SELECT 1 FROM "tags" WHERE "tags"."entity_type" = 'F' AND "tags"."entity_id" = "tables__with_names"."function_id" AND "tags"."key" = 'team' AND (seen) AND "tags"."value" = 'a')"#
        );
    }

    #[test]
    fn test_each_tag_test() {
        let ne = tables_where(&["table.team:ne:a"]);
        assert!(
            ne.contains(r#"EXISTS(SELECT 1"#) && ne.contains(r#""tags"."value" <> 'a'"#),
            "{ne}"
        );
        let like = tables_where(&["table.team:lk:data*"]);
        assert!(
            like.contains(r#""tags"."value" LIKE 'data%' ESCAPE E'\\'"#),
            "{like}"
        );
        let present = tables_where(&["table.flag:is:not_null"]);
        assert!(
            present.starts_with(r#"SELECT 1 FROM "tables__with_names" WHERE EXISTS("#),
            "{present}"
        );
        assert!(!present.contains(r#""tags"."value""#), "{present}");
        let absent = tables_where(&["table.flag:is:null"]);
        assert!(absent.contains("WHERE NOT EXISTS("), "{absent}");
    }

    #[test]
    fn test_one_key_any_test_and_every_key() {
        let both = tables_where(&["table.team:eq:a", "table.env:eq:prod"]);
        assert!(both.contains(") AND EXISTS("), "{both}");
        let either = tables_where(&["table.team:eq:a", "table.team:eq:b"]);
        assert!(either.contains(") OR EXISTS("), "{either}");
        assert_eq!(
            tables_where(&[]),
            r#"SELECT 1 FROM "tables__with_names" WHERE TRUE"#
        );
    }
}
