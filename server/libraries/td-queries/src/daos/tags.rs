//
// Copyright 2026 Tabsdata Inc.
//

//! The tag vocabulary in use: the keys open on the project's entities and the
//! values one key takes.

use crate::Serves;
use crate::generic::grain::{Current, Grain};
use sea_query::{Condition, Expr, ExprTrait, Order, Query, SelectStatement};
use ta_objects::errors::SqlError;
use ta_objects::{Dao, HasColumn, SeaValue};
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::tag::{TagDB, TagFacetParams};
use td_objects::types::basic::{TagEntityType, TagKey, TagKind, TagPrefix, TagValue};

pub struct Tags<E> {
    pub(crate) executor: E,
}

impl<E> Tags<E>
where
    E: Serves<TagDB>,
{
    pub async fn keys(self, facet: &TagFacetParams) -> Result<Vec<TagKey>, SqlError> {
        keys_query(facet)
            .build_query_scalar()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<TagDB>)
    }

    pub async fn values(
        self,
        key: &TagKey,
        facet: &TagFacetParams,
    ) -> Result<Vec<TagValue>, SqlError> {
        values_query(key, facet)
            .build_query_scalar()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<TagDB>)
    }
}

fn column<T>() -> (<TagDB as Dao>::Iden, <TagDB as Dao>::Iden)
where
    TagDB: HasColumn<T>,
{
    (TagDB::TABLE, <TagDB as HasColumn<T>>::COL)
}

/// `prefix` as a LIKE pattern under the default `\` escape, which keeps it a
/// range scan of the `text_pattern_ops` index.
fn starts_with(prefix: &TagPrefix) -> String {
    let mut pattern = String::with_capacity(prefix.len() + 1);
    for c in prefix.chars() {
        if matches!(c, '\\' | '%' | '_') {
            pattern.push('\\');
        }
        pattern.push(c);
    }
    pattern.push('%');
    pattern
}

fn facet_query<T>(facet: &TagFacetParams) -> SelectStatement
where
    TagDB: HasColumn<T>,
{
    let mut statement = Query::select()
        .distinct()
        .column(column::<T>())
        .from(TagDB::TABLE)
        .cond_where(<Current as Grain<TagDB>>::condition(&Current).unwrap_or_else(Condition::all))
        .take();
    if let Some(entity_type) = &facet.entity_type {
        statement.and_where(Expr::col(column::<TagEntityType>()).eq(entity_type.as_sea_expr()));
    }
    if let Some(kind) = &facet.kind {
        statement.and_where(Expr::col(column::<TagKind>()).eq(kind.as_sea_expr()));
    }
    if let Some(prefix) = &facet.prefix {
        statement.and_where(Expr::col(column::<T>()).like(starts_with(prefix)));
    }
    statement
        .order_by(column::<T>(), Order::Asc)
        .limit(*facet.len as u64)
        .take()
}

pub(crate) fn keys_query(facet: &TagFacetParams) -> SelectStatement {
    facet_query::<TagKey>(facet)
}

pub(crate) fn values_query(key: &TagKey, facet: &TagFacetParams) -> SelectStatement {
    let mut statement = facet_query::<TagValue>(facet);
    statement.and_where(Expr::col(column::<TagKey>()).eq(key.as_sea_expr()));
    statement
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;
    use ta_objects::Typed;
    use td_objects::types::basic::TagFacetLen;

    fn facet(
        entity_type: Option<TagEntityType>,
        kind: Option<TagKind>,
        prefix: Option<&str>,
    ) -> TagFacetParams {
        TagFacetParams {
            entity_type,
            kind,
            prefix: prefix.map(|p| TagPrefix::try_new(p).unwrap()),
            len: TagFacetLen::default(),
        }
    }

    #[test]
    fn test_keys_in_use() {
        assert_eq!(
            keys_query(&facet(None, None, None)).to_string(PostgresQueryBuilder),
            r#"SELECT DISTINCT "tags"."key" FROM "tags" WHERE "valid_to" = ('infinity'::timestamptz) ORDER BY "tags"."key" ASC LIMIT 100"#
        );
        assert_eq!(
            keys_query(&facet(
                Some(TagEntityType::Table),
                Some(TagKind::User),
                None
            ))
            .to_string(PostgresQueryBuilder),
            r#"SELECT DISTINCT "tags"."key" FROM "tags" WHERE "valid_to" = ('infinity'::timestamptz) AND "tags"."entity_type" = 'T' AND "tags"."kind" = 'U' ORDER BY "tags"."key" ASC LIMIT 100"#
        );
    }

    #[test]
    fn test_values_of_a_key_by_literal_prefix() {
        let key = TagKey::try_new("cost-center").unwrap();
        assert_eq!(
            values_query(&key, &facet(None, None, Some("eu_1%"))).to_string(PostgresQueryBuilder),
            r#"SELECT DISTINCT "tags"."value" FROM "tags" WHERE "valid_to" = ('infinity'::timestamptz) AND "tags"."value" LIKE E'eu\\_1\\%%' AND "tags"."key" = 'cost-center' ORDER BY "tags"."value" ASC LIMIT 100"#
        );
    }
}
