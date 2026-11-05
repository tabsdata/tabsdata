//
// Copyright 2026 Tabsdata Inc.
//

//! The catalog reads a derived index of the catalogs is kept with, and the
//! one write that tells it a catalog's texts changed. The index lives
//! elsewhere and reads only these: the catalogs a caller sees, the catalogs
//! whose texts changed after a revision, with their entities, and a page of
//! the live catalogs' revisions. `text_revision` is a value of one sequence,
//! so it is both a version per catalog and a cursor over every catalog.

use crate::primitives::scd2::open_interval_at;
use crate::{QueryExecutor, Writes};
use sea_query::{Alias, Expr, ExprTrait, Func, OnConflict, Order, Query, SubQueryStatement};
use ta_objects::errors::SqlError;
use ta_objects::{Dao, HasColumn, Scd2Dao, SeaValue};
use td_database::access::{Holds, InProject, ProjectLock, SeesProjectRows};
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::connection_catalog::{
    CatalogEntityTextRow, CatalogFilter, CatalogRevisionRow, CatalogTextRow, CatalogVisibleRow,
    ConnectionCatalogDB, ConnectionCatalogDBIden as Cat, ConnectionCatalogDBWithNames,
    ConnectionCatalogDBWithNamesIden as View, ConnectionEntityDB, ConnectionEntityDBIden as Ent,
};
use td_objects::dxo::tag::TagDB;
use td_objects::types::basic::{
    ConnectionEntityId, ConnectionId, TagEntityId, TagEntityType, TagKey, TagKind, TagValue,
    TextRevision,
};
use td_objects::types::visible_projects::VisibleProjects;

/// The catalog view's column of the collection's description, which its DAO
/// does not read.
const COLLECTION_DESCRIPTION: &str = "collection_description";
const COLLECTION_TAGS: &str = "collection_tags";

pub struct ConnectionCatalogs<E> {
    pub(crate) executor: E,
}

fn tag_column<T>() -> (<TagDB as Dao>::Iden, <TagDB as Dao>::Iden)
where
    TagDB: HasColumn<T>,
{
    (TagDB::TABLE, <TagDB as HasColumn<T>>::COL)
}

/// The open user tags of the view row's collection, as a JSONB object of key
/// to value; `{}` when it has none.
fn collection_tags() -> Expr {
    let tags = Query::select()
        .expr(
            Func::cust("jsonb_object_agg")
                .arg(Expr::col(tag_column::<TagKey>()))
                .arg(Expr::col(tag_column::<TagValue>())),
        )
        .from(TagDB::TABLE)
        .and_where(
            Expr::col(tag_column::<TagEntityType>()).eq(TagEntityType::Collection.as_sea_expr()),
        )
        .and_where(
            Expr::col(tag_column::<TagEntityId>())
                .equals((ConnectionCatalogDBWithNames::TABLE, View::CollectionId)),
        )
        .and_where(Expr::col(tag_column::<TagKind>()).eq(TagKind::User.as_sea_expr()))
        .and_where(open_interval_at((TagDB::TABLE, TagDB::VALID_TO)))
        .take();
    Expr::expr(Func::coalesce([
        Expr::SubQuery(None, Box::new(SubQueryStatement::SelectStatement(tags))),
        Expr::cust("'{}'::jsonb"),
    ]))
}

fn texts() -> sea_query::SelectStatement {
    Query::select()
        .column(View::ConnectionId)
        .column(View::TextRevision)
        .column(View::Collection)
        .expr_as(
            Expr::expr(Func::coalesce([
                Expr::col(Alias::new(COLLECTION_DESCRIPTION)),
                Expr::val(""),
            ])),
            Alias::new(COLLECTION_DESCRIPTION),
        )
        .column(View::Kind)
        .expr_as(collection_tags(), Alias::new(COLLECTION_TAGS))
        .from(ConnectionCatalogDBWithNames::TABLE)
        .to_owned()
}

fn ids_of(ids: &[ConnectionId]) -> Vec<Expr> {
    ids.iter().map(|id| id.as_sea_expr()).collect()
}

impl<E> ConnectionCatalogs<E>
where
    E: QueryExecutor + Domained,
    E::Scope: SeesProjectRows,
{
    /// The connected collections within `visible` that `filter` keeps, by
    /// connection, each with whether it is within `develop` too.
    pub async fn visible(
        self,
        filter: &CatalogFilter,
        visible: &VisibleProjects,
        develop: &VisibleProjects,
    ) -> Result<Vec<CatalogVisibleRow>, SqlError> {
        let mut select = Query::select();
        select
            .column(View::ConnectionId)
            .column(View::Collection)
            .expr_as(
                Expr::case(develop.narrows(Expr::col(View::ProjectId)), true).finally(false),
                Alias::new("develop"),
            )
            .from(ConnectionCatalogDBWithNames::TABLE)
            .cond_where(visible.narrows(Expr::col(View::ProjectId)))
            .order_by(View::ConnectionId, Order::Asc);
        if let Some(kind) = &filter.kind {
            select.and_where(Expr::col(View::Kind).eq(kind.as_sea_expr()));
        }
        if let Some(collection_type) = &filter.collection_type {
            select.and_where(Expr::col(View::CollectionType).eq(collection_type.as_sea_expr()));
        }
        if let Some(project) = &filter.project {
            select.and_where(Expr::col(View::Project).eq(project.as_sea_expr()));
        }
        if let Some(since) = &filter.metadata_since {
            select.and_where(Expr::col(View::MetadataOn).gte(since.as_sea_expr()));
        }
        select
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<ConnectionCatalogDBWithNames>)
    }

    /// The connected collections of `ids` within `visible`; one deleted, or
    /// moved out of sight, is absent.
    pub async fn of(
        self,
        ids: &[ConnectionId],
        visible: &VisibleProjects,
    ) -> Result<Vec<ConnectionCatalogDBWithNames>, SqlError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        Query::select()
            .columns(ConnectionCatalogDBWithNames::columns())
            .from(ConnectionCatalogDBWithNames::TABLE)
            .and_where(Expr::col(View::ConnectionId).is_in(ids_of(ids)))
            .cond_where(visible.narrows(Expr::col(View::ProjectId)))
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<ConnectionCatalogDBWithNames>)
    }

    /// The live catalogs whose texts changed after `after`, the oldest change
    /// first, at most `limit`. Revisions can commit out of order, and a
    /// deleted collection's catalog just leaves this view, so a reader
    /// following them as a cursor also compares [`Self::live`] now and then.
    pub async fn changed_after(
        self,
        after: &TextRevision,
        limit: u64,
    ) -> Result<Vec<CatalogTextRow>, SqlError> {
        texts()
            .and_where(Expr::col(View::TextRevision).gt(after.as_sea_expr()))
            .order_by(View::TextRevision, Order::Asc)
            .order_by(View::ConnectionId, Order::Asc)
            .limit(limit)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<ConnectionCatalogDBWithNames>)
    }

    /// The entities of the catalogs of `ids`, by catalog and path.
    pub async fn entity_texts(
        self,
        ids: &[ConnectionId],
    ) -> Result<Vec<CatalogEntityTextRow>, SqlError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        Query::select()
            .columns([
                Ent::Id,
                Ent::ConnectionId,
                Ent::Fqn,
                Ent::EntityKind,
                Ent::FieldNames,
            ])
            .from(ConnectionEntityDB::TABLE)
            .and_where(Expr::col(Ent::ConnectionId).is_in(ids_of(ids)))
            .order_by(Ent::ConnectionId, Order::Asc)
            .order_by(Ent::Fqn, Order::Asc)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<ConnectionEntityDB>)
    }

    /// The live catalogs of `ids` with their texts, as
    /// [`Self::changed_after`] reads them.
    pub async fn texts_of(self, ids: &[ConnectionId]) -> Result<Vec<CatalogTextRow>, SqlError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        texts()
            .and_where(Expr::col(View::ConnectionId).is_in(ids_of(ids)))
            .and_where(Expr::col(View::TextRevision).gt(0i64))
            .order_by(View::ConnectionId, Order::Asc)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<ConnectionCatalogDBWithNames>)
    }

    /// The entities of `ids` within `develop`; one deleted, or out of sight,
    /// is absent.
    pub async fn entities(
        self,
        ids: &[ConnectionEntityId],
        develop: &VisibleProjects,
    ) -> Result<Vec<CatalogEntityTextRow>, SqlError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        Query::select()
            .columns([
                Ent::Id,
                Ent::ConnectionId,
                Ent::Fqn,
                Ent::EntityKind,
                Ent::FieldNames,
            ])
            .from(ConnectionEntityDB::TABLE)
            .and_where(Expr::col(Ent::Id).is_in(ids.iter().map(|id| id.as_sea_expr())))
            .cond_where(develop.narrows(Expr::col(Ent::ProjectId)))
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<ConnectionEntityDB>)
    }

    /// The live catalogs after `after` by connection, at most `limit`, each
    /// with its text revision and schema fingerprint.
    pub async fn live(
        self,
        after: Option<&ConnectionId>,
        limit: u64,
    ) -> Result<Vec<CatalogRevisionRow>, SqlError> {
        let mut select = Query::select();
        select
            .column(View::ConnectionId)
            .column(View::TextRevision)
            .column(View::SchemaHash)
            .from(ConnectionCatalogDBWithNames::TABLE)
            .and_where(Expr::col(View::TextRevision).gt(0i64))
            .order_by(View::ConnectionId, Order::Asc)
            .limit(limit);
        if let Some(after) = after {
            select.and_where(Expr::col(View::ConnectionId).gt(after.as_sea_expr()));
        }
        select
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<ConnectionCatalogDBWithNames>)
    }
}

impl<E> ConnectionCatalogs<E>
where
    E: Writes<Scope = InProject, Mode: Holds<ProjectLock>>,
{
    /// The catalog's texts changed: give it the next text revision (the
    /// identity column's default, so the serving role needs no grant on its
    /// sequence). A connection without a catalog row has nothing to mark.
    pub async fn text_changed(self, connection_id: &ConnectionId) -> Result<(), SqlError> {
        Query::update()
            .table(ConnectionCatalogDB::TABLE)
            .value(Cat::TextRevision, Expr::cust("DEFAULT"))
            .and_where(Expr::col(Cat::ConnectionId).eq(connection_id.as_sea_expr()))
            .build_query()
            .execute(self.executor)
            .await
            .map(drop)
            .map_err(|e| SqlError::on_write::<ConnectionCatalogDB>(e, &[]))
    }

    /// The connected collection's texts were written: its catalog row is
    /// created from the active collection when it has none, so a connection
    /// never probed is indexed too; one it has gets the next text revision.
    pub async fn texts_written(self, connection_id: &ConnectionId) -> Result<(), SqlError> {
        let from = Query::select()
            .columns([
                View::ConnectionId,
                View::ProjectId,
                View::CollectionId,
                View::Kind,
            ])
            .from(ConnectionCatalogDBWithNames::TABLE)
            .and_where(Expr::col(View::ConnectionId).eq(connection_id.as_sea_expr()))
            .to_owned();
        Query::insert()
            .into_table(ConnectionCatalogDB::TABLE)
            .columns([
                Cat::ConnectionId,
                Cat::ProjectId,
                Cat::CollectionId,
                Cat::Kind,
            ])
            .select_from(from)?
            .on_conflict(
                OnConflict::column(Cat::ConnectionId)
                    .value(Cat::TextRevision, Expr::cust("DEFAULT"))
                    .to_owned(),
            )
            .build_query()
            .execute(self.executor)
            .await
            .map(drop)
            .map_err(|e| SqlError::on_write::<ConnectionCatalogDB>(e, &[]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;

    #[test]
    fn test_texts_carry_the_collections_open_user_tags() {
        assert_eq!(
            texts().to_string(PostgresQueryBuilder),
            r#"SELECT "connection_id", "text_revision", "collection", COALESCE("collection_description", '') AS "collection_description", "kind", COALESCE((SELECT jsonb_object_agg("tags"."key", "tags"."value") FROM "tags" WHERE "tags"."entity_type" = 'C' AND "tags"."entity_id" = "connection_catalogs__with_names"."collection_id" AND "tags"."kind" = 'U' AND "tags"."valid_to" = ('infinity'::timestamptz)), '{}'::jsonb) AS "collection_tags" FROM "connection_catalogs__with_names""#
        );
    }
}
