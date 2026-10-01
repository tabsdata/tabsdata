//
// Copyright 2026 Tabsdata Inc.
//

//! Function-version lookups the stage-trigger builder needs, chunked over
//! the caller's whole fvid set.

use crate::QueryExecutor;
use crate::daos::function_runs::LogicalRuns;
use crate::primitives::chunked::fetch_chunked;
use sea_query::{Alias, Expr, ExprTrait, Func, JoinType, Query, SelectStatement};
use std::collections::HashMap;
use ta_objects::Dao;
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_objects::dxo::bundle::BundleDBIden;
use td_objects::dxo::collection::{ActiveCollectionDBIden, ResolvedCollectionDBIden};
use td_objects::dxo::function::{FunctionDB, FunctionDBIden, FunctionDBWithBundleBasePath};
use td_objects::types::basic::{BasePath, FunctionVersionId};

impl<E> LogicalRuns<E>
where
    E: QueryExecutor,
{
    /// Effective output `base_path` per function version: the version's own
    /// override, else the owning collection's effective prefix.
    pub async fn effective_output_base_paths(
        self,
        fvids: &[FunctionVersionId],
    ) -> Result<HashMap<FunctionVersionId, BasePath>, SqlError> {
        let rows: Vec<(FunctionVersionId, BasePath)> =
            fetch_chunked::<FunctionDB, _, _, _>(self.executor, fvids, |chunk| {
                fetch_effective_output_base_paths_query(chunk)
            })
            .await?;
        Ok(rows.into_iter().collect())
    }

    /// `FunctionDB` rows with the effective bundle's stamped `base_path`.
    pub async fn with_bundle_base_path(
        self,
        fvids: &[FunctionVersionId],
    ) -> Result<Vec<FunctionDBWithBundleBasePath>, SqlError> {
        fetch_chunked::<FunctionDB, _, _, _>(self.executor, fvids, |chunk| {
            fetch_functions_with_bundle_base_path_query(chunk)
        })
        .await
    }
}

fn fvid_in(fvids: &[FunctionVersionId]) -> sea_query::SimpleExpr {
    Expr::col((FunctionDBIden::Table, FunctionDBIden::Id))
        .is_in(fvids.iter().map(SeaValue::as_sea_value))
}

/// `(id, base_path)`, decoded positionally into a tuple. The collection's
/// `effective_base_path` is NOT NULL, so the COALESCE always resolves.
pub(crate) fn fetch_effective_output_base_paths_query(
    fvids: &[FunctionVersionId],
) -> SelectStatement {
    Query::select()
        .expr_as(
            Expr::col((FunctionDBIden::Table, FunctionDBIden::Id)),
            FunctionDBIden::Id,
        )
        .expr_as(
            Expr::expr(Func::coalesce([
                Expr::col((FunctionDBIden::Table, FunctionDBIden::BasePath)),
                Expr::col((
                    ResolvedCollectionDBIden::Table,
                    ActiveCollectionDBIden::EffectiveBasePath,
                )),
            ])),
            Alias::new("base_path"),
        )
        .from(FunctionDBIden::Table)
        .join(
            JoinType::Join,
            ResolvedCollectionDBIden::Table,
            Expr::col((ResolvedCollectionDBIden::Table, ActiveCollectionDBIden::Id))
                .equals((FunctionDBIden::Table, FunctionDBIden::CollectionId)),
        )
        .and_where(fvid_in(fvids))
        .take()
}

/// LEFT JOIN: a function whose bundle row is absent must still come back or
/// its stage-trigger worker would be silently dropped; `stage_base_path` is
/// NOT NULL, so `bundle_base_path` never decodes to NULL.
pub(crate) fn fetch_functions_with_bundle_base_path_query(
    fvids: &[FunctionVersionId],
) -> SelectStatement {
    let mut stmt = Query::select();
    for col in FunctionDB::columns() {
        stmt.column((FunctionDBIden::Table, col));
    }
    stmt.expr_as(
        Expr::expr(Func::coalesce([
            Expr::col((BundleDBIden::Table, BundleDBIden::BasePath)),
            Expr::col((FunctionDBIden::Table, FunctionDBIden::StageBasePath)),
        ])),
        Alias::new("bundle_base_path"),
    )
    .from(FunctionDBIden::Table)
    .join(
        JoinType::LeftJoin,
        BundleDBIden::Table,
        Expr::col((BundleDBIden::Table, BundleDBIden::Id))
            .equals((FunctionDBIden::Table, FunctionDBIden::BundleId)),
    )
    .and_where(fvid_in(fvids));
    stmt.take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;

    fn fvids() -> Vec<FunctionVersionId> {
        vec![FunctionVersionId::try_from("fv20260101-0000000001").unwrap()]
    }

    #[test]
    fn test_fetch_effective_output_base_paths_query() {
        let sql = fetch_effective_output_base_paths_query(&fvids()).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "functions"."id" AS "id", COALESCE("functions"."base_path", "collections_resolved"."effective_base_path") AS "base_path" FROM "functions" JOIN "collections_resolved" ON "collections_resolved"."id" = "functions"."collection_id" WHERE "functions"."id" IN (202601010000000001)"#
        );
    }

    #[test]
    fn test_fetch_functions_with_bundle_base_path_query() {
        let sql =
            fetch_functions_with_bundle_base_path_query(&fvids()).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "functions"."id", "functions"."parent_function_id", "functions"."collection_id", "functions"."name", "functions"."description", "functions"."decorator", "functions"."function_type", "functions"."connector", "functions"."connection", "functions"."runtime_values", "functions"."function_id", "functions"."base_path", "functions"."stage_id", "functions"."stage_base_path", "functions"."storage_version", "functions"."bundle_id", "functions"."snippet", "functions"."defined_by_id", "functions"."status", "functions"."enabled", "functions"."project_id", "functions"."valid_from", "functions"."valid_to", COALESCE("bundles"."base_path", "functions"."stage_base_path") AS "bundle_base_path" FROM "functions" LEFT JOIN "bundles" ON "bundles"."id" = "functions"."bundle_id" WHERE "functions"."id" IN (202601010000000001)"#
        );
    }
}
