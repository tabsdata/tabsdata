//
// Copyright 2026 Tabsdata Inc.
//

//! `/metrics` over the `metrics_facts` table and the point-in-time `/inflight`.
//! Facts are appended when an attempt ends and never deducted: a superseded
//! attempt keeps its rows with `current = false`, and reads filter.

mod inflight;
pub mod load;
pub(crate) mod read;
pub mod rows;
#[cfg(test)]
mod tests;
pub mod write;

use crate::QueryExecutor;
use crate::daos::metrics::inflight::inflight_query;
use crate::daos::metrics::read::per_table_query;
use ta_objects::errors::SqlError;
use td_database::access::SeesProjectRows;
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::metrics::{InflightRequest, InflightRow, MetricsRequest};
use td_objects::dxo::metrics_facts::{MetricsFactDB, MetricsTableRow};
use td_objects::types::visible_projects::VisibleProjects;

pub struct Metrics<E> {
    pub(crate) executor: E,
}

impl<E> Metrics<E>
where
    E: QueryExecutor + Domained,
    E::Scope: SeesProjectRows,
{
    pub async fn inflight(
        self,
        req: &InflightRequest,
        visible_projects: &VisibleProjects,
    ) -> Result<Vec<InflightRow>, SqlError> {
        inflight_query(req, visible_projects)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }

    pub async fn per_table(
        self,
        req: &MetricsRequest,
        visible_projects: &VisibleProjects,
    ) -> Result<Vec<MetricsTableRow>, SqlError> {
        req.window
            .validate()
            .map_err(|e| SqlError::DtoConversionError(td_error::TdError::new(e)))?;
        per_table_query(req, visible_projects)
            .map_err(SqlError::DtoConversionError)?
            .build_query_as()
            .fetch_all_planned_per_call(self.executor)
            .await
            .map_err(SqlError::on_read::<MetricsFactDB>)
    }
}

/// The metrics statement, for plan inspection in the load suite.
#[cfg(feature = "td-test")]
pub fn metrics_plan_sql(
    req: &MetricsRequest,
) -> Result<[(&'static str, String); 1], td_error::TdError> {
    use sea_query::PostgresQueryBuilder;
    Ok([(
        "per_table",
        per_table_query(req, &VisibleProjects::All)?.to_string(PostgresQueryBuilder),
    )])
}
