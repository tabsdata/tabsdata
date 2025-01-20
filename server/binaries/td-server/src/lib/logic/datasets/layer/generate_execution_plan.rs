//
// Copyright 2024 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use td_common::error::TdError;
use td_execution::dataset::{RelativeVersions, ResolvedVersion};
use td_execution::execution_planner::{ExecutionPlan, ExecutionTemplate};
use td_execution::version_finder::SqlVersionFinder;
use td_execution::version_resolver::VersionResolver;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn generate_execution_plan(
    Connection(connection): Connection,
    Input(trigger_time): Input<DateTime<Utc>>,
    Input(execution_template): Input<ExecutionTemplate>,
) -> Result<ExecutionPlan, TdError> {
    let execution_plan = execution_template
        .versioned(|dataset, relative_versions| {
            let connection = connection.clone();
            let trigger_time = trigger_time.clone();
            async move {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let lookup_versions = match relative_versions.clone() {
                    RelativeVersions::Plan(versions) => versions,
                    RelativeVersions::Current(versions) => versions,
                    RelativeVersions::Same(mut versions) => {
                        // We are correcting self HEAD references to the previous version to lookup,
                        // given that the HEAD of self and the HEAD of another dataset are not the same.
                        versions.shift(-1);
                        versions
                    }
                };

                let mut version_finder = SqlVersionFinder::new(conn, dataset, &trigger_time);
                let absolute_versions = VersionResolver::new(&mut version_finder, &lookup_versions)
                    .resolve()
                    .await?;

                let relative_versions = relative_versions.clone();
                let resolved_version = ResolvedVersion::new(absolute_versions, relative_versions);
                Ok::<_, TdError>(resolved_version)
            }
        })
        .await?;

    Ok(execution_plan)
}
