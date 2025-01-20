//
// Copyright 2024 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use td_common::dataset::DatasetRef;
use td_common::error::TdError;
use td_common::id::{id, Id};
use td_execution::error::ExecutionPlannerError;
use td_execution::execution_planner::ExecutionPlan;
use td_objects::datasets::dao::*;
use td_objects::dlo::ExecutionPlanId;
use td_tower::extractors::Input;
use td_transaction::TransactionMap;

pub async fn build_execution_requirements(
    Input(execution_plan_id): Input<ExecutionPlanId>,
    Input(trigger_time): Input<DateTime<Utc>>,
    Input(transaction_ids): Input<TransactionMap<Id>>,
    Input(execution_plan): Input<ExecutionPlan>,
) -> Result<Vec<DsExecutionRequirement>, TdError> {
    let mut execution_requirements = vec![];

    // Dependency requirements. Note that we need ALL the requirements, because some versions
    // might not be available yet, because of other ongoing plans.
    // We do not need to create a requirement for the manual trigger, because of the implicit dependencies
    // a dataset has on its own versions.
    let (requirements, count) = execution_plan.requirements();

    // For each requirement, we need to create an execution requirement.
    for requirement in requirements.iter() {
        let source_versions = requirement.source_version();
        let target_versions = requirement.target_version();
        let target_dependency_count = *count.get(&requirement.target_version()).unwrap() as i64;

        let td_version = source_versions.relative_versions().versions();
        let source_formal_data_version = td_version.versions();
        let source_pos = td_version.position();

        let transaction_id = transaction_ids.get(requirement.target())?;

        // A single formal version could have exploded into different versions (range into multiple
        // absolute versions). We need to create an execution requirement for each of them.
        for source_version in source_versions.absolute_versions().iter() {
            // We will always have 1 target version for each source version for now, but
            // we might be able to generate multiple target versions in the future.
            let source_function_id = Some(source_version.function_id().to_string());
            let source_data_version = source_version.id().map(|id| id.to_string());
            let source_table_id = source_version.table_id().map(|id| id.to_string());
            let source_data_version_pos = source_version.position();

            for target_version in target_versions.absolute_versions().iter() {
                let target_data_version = match target_version.id() {
                    Some(id) => Ok(id.to_string()),
                    _ => Err(ExecutionPlannerError::DependencyWithoutTargetVersion),
                }?;

                let execution_requirement = DsExecutionRequirement::builder()
                    .id(id())
                    .transaction_id(transaction_id.to_string())
                    .execution_plan_id(execution_plan_id.as_str())
                    .execution_plan_triggered_on(*trigger_time)
                    .target_collection_id(requirement.target().collection())
                    .target_dataset_id(requirement.target().dataset())
                    .target_function_id(target_version.function_id().to_string())
                    .target_data_version(target_data_version)
                    .target_existing_dependency_count(target_dependency_count)
                    .dependency_collection_id(Some(requirement.source().collection().to_string()))
                    .dependency_dataset_id(Some(requirement.source().dataset().to_string()))
                    .dependency_function_id(source_function_id.clone())
                    .dependency_table_id(source_table_id.clone())
                    .dependency_pos(source_pos)
                    .dependency_data_version(source_data_version.clone())
                    .dependency_formal_data_version(source_formal_data_version.to_string())
                    .dependency_data_version_pos(source_data_version_pos)
                    .build()
                    .unwrap();
                execution_requirements.push(execution_requirement);
            }
        }
    }

    Ok(execution_requirements)
}

#[cfg(test)]
mod tests {}
