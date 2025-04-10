//
// Copyright 2025 Tabs Data Inc.
//

use crate::logic::datasets::layer::deserialize_execution_plan::deserialize_execution_plan;
use crate::logic::datasets::layer::execution_graph_with_names::execution_graph_with_names;
use crate::logic::datasets::layer::execution_plan_to_api::execution_plan_to_api;
use crate::logic::datasets::layer::execution_plan_with_names::execution_plan_with_names;
use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
use crate::logic::datasets::layer::select_execution_plan::select_execution_plan;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::datasets::dto::ExecutionPlanRead;
use td_objects::dlo::ExecutionPlanId;
use td_objects::rest_urls::ExecutionPlanIdParam;
use td_objects::tower_service::extractor::extract_name;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};
use td_transaction::TransactionBy;

pub struct ReadPlanService {
    provider: ServiceProvider<ReadRequest<ExecutionPlanIdParam>, ExecutionPlanRead, TdError>,
}

impl ReadPlanService {
    /// Creates a new instance of [`ReadPlanService`].
    pub fn new(db: DbPool, transaction_by: Arc<TransactionBy>) -> Self {
        Self {
            provider: Self::provider(db, transaction_by),
        }
    }

    p! {
        provider(db: DbPool, transaction_by: Arc<TransactionBy>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(transaction_by),
                ConnectionProvider::new(db),
                from_fn(read_dataset_authorize),
                from_fn(extract_name::<ReadRequest<ExecutionPlanIdParam>, ExecutionPlanIdParam, ExecutionPlanId>),
                from_fn(select_execution_plan),
                from_fn(deserialize_execution_plan),
                from_fn(execution_plan_with_names),
                from_fn(execution_graph_with_names),
                from_fn(execution_plan_to_api),
            ))
        }
    }

    /// Returns a service that read the execution plan.
    pub async fn service(
        &self,
    ) -> TdBoxService<ReadRequest<ExecutionPlanIdParam>, ExecutionPlanRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_common::id;
    use td_common::uri::{Version, Versions};
    use td_execution::execution_planner::{ExecutionPlan, ExecutionTemplate};
    use td_execution::function::{
        AbsoluteVersion, AbsoluteVersions, FunctionNode, GraphEdge, RelativeVersions,
        ResolvedVersion,
    };
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_execution_plan::seed_execution_plan_serialized;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_read_plan() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ReadPlanService::provider(db, Arc::new(TransactionBy::default()));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ReadRequest<ExecutionPlanIdParam>, ExecutionPlanRead>(&[
            type_of_val(&read_dataset_authorize),
            type_of_val(
                &extract_name::<
                    ReadRequest<ExecutionPlanIdParam>,
                    ExecutionPlanIdParam,
                    ExecutionPlanId,
                >,
            ),
            type_of_val(&select_execution_plan),
            type_of_val(&deserialize_execution_plan),
            type_of_val(&execution_plan_with_names),
            type_of_val(&execution_graph_with_names),
            type_of_val(&execution_plan_to_api),
        ]);
    }

    #[tokio::test]
    async fn test_execution_plan_read_service() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        // Create the mock datasets, without relations.
        let (d0, f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t01", "t02"],
            &[],
            &[],
            "hash",
        )
        .await;

        let (d1, _f1) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[],
            &[],
            "hash",
        )
        .await;

        let (d2, _f2) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d2",
            &["t2"],
            &[],
            &[],
            "hash",
        )
        .await;

        // Create and serialize plan.
        let dataset_0 = FunctionNode::new(&collection_id.to_string(), &d0.to_string());
        let dataset_1 = FunctionNode::new(&collection_id.to_string(), &d1.to_string());
        let dataset_2 = FunctionNode::new(&collection_id.to_string(), &d2.to_string());
        let version_0_1 = RelativeVersions::Plan(GraphEdge::from_table(
            Versions::Single(Version::Head(0)),
            "t01".to_string(),
            0,
        ));
        let version_0_2 = RelativeVersions::Plan(GraphEdge::from_table(
            Versions::Single(Version::Head(0)),
            "t02".to_string(),
            0,
        ));
        let version_1 = RelativeVersions::Plan(GraphEdge::from_table(
            Versions::Single(Version::Head(0)),
            "t1".to_string(),
            0,
        ));

        let mut template = ExecutionTemplate::with_trigger(&dataset_0);
        template.add_trigger(&dataset_1);
        template.add_trigger(&dataset_2);
        template.add_dependency(&dataset_1, &dataset_0, version_0_1.clone());
        template.add_dependency(&dataset_1, &dataset_0, version_0_2.clone());
        template.add_dependency(&dataset_2, &dataset_1, version_1.clone());
        template.add_trigger_requirement(&dataset_1, &dataset_0);
        template.add_trigger_requirement(&dataset_2, &dataset_1);

        let plan: ExecutionPlan = template
            .versioned(|_d, v| async move {
                let resolved = ResolvedVersion::new(
                    AbsoluteVersions::new(vec![AbsoluteVersion::new(
                        id::id(),
                        GraphEdge::Table {
                            versions: Some(id::id()),
                            table: id::id(),
                            pos: 0,
                        },
                        0,
                    )]),
                    v.clone(),
                );
                Ok::<_, TdError>(resolved)
            })
            .await
            .unwrap();

        let serialized = serde_json::to_string(&plan).unwrap();

        // Insert serialized plan, associated to the created dataset.
        let execution_plan_id = seed_execution_plan_serialized(
            &db,
            "test",
            &collection_id,
            &d0,
            &f0,
            Some(user_id.to_string()),
            &serialized,
        )
        .await;

        let service = ReadPlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await;

        let request =
            RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
                .read(ExecutionPlanIdParam::new(execution_plan_id.to_string()));
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.name(), "test");
        assert_eq!(response.triggered_datasets_with_ids().len(), 3);
        let _ = &[
            format!("{}/{}", collection_id, d0),
            format!("{}/{}", collection_id, d1),
            format!("{}/{}", collection_id, d2),
        ]
        .iter()
        .for_each(|id| {
            assert!(response
                .triggered_datasets_with_ids()
                .contains(&id.to_string()))
        });

        assert_eq!(response.triggered_datasets_with_names().len(), 3);
        let _ = &["ds0/d0", "ds0/d1", "ds0/d2"].iter().for_each(|name| {
            assert!(response
                .triggered_datasets_with_names()
                .contains(&name.to_string()))
        });
        assert!(response.dot().contains("digraph"));
    }
}
