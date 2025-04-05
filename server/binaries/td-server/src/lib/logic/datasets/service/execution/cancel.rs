//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::event_time::event_time;
use crate::logic::datasets::layer::recover_request_to_state::{
    cancel_state, recover_request_to_state,
};
use crate::logic::datasets::layer::select_transaction::select_transaction;
use crate::logic::datasets::layer::select_transaction_versions::select_transaction_versions;
use crate::logic::datasets::layer::update_data_version_status::update_data_version_status;
use crate::logic::datasets::layer::update_dependants_status::update_dependants_status;
use crate::logic::datasets::layer::update_publish_status::update_publish_status;
use crate::logic::datasets::layer::update_transaction_status::update_transaction_status;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::UpdateRequest;
use td_objects::dlo::TransactionId;
use td_objects::tower_service::extractor::extract_req_name;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct CancelExecutionService {
    provider: ServiceProvider<UpdateRequest<TransactionId, ()>, (), TdError>,
}

impl CancelExecutionService {
    /// Creates a new instance of [`CancelExecutionService`].
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: Self::provider(db.clone()),
        }
    }

    p! {
        provider(db: DbPool) -> TdError {
            service_provider!(layers!(
                from_fn(event_time),
                from_fn(cancel_state),
                from_fn(recover_request_to_state),
                from_fn(extract_req_name::<UpdateRequest<TransactionId, ()>, TransactionId>),
                TransactionProvider::new(db),
                from_fn(select_transaction),
                from_fn(select_transaction_versions),
                from_fn(update_data_version_status),
                from_fn(update_transaction_status),
                from_fn(update_dependants_status),
                from_fn(update_publish_status),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<UpdateRequest<TransactionId, ()>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::datasets::service::execution::schedule::tests::td_uri;
    use td_common::execution_status::{DataVersionStatus, ExecutionPlanStatus, TransactionStatus};
    use td_objects::crudl::RequestContext;
    use td_objects::datasets::dao::{DsDataVersion, DsExecutionPlanWithNames, DsTransaction};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_data_version::seed_data_version;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_execution_plan::seed_execution_plan;
    use td_objects::test_utils::seed_execution_requirement::seed_execution_requirement;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_cancel_service() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = CancelExecutionService::provider(db.clone());
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<UpdateRequest<TransactionId, ()>, ()>(&[
            type_of_val(&event_time),
            type_of_val(&cancel_state),
            type_of_val(&recover_request_to_state),
            type_of_val(&extract_req_name::<UpdateRequest<TransactionId, ()>, TransactionId>),
            type_of_val(&select_transaction),
            type_of_val(&select_transaction_versions),
            type_of_val(&update_data_version_status),
            type_of_val(&update_transaction_status),
            type_of_val(&update_dependants_status),
            type_of_val(&update_publish_status),
        ]);
    }

    #[tokio::test]
    async fn test_single() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;

        let execution_plan_id =
            seed_execution_plan(&db, "exec_plan_0", &collection_id, &d0, &f0, None).await;
        let transaction_id =
            seed_transaction(&db, &execution_plan_id, None, TransactionStatus::Failed).await;

        let data_version = seed_data_version(
            &db,
            &collection_id,
            &d0,
            &f0,
            &transaction_id,
            &execution_plan_id,
            "M",
            "F",
        )
        .await;
        let _er_id = seed_execution_requirement(
            &db,
            &transaction_id,
            &execution_plan_id,
            &collection_id,
            &d0,
            &f0,
            &data_version,
            0,
            None,
            None,
            None,
            None,
            None,
        )
        .await;

        let provider = CancelExecutionService::provider(db.clone());
        let service = provider.make().await;

        let request: UpdateRequest<TransactionId, ()> =
            RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
                .update(TransactionId::new(transaction_id.to_string()), ());
        let _: () = service.raw_oneshot(request).await.unwrap();

        // Assert db state
        const SELECT_EXECUTION_PLAN: &str = r#"
            SELECT * FROM ds_execution_plans_with_names
            WHERE id = ?1
        "#;

        let status: DsExecutionPlanWithNames = sqlx::query_as(SELECT_EXECUTION_PLAN)
            .bind(execution_plan_id.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &ExecutionPlanStatus::Incomplete);

        const SELECT_TRANSACTION_STATUS: &str = r#"
            SELECT * FROM ds_transactions
            WHERE id = ?1
        "#;

        let status: DsTransaction = sqlx::query_as(SELECT_TRANSACTION_STATUS)
            .bind(transaction_id.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &TransactionStatus::Canceled);

        const SELECT_DATA_VERSION_STATUS: &str = r#"
            SELECT * FROM ds_data_versions
            WHERE id = ?1
        "#;

        let status: DsDataVersion = sqlx::query_as(SELECT_DATA_VERSION_STATUS)
            .bind(data_version.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &DataVersionStatus::Canceled);
    }

    #[tokio::test]
    async fn test_multiple() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;

        let (d1, f1) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[td_uri(&collection_id, &d0, Some("t0"), Some("HEAD"))],
            &[td_uri(&collection_id, &d0, None, None)],
            "hash",
        )
        .await;

        let execution_plan_id_1 =
            seed_execution_plan(&db, "exec_plan_1", &collection_id, &d0, &f0, None).await;
        let transaction_id_1 =
            seed_transaction(&db, &execution_plan_id_1, None, TransactionStatus::Failed).await;

        let data_version_0 = seed_data_version(
            &db,
            &collection_id,
            &d0,
            &f0,
            &transaction_id_1,
            &execution_plan_id_1,
            "M",
            "F",
        )
        .await;
        let _er_id = seed_execution_requirement(
            &db,
            &transaction_id_1,
            &execution_plan_id_1,
            &collection_id,
            &d0,
            &f0,
            &data_version_0,
            0,
            None,
            None,
            None,
            None,
            None,
        )
        .await;

        let data_version_1 = seed_data_version(
            &db,
            &collection_id,
            &d1,
            &f1,
            &transaction_id_1,
            &execution_plan_id_1,
            "M",
            "F",
        )
        .await;
        let _er_id = seed_execution_requirement(
            &db,
            &transaction_id_1,
            &execution_plan_id_1,
            &collection_id,
            &d0,
            &f0,
            &data_version_1,
            0,
            None,
            None,
            None,
            None,
            None,
        )
        .await;

        let (d2, f2) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d2",
            &["t2"],
            &[td_uri(&collection_id, &d0, Some("t0"), Some("HEAD"))],
            &[],
            "hash",
        )
        .await;

        let execution_plan_id_2 =
            seed_execution_plan(&db, "exec_plan_2", &collection_id, &d2, &f2, None).await;
        let transaction_id_2 =
            seed_transaction(&db, &execution_plan_id_2, None, TransactionStatus::OnHold).await;

        let data_version_2 = seed_data_version(
            &db,
            &collection_id,
            &d2,
            &f2,
            &transaction_id_2,
            &execution_plan_id_2,
            "M",
            "H",
        )
        .await;
        let _er_id = seed_execution_requirement(
            &db,
            &transaction_id_2,
            &execution_plan_id_2,
            &collection_id,
            &d2,
            &f2,
            &data_version_2,
            0,
            Some(&collection_id),
            Some(&d0),
            Some(&f0),
            None,
            Some(&data_version_0),
        )
        .await;

        let provider = CancelExecutionService::provider(db.clone());
        let service = provider.make().await;

        let request: UpdateRequest<TransactionId, ()> =
            RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
                .update(TransactionId::new(transaction_id_1.to_string()), ());
        let _: () = service.raw_oneshot(request).await.unwrap();

        // Assert db state
        const SELECT_EXECUTION_PLAN: &str = r#"
            SELECT * FROM ds_execution_plans_with_names
            WHERE id = ?1
        "#;

        let status: DsExecutionPlanWithNames = sqlx::query_as(SELECT_EXECUTION_PLAN)
            .bind(execution_plan_id_1.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &ExecutionPlanStatus::Incomplete);

        let status: DsExecutionPlanWithNames = sqlx::query_as(SELECT_EXECUTION_PLAN)
            .bind(execution_plan_id_2.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &ExecutionPlanStatus::Incomplete);

        const SELECT_TRANSACTION_STATUS: &str = r#"
            SELECT * FROM ds_transactions
            WHERE id = ?1
        "#;

        let status: DsTransaction = sqlx::query_as(SELECT_TRANSACTION_STATUS)
            .bind(transaction_id_1.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &TransactionStatus::Canceled);

        let status: DsTransaction = sqlx::query_as(SELECT_TRANSACTION_STATUS)
            .bind(transaction_id_2.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &TransactionStatus::Canceled);

        const SELECT_DATA_VERSION_STATUS: &str = r#"
            SELECT * FROM ds_data_versions
            WHERE id = ?1
        "#;

        let status: DsDataVersion = sqlx::query_as(SELECT_DATA_VERSION_STATUS)
            .bind(data_version_0.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &DataVersionStatus::Canceled);

        let status: DsDataVersion = sqlx::query_as(SELECT_DATA_VERSION_STATUS)
            .bind(data_version_1.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &DataVersionStatus::Canceled);

        let status: DsDataVersion = sqlx::query_as(SELECT_DATA_VERSION_STATUS)
            .bind(data_version_2.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &DataVersionStatus::Canceled);
    }
}
