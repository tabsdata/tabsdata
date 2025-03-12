//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::build_ds_worker_message::build_ds_worker_message;
use crate::logic::datasets::layer::check_data_version_run_requested_status::check_data_version_run_requested_status;
use crate::logic::datasets::layer::commit_worker_message::commit_worker_message;
use crate::logic::datasets::layer::insert_ds_worker_message::insert_ds_worker_message;
use crate::logic::datasets::layer::rollback_worker_message::rollback_worker_message;
use crate::logic::datasets::layer::select_data_version::select_data_version;
use crate::logic::datasets::layer::set_data_version_state;
use crate::logic::datasets::layer::update_data_version_status::update_data_version_status;
use crate::logic::datasets::layer::worker_message_to_data_version_id::worker_message_to_data_version_id;
use std::marker::PhantomData;
use std::sync::Arc;
use td_common::server::{SupervisorMessage, WorkerMessageQueue};
use td_database::sql::DbPool;
use td_error::TdError;
use td_execution::parameters::FunctionInput;
use td_objects::datasets::dao::DsDataVersion;
use td_objects::tower_service::extractor::{extract_message_id, to_vec};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{conditional, Do, Else, If, SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service, service_provider};

pub struct CommitMessagesService<Q> {
    provider: ServiceProvider<SupervisorMessage<FunctionInput>, (), TdError>,
    phantom: PhantomData<Q>,
}

impl<Q> CommitMessagesService<Q>
where
    Q: WorkerMessageQueue,
{
    /// Creates a new instance of [`CommitMessagesService`].
    pub fn new(db: DbPool, message_queue: Arc<Q>) -> Self {
        Self {
            provider: Self::provider(db.clone(), message_queue.clone()),
            phantom: PhantomData,
        }
    }

    p! {
        provider(db: DbPool, message_queue: Arc<Q>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(message_queue),
                TransactionProvider::new(db.clone()),
                from_fn(extract_message_id::<SupervisorMessage<FunctionInput>>),
                from_fn(worker_message_to_data_version_id),
                conditional(
                    If(service!(layers!(
                        from_fn(check_data_version_run_requested_status),
                    ))),
                    Do(service!(layers!(
                        from_fn(select_data_version),
                        conditional(
                            If(service!(layers!(
                                from_fn(build_ds_worker_message),
                                from_fn(insert_ds_worker_message),
                                from_fn(commit_worker_message::<Q>),
                            ))),
                            Do(service!()),
                            Else(service!(layers!(
                                from_fn(set_data_version_state::scheduled),
                                from_fn(to_vec::<DsDataVersion>),
                                from_fn(update_data_version_status),
                                from_fn(rollback_worker_message::<Q>),
                            ))),
                        ),
                    ))),
                    Else(service!(layers!(
                        from_fn(rollback_worker_message::<Q>),
                    ))),
                )
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<SupervisorMessage<FunctionInput>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::datasets::service::execution::schedule::tests::{
        mock_supervisor_message, mock_supervisor_message_payload, MockWorkerMessageQueue, State,
        StatefulMessage,
    };
    use td_common::execution_status::{DataVersionStatus, ExecutionPlanStatus, TransactionStatus};
    use td_objects::datasets::dao::{
        DsExecutionPlanWithNames, DsExecutionRequirement, DsTransaction, DsWorkerMessage,
    };
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_data_version::seed_data_version;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_execution_plan::seed_execution_plan;
    use td_objects::test_utils::seed_execution_requirement::seed_execution_requirement;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::test_utils::seed_user::seed_user;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_commit_messages_service() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let message_queue = Arc::new(MockWorkerMessageQueue::new(vec![]));
        let provider = CommitMessagesService::provider(db, message_queue);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<SupervisorMessage<FunctionInput>, ()>(&[
            type_of_val(&extract_message_id::<SupervisorMessage<FunctionInput>>),
            type_of_val(&worker_message_to_data_version_id),
            // if
            type_of_val(&check_data_version_run_requested_status),
            // do
            type_of_val(&select_data_version),
            // if
            type_of_val(&build_ds_worker_message),
            type_of_val(&insert_ds_worker_message),
            type_of_val(&commit_worker_message::<MockWorkerMessageQueue>),
            // else
            type_of_val(&set_data_version_state::scheduled),
            type_of_val(&to_vec::<DsDataVersion>),
            type_of_val(&update_data_version_status),
            type_of_val(&rollback_worker_message::<MockWorkerMessageQueue>),
            // else
            type_of_val(&rollback_worker_message::<MockWorkerMessageQueue>),
            //
        ]);
    }

    #[tokio::test]
    async fn test_commit_message() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
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

        let execution_plan_id =
            seed_execution_plan(&db, "exec_plan_0", &collection_id, &d0, &f0, None).await;
        let transaction_id =
            seed_transaction(&db, &execution_plan_id, None, TransactionStatus::Scheduled).await;

        let data_version = seed_data_version(
            &db,
            &collection_id,
            &d0,
            &f0,
            &transaction_id,
            &execution_plan_id,
            "M",
            "Rr",
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

        let messages = vec![StatefulMessage::new(
            mock_supervisor_message("id1", mock_supervisor_message_payload(data_version)),
            State::Locked,
        )];
        let message_queue = Arc::new(MockWorkerMessageQueue::new(messages.clone()));

        // Get the first message from the list, which is locked
        let supervisor_messages = messages
            .iter()
            .map(|m| m.message().clone())
            .collect::<Vec<_>>();
        let supervisor_message = supervisor_messages.first().unwrap();

        // Run service, which should move to commited state
        let provider = CommitMessagesService::new(db.clone(), message_queue.clone());
        let service = provider.service().await;
        service
            .raw_oneshot(supervisor_message.clone())
            .await
            .unwrap();

        // Assert no locked messages
        let locked_messages: Vec<SupervisorMessage<FunctionInput>> =
            message_queue.locked_messages().await;
        assert!(locked_messages.is_empty());

        // Assert one commit message
        let commited_messages = message_queue.commited_messages().await;
        assert_eq!(commited_messages.len(), 1);
        let commited_message = commited_messages.first().unwrap();
        assert_eq!(*commited_message, *supervisor_message);

        // Assert no rollback messages
        let rollback_messages = message_queue.rollback_messages().await;
        assert_eq!(rollback_messages.len(), 0);

        // Assert db state
        const SELECT_REQUIREMENT: &str = r#"
            SELECT * FROM ds_execution_requirements
            WHERE target_data_version = ?1
        "#;

        let req: DsExecutionRequirement = sqlx::query_as(SELECT_REQUIREMENT)
            .bind(data_version.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(req.target_data_version(), &data_version.to_string());

        const SELECT_EXECUTION_PLAN: &str = r#"
            SELECT * FROM ds_execution_plans_with_names
            WHERE id = (SELECT execution_plan_id FROM ds_data_versions WHERE id = ?1)
        "#;

        let status: DsExecutionPlanWithNames = sqlx::query_as(SELECT_EXECUTION_PLAN)
            .bind(data_version.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &ExecutionPlanStatus::Scheduled);

        const SELECT_TRANSACTION_STATUS: &str = r#"
            SELECT * FROM ds_transactions
            WHERE id = (SELECT transaction_id FROM ds_data_versions WHERE id = ?1)
        "#;

        let status: DsTransaction = sqlx::query_as(SELECT_TRANSACTION_STATUS)
            .bind(data_version.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &TransactionStatus::Scheduled);

        const SELECT_DATA_VERSION_STATUS: &str = r#"
            SELECT * FROM ds_data_versions
            WHERE id = ?1
        "#;

        let status: DsDataVersion = sqlx::query_as(SELECT_DATA_VERSION_STATUS)
            .bind(data_version.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status.status(), &DataVersionStatus::RunRequested);

        const SELECT_MESSAGE: &str = r#"
            SELECT * FROM ds_worker_messages
            WHERE data_version_id = ?1
        "#;

        let msg: DsWorkerMessage = sqlx::query_as(SELECT_MESSAGE)
            .bind(data_version.to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(msg.transaction_id(), &transaction_id.to_string());
        assert_eq!(msg.collection_id(), &collection_id.to_string());
        assert_eq!(msg.execution_plan_id(), &execution_plan_id.to_string());
    }

    #[tokio::test]
    async fn test_rollback_message() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
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

        let execution_plan_id =
            seed_execution_plan(&db, "exec_plan_0", &collection_id, &d0, &f0, None).await;
        let transaction_id =
            seed_transaction(&db, &execution_plan_id, None, TransactionStatus::Scheduled).await;

        // The message doesn't have a valid data_version
        let messages = vec![StatefulMessage::new(
            mock_supervisor_message("id1", mock_supervisor_message_payload("wrong_data_version")),
            State::Locked,
        )];
        let message_queue = Arc::new(MockWorkerMessageQueue::new(messages.clone()));

        // Get the first message from the list, which is locked
        let supervisor_messages = messages
            .iter()
            .map(|m| m.message().clone())
            .collect::<Vec<_>>();
        let supervisor_message = supervisor_messages.first().unwrap();

        // Run service, which should move to rollback state
        let provider = CommitMessagesService::new(db.clone(), message_queue.clone());
        let service = provider.service().await;
        service
            .raw_oneshot(supervisor_message.clone())
            .await
            .unwrap();

        // Assert no locked messages
        let locked_messages: Vec<SupervisorMessage<FunctionInput>> =
            message_queue.locked_messages().await;
        assert!(locked_messages.is_empty());

        // Assert no commit messages
        let commited_messages = message_queue.commited_messages().await;
        assert_eq!(commited_messages.len(), 0);

        // Assert one rollback message
        let rollback_messages = message_queue.rollback_messages().await;
        assert_eq!(rollback_messages.len(), 1);
        let rollback_message = rollback_messages.first().unwrap();
        assert_eq!(*rollback_message, *supervisor_message);

        // Assert db state
        const SELECT_MESSAGE: &str = r#"
            SELECT * FROM ds_worker_messages
            WHERE transaction_id = ?1
        "#;

        let msg: Option<DsWorkerMessage> = sqlx::query_as(SELECT_MESSAGE)
            .bind(transaction_id.to_string())
            .fetch_optional(&db)
            .await
            .unwrap();
        assert!(msg.is_none());
    }
}
