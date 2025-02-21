//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::list_locked_worker_messages::list_locked_worker_messages;
use std::marker::PhantomData;
use std::sync::Arc;
use td_common::error::TdError;
use td_common::server::{SupervisorMessage, WorkerMessageQueue};
use td_execution::parameters::FunctionInput;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::SrvCtxProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ListCreatedMessagesService<Q> {
    provider: ServiceProvider<(), Vec<SupervisorMessage<FunctionInput>>, TdError>,
    phantom: PhantomData<Q>,
}

impl<Q> ListCreatedMessagesService<Q>
where
    Q: WorkerMessageQueue,
{
    /// Creates a new instance of [`ListCreatedMessagesService`].
    pub fn new(message_queue: Arc<Q>) -> Self {
        Self {
            provider: Self::provider(message_queue.clone()),
            phantom: PhantomData,
        }
    }

    p! {
        provider(message_queue: Arc<Q>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(message_queue),
                from_fn(list_locked_worker_messages::<Q>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<(), Vec<SupervisorMessage<FunctionInput>>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::datasets::service::execution::schedule::tests::MockWorkerMessageQueue;
    use crate::logic::datasets::service::execution::schedule::tests::{
        mock_supervisor_message, mock_supervisor_message_payload, State, StatefulMessage,
    };
    use td_common::server::SupervisorMessagePayload;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_messages_service() {
        use td_tower::metadata::{type_of_val, Metadata};

        let message_queue = Arc::new(MockWorkerMessageQueue::new(vec![]));
        let provider = ListCreatedMessagesService::provider(message_queue);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<(), Vec<SupervisorMessage<FunctionInput>>>(&[type_of_val(
            &list_locked_worker_messages::<MockWorkerMessageQueue>,
        )]);
    }

    #[tokio::test]
    async fn test_list_messages_multiple_messages() {
        let messages = vec![
            StatefulMessage::new(
                mock_supervisor_message("id1", mock_supervisor_message_payload("message1")),
                State::Locked,
            ),
            StatefulMessage::new(
                mock_supervisor_message("id2", mock_supervisor_message_payload("message1")),
                State::Locked,
            ),
            StatefulMessage::new(
                mock_supervisor_message("id3", mock_supervisor_message_payload("message1")),
                State::Locked,
            ),
        ];
        let message_queue = Arc::new(MockWorkerMessageQueue::new(messages.clone()));
        let provider = ListCreatedMessagesService::new(message_queue);

        let service = provider.service().await;
        let result: Vec<SupervisorMessage<FunctionInput>> = service.raw_oneshot(()).await.unwrap();

        assert_eq!(result.len(), messages.len());
        for res in result.iter() {
            assert!(res.id().eq("id1") || res.id().eq("id2") || res.id().eq("id3"));

            // Get first one as all have the same content
            let msg = messages.first().unwrap().message();
            assert_eq!(res.work(), msg.work());
            assert_eq!(res.file(), msg.file());
            let res_payload = match res.payload() {
                SupervisorMessagePayload::SupervisorRequestMessagePayload(res) => res,
                _ => panic!("Expected FunctionInput"),
            };
            let msg_payload = match msg.payload() {
                SupervisorMessagePayload::SupervisorRequestMessagePayload(res) => res,
                _ => panic!("Expected FunctionInput"),
            };
            assert_eq!(res_payload.class(), msg_payload.class());
            assert_eq!(res_payload.worker(), msg_payload.worker());
            assert_eq!(res_payload.action(), msg_payload.action());
            assert_eq!(res_payload.arguments(), msg_payload.arguments());
        }
    }

    #[tokio::test]
    async fn test_list_message_empty() {
        let message_queue = Arc::new(MockWorkerMessageQueue::new(vec![]));
        let provider = ListCreatedMessagesService::new(message_queue);

        let service = provider.service().await;
        let result: Vec<SupervisorMessage<FunctionInput>> = service.raw_oneshot(()).await.unwrap();

        assert!(result.is_empty());
    }
}
