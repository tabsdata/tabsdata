//
//   Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::service::execution::schedule::commit_message::CommitMessagesService;
use crate::logic::datasets::service::execution::schedule::create_message::CreateMessageService;
use crate::logic::datasets::service::execution::schedule::list_created_messages::ListCreatedMessagesService;
use crate::logic::datasets::service::execution::schedule::poll_datasets::PollDatasetsService;
use std::net::SocketAddr;
use std::sync::Arc;
use td_common::server::{SupervisorMessage, WorkerMessageQueue};
use td_database::sql::DbPool;
use td_error::TdError;
use td_execution::parameters::FunctionInput;
use td_objects::datasets::dao::DsReadyToExecute;
use td_storage::Storage;
use td_tower::service_provider::TdBoxService;

pub mod commit_message;
pub mod create_message;
pub mod list_created_messages;
pub mod poll_datasets;

pub struct ScheduleServices<Q> {
    poll_datasets_provider: PollDatasetsService,
    create_message_provider: CreateMessageService<Q>,
    list_created_messages_provider: ListCreatedMessagesService<Q>,
    commit_message_provider: CommitMessagesService<Q>,
}

impl<T> ScheduleServices<T>
where
    T: WorkerMessageQueue,
{
    pub fn new(
        db: DbPool,
        storage: Arc<Storage>,
        message_queue: Arc<T>,
        server_url: Arc<SocketAddr>,
    ) -> Self {
        Self {
            poll_datasets_provider: PollDatasetsService::new(db.clone()),
            create_message_provider: CreateMessageService::new(
                db.clone(),
                storage.clone(),
                message_queue.clone(),
                server_url,
            ),
            list_created_messages_provider: ListCreatedMessagesService::new(message_queue.clone()),
            commit_message_provider: CommitMessagesService::new(db.clone(), message_queue.clone()),
        }
    }

    pub async fn poll(&self) -> TdBoxService<(), Vec<DsReadyToExecute>, TdError> {
        self.poll_datasets_provider.service().await
    }

    pub async fn create(&self) -> TdBoxService<DsReadyToExecute, (), TdError> {
        self.create_message_provider.service().await
    }

    pub async fn list(&self) -> TdBoxService<(), Vec<SupervisorMessage<FunctionInput>>, TdError> {
        self.list_created_messages_provider.service().await
    }

    pub async fn commit(&self) -> TdBoxService<SupervisorMessage<FunctionInput>, (), TdError> {
        self.commit_message_provider.service().await
    }
}

#[cfg(test)]
pub mod tests {
    use async_trait::async_trait;
    use http::Method;
    use serde::de::DeserializeOwned;
    use serde::Serialize;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use td_common::id::Id;
    use td_common::server::{
        Callback, HttpCallbackBuilder, MessageAction, QueueError, RequestMessagePayload,
        SupervisorMessage, SupervisorMessagePayload, WorkerClass, WorkerMessageQueue,
    };
    use td_common::uri::TdUri;
    use td_execution::parameters::FunctionInput;
    use tokio::sync::Mutex;
    use url::Url;

    pub fn td_uri(
        collection_id: &Id,
        dataset_id: &Id,
        table: Option<&str>,
        version: Option<&str>,
    ) -> TdUri {
        let version = version.map(|v| v.to_string());
        let version = version.as_deref();
        TdUri::new(
            &collection_id.to_string(),
            &dataset_id.to_string(),
            table,
            version,
        )
        .unwrap()
    }

    #[derive(Clone, Debug)]
    pub enum State {
        Locked,
        Commit,
        Rollback,
    }

    #[derive(Clone, Debug)]
    pub struct StatefulMessage(SupervisorMessage<FunctionInput>, State);

    impl StatefulMessage {
        pub fn new<T: Clone + Serialize>(message: SupervisorMessage<T>, state: State) -> Self {
            // With this we convert the messages to the concrete type
            let message = serde_yaml::from_value::<SupervisorMessage<FunctionInput>>(
                serde_yaml::to_value(&message).unwrap(),
            )
            .unwrap();
            Self(message, state)
        }

        pub fn message(&self) -> &SupervisorMessage<FunctionInput> {
            &self.0
        }

        pub fn state(&self) -> &State {
            &self.1
        }
    }

    pub struct MockWorkerMessageQueue {
        messages: Mutex<HashMap<String, StatefulMessage>>,
    }

    impl MockWorkerMessageQueue {
        pub fn new(messages: Vec<StatefulMessage>) -> Self {
            let messages = messages.iter().fold(HashMap::new(), |mut acc, msg| {
                acc.insert(msg.message().id().clone(), msg.clone());
                acc
            });
            Self {
                messages: Mutex::new(messages),
            }
        }
    }

    #[async_trait]
    impl WorkerMessageQueue for MockWorkerMessageQueue {
        async fn put<T: Serialize + Clone + Send + Sync>(
            &self,
            id: String,
            payload: RequestMessagePayload<T>,
        ) -> Result<SupervisorMessage<T>, QueueError> {
            if self.messages.lock().await.contains_key(&id) {
                return Err(QueueError::MessageAlreadyExisting { id: id.clone() });
            }
            let message = mock_supervisor_message(&id, payload);
            self.messages
                .lock()
                .await
                .insert(id, StatefulMessage::new(message.clone(), State::Locked));
            Ok(message)
        }

        async fn commit(&self, id: String) -> Result<(), QueueError> {
            if !self.messages.lock().await.contains_key(&id) {
                return Err(QueueError::MessageNonExisting { id: id.clone() });
            }
            self.messages.lock().await.get_mut(&id).map(|msg| {
                msg.1 = State::Commit;
                Some(())
            });
            Ok(())
        }

        async fn rollback(&self, id: String) -> Result<(), QueueError> {
            if !self.messages.lock().await.contains_key(&id) {
                return Err(QueueError::MessageNonExisting { id: id.clone() });
            }
            self.messages.lock().await.get_mut(&id).map(|msg| {
                msg.1 = State::Rollback;
                Some(())
            });
            Ok(())
        }

        async fn locked_messages<T: DeserializeOwned + Clone + Send + Sync>(
            &self,
        ) -> Vec<SupervisorMessage<T>> {
            self.messages
                .lock()
                .await
                .iter()
                .fold(vec![], |mut acc, (_, msg)| {
                    if let State::Locked = msg.state() {
                        let (msg, payload) =
                            if let SupervisorMessagePayload::SupervisorRequestMessagePayload(
                                payload,
                            ) = msg.message().payload()
                            {
                                (msg.message(), payload)
                            } else {
                                panic!("Invalid message payload: {:?}", msg.message())
                            };

                        let msg = SupervisorMessage::new(
                            msg.id().clone(),
                            msg.work().clone(),
                            msg.file().clone(),
                            SupervisorMessagePayload::SupervisorRequestMessagePayload(
                                RequestMessagePayload::builder()
                                    .class(payload.class().clone())
                                    .worker(payload.worker().clone())
                                    .action(payload.action().clone())
                                    .arguments(payload.arguments().clone())
                                    .callback(payload.callback().clone())
                                    .context(
                                        // With this we convert the messages to the generic type T
                                        serde_yaml::from_value::<T>(
                                            serde_yaml::to_value(payload.context()).unwrap(),
                                        )
                                        .unwrap(),
                                    )
                                    .build()
                                    .unwrap(),
                            ),
                        );
                        acc.push(msg);
                    }
                    acc
                })
        }
    }

    impl MockWorkerMessageQueue {
        pub async fn commited_messages(&self) -> Vec<SupervisorMessage<FunctionInput>> {
            self.messages
                .lock()
                .await
                .iter()
                .filter(|(_, msg)| matches!(msg.state(), State::Commit))
                .map(|(_, msg)| msg.message())
                .cloned()
                .collect()
        }

        pub async fn rollback_messages(&self) -> Vec<SupervisorMessage<FunctionInput>> {
            self.messages
                .lock()
                .await
                .iter()
                .filter(|(_, msg)| matches!(msg.state(), State::Rollback))
                .map(|(_, msg)| msg.message())
                .cloned()
                .collect()
        }
    }

    pub fn mock_supervisor_message<T: Clone>(
        id: &str,
        payload: RequestMessagePayload<T>,
    ) -> SupervisorMessage<T> {
        SupervisorMessage::new(
            id.to_string(),
            "dataset".to_string(),
            PathBuf::from("file"),
            SupervisorMessagePayload::SupervisorRequestMessagePayload(payload),
        )
    }

    pub fn mock_supervisor_message_payload(
        payload: impl ToString,
    ) -> RequestMessagePayload<FunctionInput> {
        RequestMessagePayload::builder()
            .class(WorkerClass::EPHEMERAL)
            .worker("worker".to_string())
            .action(MessageAction::Start)
            .arguments(vec!["arg1".to_string()])
            .callback(Some(Callback::Http(
                HttpCallbackBuilder::default()
                    .url(Url::parse("http://localhost").unwrap())
                    .method(Method::GET)
                    .headers(HashMap::new())
                    .body(false)
                    .build()
                    .unwrap(),
            )))
            .context(Some(FunctionInput::V0(payload.to_string())))
            .build()
            .unwrap()
    }
}
