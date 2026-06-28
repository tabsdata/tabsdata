//
// Copyright 2026 Tabs Data Inc.
//

use td_bridge::messaging::{CronStartRequest, FunctionStartRequest, StageTriggerStartRequest};
use td_common::worker::Callback;

use crate::base::labels::{
    LABEL_COLLECTION, LABEL_COLLECTION_ID, LABEL_FUNCTION, LABEL_FUNCTION_ID,
    LABEL_FUNCTION_VERSION_ID, LABEL_PROJECT_ID,
};

pub(crate) trait WorkerStartRequest {
    fn worker(&self) -> &str;
    fn worker_id(&self) -> String;
    fn decorator(&self) -> Option<&str>;
    fn resolver(&self) -> Option<&Callback>;
    fn trigger(&self) -> Option<&Callback>;
    fn responder(&self) -> Option<&Callback>;

    fn project_id(&self) -> String;
    fn collection_id(&self) -> String;
    fn collection(&self) -> String;
    fn function(&self) -> String;

    fn custom_labels(&self) -> Vec<(String, String)>;

    fn labels(&self) -> Vec<(String, String)> {
        let mut labels = vec![
            (LABEL_PROJECT_ID.to_owned(), self.project_id()),
            (LABEL_COLLECTION_ID.to_owned(), self.collection_id()),
            (LABEL_COLLECTION.to_owned(), self.collection()),
            (LABEL_FUNCTION.to_owned(), self.function()),
        ];
        labels.extend(self.custom_labels());
        labels
    }
}

impl WorkerStartRequest for FunctionStartRequest {
    fn worker(&self) -> &str {
        self.worker.as_ref()
    }

    fn worker_id(&self) -> String {
        self.worker_id.to_string()
    }

    fn decorator(&self) -> Option<&str> {
        Some(self.decorator.as_ref())
    }

    fn resolver(&self) -> Option<&Callback> {
        Some(&self.resolver)
    }

    fn trigger(&self) -> Option<&Callback> {
        None
    }

    fn responder(&self) -> Option<&Callback> {
        Some(&self.responder)
    }

    fn project_id(&self) -> String {
        self.project_id.to_string()
    }

    fn collection_id(&self) -> String {
        self.collection_id.to_string()
    }

    fn collection(&self) -> String {
        self.collection.to_string()
    }

    fn function(&self) -> String {
        self.function.to_string()
    }

    fn custom_labels(&self) -> Vec<(String, String)> {
        vec![(LABEL_FUNCTION_ID.to_owned(), self.function_id.to_string())]
    }
}

impl WorkerStartRequest for StageTriggerStartRequest {
    fn worker(&self) -> &str {
        self.worker.as_ref()
    }

    fn worker_id(&self) -> String {
        self.worker_id.to_string()
    }

    fn decorator(&self) -> Option<&str> {
        Some(self.decorator.as_ref())
    }

    fn resolver(&self) -> Option<&Callback> {
        Some(&self.resolver)
    }

    fn trigger(&self) -> Option<&Callback> {
        Some(&self.trigger)
    }

    fn responder(&self) -> Option<&Callback> {
        Some(&self.responder)
    }

    fn project_id(&self) -> String {
        self.project_id.to_string()
    }

    fn collection_id(&self) -> String {
        self.collection_id.to_string()
    }

    fn collection(&self) -> String {
        self.collection.to_string()
    }

    fn function(&self) -> String {
        self.function.to_string()
    }

    fn custom_labels(&self) -> Vec<(String, String)> {
        vec![(
            LABEL_FUNCTION_VERSION_ID.to_owned(),
            self.function_version_id.to_string(),
        )]
    }
}

impl WorkerStartRequest for CronStartRequest {
    fn worker(&self) -> &str {
        self.worker.as_ref()
    }

    fn worker_id(&self) -> String {
        self.worker_id.to_string()
    }

    fn decorator(&self) -> Option<&str> {
        None
    }

    fn resolver(&self) -> Option<&Callback> {
        None
    }

    fn trigger(&self) -> Option<&Callback> {
        Some(&self.trigger)
    }

    fn responder(&self) -> Option<&Callback> {
        Some(&self.responder)
    }

    fn project_id(&self) -> String {
        self.project_id.to_string()
    }

    fn collection_id(&self) -> String {
        self.collection_id.to_string()
    }

    fn collection(&self) -> String {
        self.collection.to_string()
    }

    fn function(&self) -> String {
        self.function.to_string()
    }

    fn custom_labels(&self) -> Vec<(String, String)> {
        vec![(
            LABEL_FUNCTION_VERSION_ID.to_owned(),
            self.function_version_id.to_string(),
        )]
    }
}
