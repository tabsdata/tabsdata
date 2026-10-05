//
// Copyright 2026 Tabsdata Inc.
//

use td_bridge::messaging::{CronStartRequest, FunctionStartRequest, StageTriggerStartRequest};
use td_common::worker::Callback;

use crate::base::naming::logical_id_text;

use crate::base::annotations::{
    ANNOTATION_COLLECTION, ANNOTATION_CONNECTION, ANNOTATION_FUNCTION, ANNOTATION_PROJECT,
};
use crate::base::labels::{
    LABEL_COLLECTION_ID, LABEL_CONNECTION_ID, LABEL_FUNCTION_ID, LABEL_FUNCTION_VERSION_ID,
    LABEL_PROJECT_ID, label_value,
};

pub(crate) trait WorkerStartRequest {
    fn worker(&self) -> &str;
    fn worker_id(&self) -> String;
    fn decorator(&self) -> Option<&str>;
    fn resolver(&self) -> Option<&Callback>;
    fn trigger(&self) -> Option<&Callback>;
    fn responder(&self) -> Option<&Callback>;

    fn project_id(&self) -> String;
    fn project(&self) -> Option<String>;
    fn collection_id(&self) -> String;
    fn collection(&self) -> String;
    fn connection_id(&self) -> Option<String>;
    fn connection(&self) -> Option<String>;
    fn function_id(&self) -> String;
    fn function(&self) -> String;

    fn custom_labels(&self) -> Vec<(String, String)> {
        Vec::new()
    }

    fn labels(&self) -> Vec<(String, String)> {
        let mut labels = vec![
            (LABEL_PROJECT_ID.to_owned(), self.project_id()),
            (LABEL_COLLECTION_ID.to_owned(), self.collection_id()),
            (LABEL_FUNCTION_ID.to_owned(), self.function_id()),
        ];
        if let Some(connection_id) = self.connection_id() {
            labels.push((LABEL_CONNECTION_ID.to_owned(), connection_id));
        }
        labels.extend(self.custom_labels());
        labels
            .into_iter()
            .map(|(name, value)| (name, label_value(&value)))
            .filter(|(_, value)| !value.is_empty())
            .collect()
    }

    fn annotations(&self) -> Vec<(String, String)> {
        let mut annotations = vec![
            (ANNOTATION_COLLECTION.to_owned(), self.collection()),
            (ANNOTATION_FUNCTION.to_owned(), self.function()),
        ];
        if let Some(project) = self.project() {
            annotations.push((ANNOTATION_PROJECT.to_owned(), project));
        }
        if let Some(connection) = self.connection() {
            annotations.push((ANNOTATION_CONNECTION.to_owned(), connection));
        }
        annotations
    }
}

impl WorkerStartRequest for FunctionStartRequest {
    fn worker(&self) -> &str {
        self.worker.as_ref()
    }

    fn worker_id(&self) -> String {
        logical_id_text(&self.worker_id)
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

    fn project(&self) -> Option<String> {
        self.project.as_ref().map(ToString::to_string)
    }

    fn collection_id(&self) -> String {
        self.collection_id.to_string()
    }

    fn collection(&self) -> String {
        self.collection.to_string()
    }

    fn connection_id(&self) -> Option<String> {
        self.connection_id.as_ref().map(ToString::to_string)
    }

    fn connection(&self) -> Option<String> {
        self.connection.as_ref().map(ToString::to_string)
    }

    fn function_id(&self) -> String {
        self.function_id.to_string()
    }

    fn function(&self) -> String {
        self.function.to_string()
    }
}

impl WorkerStartRequest for StageTriggerStartRequest {
    fn worker(&self) -> &str {
        self.worker.as_ref()
    }

    fn worker_id(&self) -> String {
        logical_id_text(&self.worker_id)
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

    fn project(&self) -> Option<String> {
        self.project.as_ref().map(ToString::to_string)
    }

    fn collection_id(&self) -> String {
        self.collection_id.to_string()
    }

    fn collection(&self) -> String {
        self.collection.to_string()
    }

    fn connection_id(&self) -> Option<String> {
        self.connection_id.as_ref().map(ToString::to_string)
    }

    fn connection(&self) -> Option<String> {
        self.connection.as_ref().map(ToString::to_string)
    }

    fn function_id(&self) -> String {
        self.function_id.to_string()
    }

    fn function(&self) -> String {
        self.function.to_string()
    }

    fn custom_labels(&self) -> Vec<(String, String)> {
        vec![(
            LABEL_FUNCTION_VERSION_ID.to_owned(),
            logical_id_text(&self.function_version_id),
        )]
    }
}

impl WorkerStartRequest for CronStartRequest {
    fn worker(&self) -> &str {
        self.worker.as_ref()
    }

    fn worker_id(&self) -> String {
        logical_id_text(&self.worker_id)
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

    fn project(&self) -> Option<String> {
        self.project.as_ref().map(ToString::to_string)
    }

    fn collection_id(&self) -> String {
        self.collection_id.to_string()
    }

    fn collection(&self) -> String {
        self.collection.to_string()
    }

    fn connection_id(&self) -> Option<String> {
        self.connection_id.as_ref().map(ToString::to_string)
    }

    fn connection(&self) -> Option<String> {
        self.connection.as_ref().map(ToString::to_string)
    }

    fn function_id(&self) -> String {
        self.function_id.to_string()
    }

    fn function(&self) -> String {
        self.function.to_string()
    }

    fn custom_labels(&self) -> Vec<(String, String)> {
        vec![(
            LABEL_FUNCTION_VERSION_ID.to_owned(),
            logical_id_text(&self.function_version_id),
        )]
    }
}

#[cfg(test)]
mod tests;
