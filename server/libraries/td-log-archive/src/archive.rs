//
// Copyright 2026 Tabsdata Inc.
//

//! Where a worker's logs are read from once its pod is gone: the cluster's own
//! log store, picked from what the apiserver pod runs as.

use std::sync::Arc;

use td_bridge::worker::WorkerLogStream;

use crate::cloudlogging::CloudLoggingReader;
use crate::cloudwatch::CloudWatchLogReader;
use crate::kminus::PodRunnerReader;
use crate::loganalytics::LogAnalyticsReader;

pub(crate) const LOG_ID_LABEL: &str = "tabsdata.com/log-id";
pub(crate) const STARTED_ON_LEEWAY_MILLIS: i64 = 30_000;

#[derive(Clone, Copy, Debug)]
pub struct ArchiveRequest<'a> {
    pub worker_id: &'a str,
    pub log_id: &'a str,
    pub started_on_millis: Option<i64>,
}

pub trait ArchiveReader: Send + Sync {
    fn stream(&self, request: ArchiveRequest<'_>) -> WorkerLogStream;
}

#[derive(Clone)]
pub struct LogArchive {
    reader: Arc<dyn ArchiveReader>,
}

impl LogArchive {
    pub fn new(reader: impl ArchiveReader + 'static) -> Self {
        Self {
            reader: Arc::new(reader),
        }
    }

    pub async fn from_environment(namespace: &str) -> Option<Self> {
        if CloudWatchLogReader::assumes_role() {
            return CloudWatchLogReader::from_environment(namespace)
                .await
                .map(Self::new);
        }
        if let Some(reader) = PodRunnerReader::from_environment() {
            return Some(Self::new(reader));
        }
        if let Some(reader) = LogAnalyticsReader::from_environment(namespace) {
            return Some(Self::new(reader));
        }
        CloudLoggingReader::from_metadata(namespace)
            .await
            .map(Self::new)
    }

    pub fn stream(&self, request: ArchiveRequest<'_>) -> WorkerLogStream {
        self.reader.stream(request)
    }
}
