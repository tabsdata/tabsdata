//
// Copyright 2026 Tabsdata Inc.
//

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use http::{Request, Response};
use kube::client::Body;
use td_log_archive::ArchiveReader;

use super::*;

const NAMESPACE: &str = "tabsdatatest";
const WORKER: &str = "wkr20260713-0000000005";
const LOGICAL_WORKER: &str = "wkr20260713-5";
const LOG_ID: &str = "pub-wkr20260713-5";
const ARCHIVED_CONTAINER: &str = "0130-run-function";
const EMPTY_POD_LIST: &str = r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}"#;

fn podless_context(listings: Arc<AtomicUsize>) -> KubeContext {
    let service = tower::service_fn(move |_: Request<Body>| {
        let listings = listings.clone();
        async move {
            listings.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Infallible>(Response::new(Body::from(
                EMPTY_POD_LIST.as_bytes().to_vec(),
            )))
        }
    });
    KubeContext::new(
        Arc::new(kube::Client::new(service, NAMESPACE)),
        NAMESPACE.to_owned(),
    )
}

/// A worker ID and log ID an archive was asked for.
type Asked = (String, String);

/// An archive holding one line for any worker, recording what it was asked.
#[derive(Clone, Default)]
struct FakeArchive {
    requests: Arc<Mutex<Vec<Asked>>>,
}

impl FakeArchive {
    fn requests(&self) -> Vec<Asked> {
        self.requests.lock().expect("requests").clone()
    }
}

impl ArchiveReader for FakeArchive {
    fn stream(&self, request: ArchiveRequest<'_>) -> WorkerLogStream {
        self.requests
            .lock()
            .expect("requests")
            .push((request.worker_id.to_owned(), request.log_id.to_owned()));
        Box::pin(futures::stream::iter([
            Ok(container_header(ARCHIVED_CONTAINER)),
            Ok(Bytes::from_static(b"archived\n")),
        ]))
    }
}

fn podless(finished: bool, archive: Option<&FakeArchive>) -> PodlessLogs {
    PodlessLogs {
        finished,
        log_id: LOG_ID.to_owned(),
        archive: archive.map(|archive| LogArchive::new(archive.clone())),
    }
}

async fn read(context: &KubeContext, follow: bool, podless: PodlessLogs) -> String {
    let worker_id = WorkerId::try_from(WORKER).expect("worker id");
    let chunks: Vec<_> = stream_worker_logs(context, &worker_id, follow, podless)
        .collect()
        .await;
    chunks
        .into_iter()
        .map(|chunk| String::from_utf8(chunk.expect("chunk").to_vec()).expect("utf-8"))
        .collect()
}

#[tokio::test]
async fn logs_without_a_pod_or_archive_say_the_logs_are_gone() {
    let context = podless_context(Arc::default());
    let output = read(&context, false, podless(true, None)).await;
    assert!(output.starts_with("no pod found for worker 'wkr20260713-5'"));
}

#[tokio::test]
async fn logs_without_a_pod_are_read_from_the_archive() {
    let archive = FakeArchive::default();
    let context = podless_context(Arc::default());
    let output = read(&context, false, podless(true, Some(&archive))).await;
    assert_eq!(
        output,
        format!(
            "{}archived\n",
            String::from_utf8(container_header(ARCHIVED_CONTAINER).to_vec()).expect("utf-8")
        )
    );
    assert_eq!(archive.requests().len(), 1);
}

#[tokio::test]
async fn logs_without_a_pod_use_the_archive_even_for_an_unfinished_worker() {
    let archive = FakeArchive::default();
    let context = podless_context(Arc::default());
    let output = read(&context, false, podless(false, Some(&archive))).await;
    assert!(output.ends_with("archived\n"));
}

#[tokio::test]
async fn tailing_a_finished_worker_without_a_pod_reads_the_archive_and_ends() {
    let archive = FakeArchive::default();
    let listings = Arc::new(AtomicUsize::new(0));
    let context = podless_context(listings.clone());
    let output = read(&context, true, podless(true, Some(&archive))).await;
    assert!(output.ends_with("archived\n"));
    assert_eq!(listings.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn tailing_an_unfinished_worker_waits_for_its_pod_then_gives_up() {
    let listings = Arc::new(AtomicUsize::new(0));
    let context = podless_context(listings.clone());
    let output = read(&context, true, podless(false, None)).await;
    assert!(output.starts_with("no pod found for worker 'wkr20260713-5'"));
    let polls =
        (LOG_POD_APPEARANCE_TIMEOUT.as_millis() / LOG_POD_POLL_INTERVAL.as_millis()) as usize;
    assert!(listings.load(Ordering::SeqCst) >= polls);
}

#[tokio::test]
async fn the_archive_is_asked_for_the_worker_and_its_log_id() {
    let archive = FakeArchive::default();
    let context = podless_context(Arc::default());

    read(&context, false, podless(true, Some(&archive))).await;

    assert_eq!(
        archive.requests(),
        vec![(LOGICAL_WORKER.to_owned(), LOG_ID.to_owned())]
    );
}
