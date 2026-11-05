//
// Copyright 2026 Tabsdata Inc.
//

//! The in-process locks alone, no database: who waits on whom, observed by
//! polling each wait once.

use crate::access::in_process::{InProcessHeld, take, take_within, tracks};
use crate::access::lock::ProjectLockKey;
use crate::test_utils::poll::poll_once;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

const COMPONENT: &str = "in_process_test";
const WAIT: Duration = Duration::from_secs(10);

fn id(prefix: &str) -> String {
    format!("{prefix}_{}", td_common::id::id())
}

type Wait = Pin<Box<dyn Future<Output = InProcessHeld>>>;

fn key(project: &str) -> ProjectLockKey {
    ProjectLockKey::in_component(COMPONENT, project)
}

fn locked(project: &str) -> Wait {
    let key = key(project);
    Box::pin(async move { take(&key).await })
}

async fn taken(wait: &mut Wait) -> Option<InProcessHeld> {
    match poll_once(wait).await {
        Poll::Ready(held) => Some(held),
        Poll::Pending => None,
    }
}

#[tokio::test]
async fn a_locked_unit_waits_for_the_last_one_of_its_project_only() {
    let (project, other) = (id("p"), id("p"));
    let held = locked(&project).await;

    let mut next = locked(&project);
    assert!(taken(&mut next).await.is_none());
    assert!(taken(&mut locked(&other)).await.is_some());

    drop(held);
    assert!(taken(&mut next).await.is_some());
}

#[tokio::test]
async fn released_keys_are_forgotten() {
    let project = id("p");

    let held = locked(&project).await;
    let mut waiting = locked(&project);
    assert!(taken(&mut waiting).await.is_none());
    drop(waiting);
    drop(held);
    assert!(!tracks(&key(&project)));
}

/// A wait gives up at its bound, holding nothing.
#[tokio::test(start_paused = true)]
async fn a_wait_gives_up_at_its_bound() {
    let project = id("p");
    let held = locked(&project).await;

    let gave_up = take_within(&key(&project), WAIT).await;
    assert!(gave_up.is_none());

    drop(held);
    let next = take_within(&key(&project), WAIT).await;
    assert!(next.is_some());
}
