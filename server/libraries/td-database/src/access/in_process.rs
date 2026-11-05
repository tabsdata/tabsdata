//
// Copyright 2026 Tabsdata Inc.
//

//! [`InProcessLocker`]: the locks in one table per process, keyed by
//! component, so every handle over a component's pool waits on the same ones.

#[cfg(any(test, feature = "test-utils"))]
use crate::access::ProjectKey;
use crate::access::lock::{Held, Locker, ProjectLockKey};
#[cfg(any(test, feature = "test-utils"))]
use crate::pool::TdPool;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{Mutex, OwnedMutexGuard};

static TABLE: LazyLock<std::sync::Mutex<Table>> = LazyLock::new(std::sync::Mutex::default);

#[derive(Default)]
struct Table {
    projects: HashMap<ProjectLockKey, Entry>,
}

#[derive(Default)]
struct Entry {
    lock: Arc<Mutex<()>>,
    waiting: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct InProcessLocker;

impl Locker for InProcessLocker {
    fn take<'a>(
        &'a self,
        key: &'a ProjectLockKey,
        within: Duration,
    ) -> BoxFuture<'a, Option<Held>> {
        async move { take_within(key, within).await.map(Held::new) }.boxed()
    }
}

/// Waits for `key`, for no longer than `wait`: `None` when it is still held
/// by then, holding nothing.
pub(crate) async fn take_within(key: &ProjectLockKey, wait: Duration) -> Option<InProcessHeld> {
    tokio::time::timeout(wait, take(key)).await.ok()
}

pub(crate) async fn take(key: &ProjectLockKey) -> InProcessHeld {
    let waiting = Waiting::on(key);
    let guard = waiting.lock.clone().lock_owned().await;
    drop(waiting);
    InProcessHeld {
        key: key.clone(),
        guard: Some(guard),
    }
}

/// A wait for a lock, counted while it lasts, so a test can see a unit
/// queued on a project.
struct Waiting {
    key: ProjectLockKey,
    lock: Arc<Mutex<()>>,
}

impl Waiting {
    fn on(key: &ProjectLockKey) -> Self {
        let mut table = TABLE.lock().unwrap();
        let entry = table.projects.entry(key.clone()).or_default();
        entry.waiting += 1;
        Self {
            key: key.clone(),
            lock: entry.lock.clone(),
        }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        let mut table = TABLE.lock().unwrap();
        if let Some(entry) = table.projects.get_mut(&self.key) {
            entry.waiting -= 1;
        }
    }
}

/// Dropping releases the lock, and forgets it when no one else holds or
/// waits on it. The guard is an `Option` only so `drop` can release it before
/// it looks.
pub(crate) struct InProcessHeld {
    key: ProjectLockKey,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for InProcessHeld {
    fn drop(&mut self) {
        drop(self.guard.take());
        let mut table = TABLE.lock().unwrap();
        if table
            .projects
            .get(&self.key)
            .is_some_and(|entry| entry.waiting == 0 && Arc::strong_count(&entry.lock) == 1)
        {
            table.projects.remove(&self.key);
        }
    }
}

#[cfg(test)]
pub(crate) fn tracks(key: &ProjectLockKey) -> bool {
    TABLE.lock().unwrap().projects.contains_key(key)
}

/// How many units wait for `project`'s lock on `pool` right now.
#[cfg(any(test, feature = "test-utils"))]
pub fn waiting_on(pool: &TdPool, project: &impl ProjectKey) -> usize {
    let key = ProjectLockKey::of(pool, &project.to_string());
    TABLE
        .lock()
        .unwrap()
        .projects
        .get(&key)
        .map_or(0, |entry| entry.waiting)
}
