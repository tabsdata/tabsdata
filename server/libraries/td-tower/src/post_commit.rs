//
// Copyright 2025 Tabsdata Inc.
//

//! Side effects deferred until *after* the surrounding transaction commits.
//!
//! [`TransactionProviderService`](crate::default_services) inserts a
//! [`PostCommitActions`] into the handler before running the inner service, and
//! drains + runs it once — and only if — the transaction commits successfully.
//! On rollback the actions are dropped, unrun.
//!
//! Use this for side effects that must observe *committed* state, most notably
//! invalidating a process-wide cache that other requests read on their own
//! (fresh) connections. Performing such an invalidation from inside the
//! transaction races the commit: a concurrent reader can repopulate the cache
//! from a pre-commit snapshot that omits this transaction's writes, and that
//! stale snapshot then sticks until the next invalidation. Deferring the
//! invalidation to post-commit closes that window.

use futures_util::future::BoxFuture;

/// A queue of actions to run after the transaction commits, in registration
/// order. Register from an inner layer via `MutRef<PostCommitActions>`.
#[derive(Default)]
pub struct PostCommitActions {
    actions: Vec<Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>>,
}

impl PostCommitActions {
    /// Queue an action to run after the surrounding transaction commits.
    pub fn push<F>(&mut self, action: F)
    where
        F: FnOnce() -> BoxFuture<'static, ()> + Send + 'static,
    {
        self.actions.push(Box::new(action));
    }

    /// `true` if no actions are queued.
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    /// Run all queued actions in registration order, consuming them.
    pub async fn run(self) {
        for action in self.actions {
            action().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn runs_actions_in_order() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut actions = PostCommitActions::default();
        assert!(actions.is_empty());
        for i in 0..3 {
            let seen = seen.clone();
            actions.push(move || Box::pin(async move { seen.lock().unwrap().push(i) }));
        }
        assert!(!actions.is_empty());
        actions.run().await;
        assert_eq!(*seen.lock().unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn dropped_actions_do_not_run() {
        let ran = Arc::new(AtomicUsize::new(0));
        let mut actions = PostCommitActions::default();
        let ran2 = ran.clone();
        actions.push(move || {
            Box::pin(async move {
                ran2.fetch_add(1, Ordering::SeqCst);
            })
        });
        drop(actions); // rollback path: never `run()`
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }
}
