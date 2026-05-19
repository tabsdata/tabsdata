//
// Copyright 2025 Tabs Data Inc.
//

// ---------------------------------------------------------------------------
// SchedulerNotify — shared signal to wake the scheduler on demand
// ---------------------------------------------------------------------------
//
// Uses two independent `tokio::sync::Notify` channels — one per scheduler loop
// (scheduler and dispatcher). Each wake uses `notify_one()`, which stores a
// permit when the target loop is busy so the next `notified_*().await` returns
// immediately instead of being lost.

#[derive(Clone)]
pub struct SchedulerNotify {
    scheduler: std::sync::Arc<tokio::sync::Notify>,
    dispatcher: std::sync::Arc<tokio::sync::Notify>,
}

impl Default for SchedulerNotify {
    fn default() -> Self {
        Self {
            scheduler: std::sync::Arc::new(tokio::sync::Notify::new()),
            dispatcher: std::sync::Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl SchedulerNotify {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wake the scheduler loop so it re-evaluates which function runs are
    /// ready to execute. Called when external state changes (execute, cancel,
    /// recover, worker callback). If the scheduler produces new `Locked`
    /// messages, it wakes the dispatcher itself.
    pub fn notify_scheduler(&self) {
        self.scheduler.notify_one();
    }

    /// Wake the dispatcher loop. Called by the scheduler after producing new
    /// `Locked` messages so the dispatcher picks them up without waiting for
    /// the failover timer.
    pub fn notify_dispatcher(&self) {
        self.dispatcher.notify_one();
    }

    /// Wait for a notification on the scheduler channel.
    pub async fn notified_scheduler(&self) {
        self.scheduler.notified().await;
    }

    /// Wait for a notification on the dispatcher channel.
    pub async fn notified_dispatcher(&self) {
        self.dispatcher.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::SchedulerNotify;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn test_notify_scheduler_wakes_scheduler() {
        let notify = SchedulerNotify::new();
        let n = notify.clone();
        let barrier = Arc::new(Barrier::new(2));
        let b = barrier.clone();

        let handle = tokio::spawn(async move {
            b.wait().await;
            n.notified_scheduler().await;
            true
        });

        barrier.wait().await;
        tokio::task::yield_now().await;
        notify.notify_scheduler();

        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("timed out")
            .expect("task panicked");
        assert!(result);
    }

    #[tokio::test]
    async fn test_notify_dispatcher_wakes_dispatcher() {
        let notify = SchedulerNotify::new();
        let n = notify.clone();
        let barrier = Arc::new(Barrier::new(2));
        let b = barrier.clone();

        let handle = tokio::spawn(async move {
            b.wait().await;
            n.notified_dispatcher().await;
            true
        });

        barrier.wait().await;
        tokio::task::yield_now().await;
        notify.notify_dispatcher();

        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("timed out")
            .expect("task panicked");
        assert!(result);
    }

    #[tokio::test]
    async fn test_notify_scheduler_does_not_wake_dispatcher() {
        let notify = SchedulerNotify::new();
        notify.notify_scheduler();

        let result =
            tokio::time::timeout(Duration::from_millis(50), notify.notified_dispatcher()).await;
        assert!(result.is_err(), "dispatcher should not have been woken");
    }

    #[tokio::test]
    async fn test_notify_dispatcher_does_not_wake_scheduler() {
        let notify = SchedulerNotify::new();
        notify.notify_dispatcher();

        let result =
            tokio::time::timeout(Duration::from_millis(50), notify.notified_scheduler()).await;
        assert!(result.is_err(), "scheduler should not have been woken");
    }

    /// The key behavioural test: notify_one() stores a permit so that a
    /// notification sent while the loop is busy is not lost.
    #[tokio::test]
    async fn test_notify_stores_permit_when_not_waiting() {
        let notify = SchedulerNotify::new();

        // Nobody is waiting — the permits should be stored.
        notify.notify_scheduler();
        notify.notify_dispatcher();

        tokio::time::timeout(Duration::from_millis(50), notify.notified_scheduler())
            .await
            .expect("scheduler should have returned immediately");
        tokio::time::timeout(Duration::from_millis(50), notify.notified_dispatcher())
            .await
            .expect("dispatcher should have returned immediately");
    }

    #[tokio::test]
    async fn test_clone_shares_channels() {
        let a = SchedulerNotify::new();
        let b = a.clone();

        let handle = tokio::spawn(async move {
            b.notified_scheduler().await;
            true
        });

        tokio::task::yield_now().await;
        a.notify_scheduler();

        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("timed out")
            .expect("task panicked");
        assert!(result);
    }

    #[tokio::test]
    async fn test_default_creates_independent_instance() {
        let a = SchedulerNotify::default();
        let b = SchedulerNotify::default();

        let b_clone = b.clone();
        let handle = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(50), b_clone.notified_scheduler()).await
        });

        tokio::task::yield_now().await;
        a.notify_scheduler();

        // b should NOT be woken by a.notify_scheduler()
        let result = handle.await.expect("task panicked");
        assert!(result.is_err(), "b should have timed out");
    }
}
