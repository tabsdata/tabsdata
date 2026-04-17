//
// Copyright 2025 Tabs Data Inc.
//

// ---------------------------------------------------------------------------
// SchedulerNotify — shared signal to wake the scheduler on demand
// ---------------------------------------------------------------------------
//
// Uses two independent `tokio::sync::Notify` channels — one per scheduler loop
// (requester and dispatcher). `notify()` calls `notify_one()` on both, which
// stores a permit when the loop is busy so the next `notified_*().await` returns
// immediately instead of being lost.

#[derive(Clone)]
pub struct SchedulerNotify {
    requester: std::sync::Arc<tokio::sync::Notify>,
    dispatcher: std::sync::Arc<tokio::sync::Notify>,
}

impl Default for SchedulerNotify {
    fn default() -> Self {
        Self {
            requester: std::sync::Arc::new(tokio::sync::Notify::new()),
            dispatcher: std::sync::Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl SchedulerNotify {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wake both scheduler loops. Uses `notify_one()` so that a permit is
    /// stored if a loop is currently busy — the next `notified_*().await`
    /// returns immediately.
    pub fn notify(&self) {
        self.requester.notify_one();
        self.dispatcher.notify_one();
    }

    /// Wait for a notification on the requester channel.
    pub async fn notified_requester(&self) {
        self.requester.notified().await;
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
    async fn test_notify_wakes_requester() {
        let notify = SchedulerNotify::new();
        let n = notify.clone();
        let barrier = Arc::new(Barrier::new(2));
        let b = barrier.clone();

        let handle = tokio::spawn(async move {
            b.wait().await;
            n.notified_requester().await;
            true
        });

        barrier.wait().await;
        tokio::task::yield_now().await;
        notify.notify();

        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("timed out")
            .expect("task panicked");
        assert!(result);
    }

    #[tokio::test]
    async fn test_notify_wakes_dispatcher() {
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
        notify.notify();

        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("timed out")
            .expect("task panicked");
        assert!(result);
    }

    #[tokio::test]
    async fn test_notify_wakes_both_loops() {
        let notify = SchedulerNotify::new();
        let barrier = Arc::new(Barrier::new(3));

        let n1 = notify.clone();
        let b1 = barrier.clone();
        let req = tokio::spawn(async move {
            b1.wait().await;
            n1.notified_requester().await;
        });

        let n2 = notify.clone();
        let b2 = barrier.clone();
        let disp = tokio::spawn(async move {
            b2.wait().await;
            n2.notified_dispatcher().await;
        });

        barrier.wait().await;
        tokio::task::yield_now().await;
        notify.notify();

        tokio::time::timeout(Duration::from_secs(1), req)
            .await
            .expect("requester timed out")
            .expect("requester panicked");
        tokio::time::timeout(Duration::from_secs(1), disp)
            .await
            .expect("dispatcher timed out")
            .expect("dispatcher panicked");
    }

    /// The key behavioural test: notify_one() stores a permit so that a
    /// notification sent while the loop is busy is not lost.
    #[tokio::test]
    async fn test_notify_stores_permit_when_not_waiting() {
        let notify = SchedulerNotify::new();

        // Nobody is waiting — the permit should be stored.
        notify.notify();

        // Both channels should return immediately.
        tokio::time::timeout(Duration::from_millis(50), notify.notified_requester())
            .await
            .expect("requester should have returned immediately");
        tokio::time::timeout(Duration::from_millis(50), notify.notified_dispatcher())
            .await
            .expect("dispatcher should have returned immediately");
    }

    #[tokio::test]
    async fn test_clone_shares_channels() {
        let a = SchedulerNotify::new();
        let b = a.clone();

        let handle = tokio::spawn(async move {
            b.notified_requester().await;
            true
        });

        tokio::task::yield_now().await;
        a.notify();

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
            tokio::time::timeout(Duration::from_millis(50), b_clone.notified_requester()).await
        });

        tokio::task::yield_now().await;
        a.notify();

        // b should NOT be woken by a.notify()
        let result = handle.await.expect("task panicked");
        assert!(result.is_err(), "b should have timed out");
    }
}
