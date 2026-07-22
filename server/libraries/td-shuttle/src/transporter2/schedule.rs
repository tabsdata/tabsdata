//
// Copyright 2026 Tabsdata Inc.
//

//! Admission control: two counting semaphores bound how many file jobs run at
//! once — one pool for **staged** jobs (download→convert→upload: disk- and
//! network-heavy) and a separate pool for **streaming** jobs (in-memory convert
//! / byte copy). They are independent, so a streaming job is admitted even when
//! every staged slot is busy (and vice versa), matching §11 of the design.

use crate::transporter2::spec::CopyOptions;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct Scheduler {
    staged: Arc<Semaphore>,
    streaming: Arc<Semaphore>,
}

impl Scheduler {
    pub fn new(max_staged: usize, max_streaming: usize) -> Self {
        Self {
            staged: Arc::new(Semaphore::new(max_staged.max(1))),
            streaming: Arc::new(Semaphore::new(max_streaming.max(1))),
        }
    }

    pub fn from_options(opts: &CopyOptions) -> Self {
        Self::new(opts.max_staged, opts.max_streaming)
    }

    /// The most jobs that can run concurrently across both pools — an upper
    /// bound for the driver's `buffered` window so the semaphores (not the
    /// stream) are the real limiter.
    pub fn capacity(&self) -> usize {
        self.staged.available_permits() + self.streaming.available_permits()
    }

    /// Acquire a slot, holding it until the returned permit is dropped. `staged`
    /// selects the pool. The semaphores are never closed, so this cannot fail.
    pub async fn admit(&self, staged: bool) -> OwnedSemaphorePermit {
        let sem = if staged {
            self.staged.clone()
        } else {
            self.streaming.clone()
        };
        sem.acquire_owned()
            .await
            .expect("transporter scheduler semaphore closed")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn streaming_admits_when_staged_is_full() {
        let sched = Scheduler::new(1, 2);
        // Exhaust the single staged slot.
        let _staged = sched.admit(true).await;
        // A streaming admission must still proceed immediately (separate pool).
        let s1 = sched.admit(false).await;
        let s2 = sched.admit(false).await;
        drop((s1, s2));
        // Capacity reflects the streaming pool freed, staged still held.
        assert_eq!(sched.streaming.available_permits(), 2);
        assert_eq!(sched.staged.available_permits(), 0);
    }
}
