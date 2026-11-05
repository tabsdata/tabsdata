//
// Copyright 2026 Tabsdata Inc.
//

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;

/// Polls `future` once: whether it can finish now, without waiting on
/// anything. A wait under test is observed by this, not by a sleep.
pub async fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx))).await
}
