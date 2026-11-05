//
// Copyright 2026 Tabsdata Inc.
//

//! Which projects owe a pass, and for what. A write marks what it can affect
//! on its project after it commits; a project with marks is runnable until a
//! runner takes it, at most one pass per project runs at a time, and marks
//! made while it runs make it runnable again when it finishes. Marks are
//! hints: a pass re-reads what each step acts on.

use std::collections::{BTreeMap, VecDeque};
use std::ops::{BitOr, BitOrAssign};
use std::sync::{Mutex, PoisonError};
use td_objects::types::basic::ProjectId;
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Concerns(u8);

impl Concerns {
    pub const NONE: Self = Self(0);
    /// Ending runs of deleted functions, settling transactions, releasing
    /// runs nothing blocks any more.
    pub const HOUSEKEEPING: Self = Self(1);
    pub const RUNNERS: Self = Self(1 << 1);
    pub const REPLAYS: Self = Self(1 << 2);
    pub const TRIGGERS: Self = Self(1 << 3);
    /// Housekeeping, then both dispatch paths: what a run whose state
    /// changed owes.
    pub const RUNS: Self = Self(0b111);
    pub const ALL: Self = Self(0b1111);

    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

impl BitOr for Concerns {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for Concerns {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

#[derive(Debug, Default)]
struct Slot {
    marked: Concerns,
    running: bool,
}

#[derive(Debug, Default)]
struct State {
    projects: BTreeMap<ProjectId, Slot>,
    runnable: VecDeque<ProjectId>,
}

#[derive(Debug, Default)]
pub struct ProjectWakes {
    state: Mutex<State>,
    runnable: Notify,
}

impl ProjectWakes {
    pub fn wake(&self, project: &ProjectId, concerns: Concerns) {
        if concerns.is_empty() {
            return;
        }
        let queued = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let slot = state.projects.entry(*project).or_default();
            let idle = slot.marked.is_empty() && !slot.running;
            slot.marked |= concerns;
            if idle {
                state.runnable.push_back(*project);
            }
            idle
        };
        if queued {
            self.runnable.notify_one();
        }
    }

    /// The next runnable project and its concerns, which it now owes nothing
    /// for until [`finish`](Self::finish).
    pub fn take(&self) -> Option<(ProjectId, Concerns)> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let project = state.runnable.pop_front()?;
        let slot = state.projects.get_mut(&project)?;
        slot.running = true;
        Some((project, std::mem::take(&mut slot.marked)))
    }

    /// Ends the pass of `project`, marking `again` on it: what the pass left
    /// undone.
    pub fn finish(&self, project: &ProjectId, again: Concerns) {
        let queued = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(slot) = state.projects.get_mut(project) else {
                return;
            };
            slot.running = false;
            slot.marked |= again;
            if slot.marked.is_empty() {
                state.projects.remove(project);
                false
            } else {
                state.runnable.push_back(*project);
                true
            }
        };
        if queued {
            self.runnable.notify_one();
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn marked(&self, project: &ProjectId) -> Concerns {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .projects
            .get(project)
            .map(|slot| slot.marked)
            .unwrap_or_default()
    }

    /// Resolves once a project became runnable since the last call.
    pub async fn runnable(&self) {
        self.runnable.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    fn project() -> ProjectId {
        ProjectId::default()
    }

    #[test]
    fn test_a_woken_project_is_taken_with_its_concerns() {
        let wakes = ProjectWakes::default();
        let p = project();
        wakes.wake(&p, Concerns::RUNNERS);
        wakes.wake(&p, Concerns::HOUSEKEEPING);
        assert_eq!(
            wakes.take(),
            Some((p, Concerns::RUNNERS | Concerns::HOUSEKEEPING))
        );
        assert_eq!(wakes.take(), None);
    }

    #[test]
    fn test_no_concerns_wake_nothing() {
        let wakes = ProjectWakes::default();
        wakes.wake(&project(), Concerns::NONE);
        assert_eq!(wakes.take(), None);
    }

    #[test]
    fn test_a_running_project_is_not_taken_twice() {
        let wakes = ProjectWakes::default();
        let p = project();
        wakes.wake(&p, Concerns::RUNNERS);
        assert!(wakes.take().is_some());
        wakes.wake(&p, Concerns::TRIGGERS);
        assert_eq!(wakes.take(), None);
        assert_eq!(wakes.marked(&p), Concerns::TRIGGERS);
    }

    #[test]
    fn test_marks_during_a_pass_run_it_again() {
        let wakes = ProjectWakes::default();
        let p = project();
        wakes.wake(&p, Concerns::RUNNERS);
        wakes.take();
        wakes.wake(&p, Concerns::TRIGGERS);
        wakes.finish(&p, Concerns::NONE);
        assert_eq!(wakes.take(), Some((p, Concerns::TRIGGERS)));
    }

    #[test]
    fn test_a_pass_left_undone_runs_again() {
        let wakes = ProjectWakes::default();
        let p = project();
        wakes.wake(&p, Concerns::RUNNERS);
        wakes.take();
        wakes.finish(&p, Concerns::RUNNERS);
        assert_eq!(wakes.take(), Some((p, Concerns::RUNNERS)));
    }

    #[test]
    fn test_a_clean_finish_forgets_the_project() {
        let wakes = ProjectWakes::default();
        let p = project();
        wakes.wake(&p, Concerns::RUNNERS);
        wakes.take();
        wakes.finish(&p, Concerns::NONE);
        assert_eq!(wakes.take(), None);
        assert_eq!(wakes.marked(&p), Concerns::NONE);
        assert!(wakes.state.lock().unwrap().projects.is_empty());
    }

    #[test]
    fn test_projects_are_taken_in_wake_order() {
        let wakes = ProjectWakes::default();
        let (a, b) = (project(), project());
        wakes.wake(&b, Concerns::RUNNERS);
        wakes.wake(&a, Concerns::RUNNERS);
        wakes.wake(&b, Concerns::TRIGGERS);
        assert_eq!(wakes.take().map(|(p, _)| p), Some(b));
        assert_eq!(wakes.take().map(|(p, _)| p), Some(a));
    }

    #[test]
    fn test_a_wake_signals_runnable() {
        let wakes = ProjectWakes::default();
        assert!(wakes.runnable().now_or_never().is_none());
        wakes.wake(&project(), Concerns::RUNNERS);
        assert!(wakes.runnable().now_or_never().is_some());
    }

    #[test]
    fn test_a_mark_on_a_queued_project_does_not_signal_again() {
        let wakes = ProjectWakes::default();
        let p = project();
        wakes.wake(&p, Concerns::RUNNERS);
        assert!(wakes.runnable().now_or_never().is_some());
        wakes.wake(&p, Concerns::TRIGGERS);
        assert!(wakes.runnable().now_or_never().is_none());
    }
}
